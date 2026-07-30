from __future__ import annotations

import json
import shlex
import subprocess
import tempfile
from typing import Any, Callable

from .config import HostConfig
from .errors import HostUnavailable

_MAX_RESPONSE_BYTES = 1024 * 1024


Runner = Callable[..., subprocess.CompletedProcess[str]]


def _bounded_run(
    argv: list[str],
    *,
    input: str,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    with (
        tempfile.TemporaryFile(mode="w+b") as stdin,
        tempfile.TemporaryFile(mode="w+b") as stdout,
    ):
        stdin.write(input.encode("utf-8"))
        stdin.seek(0)
        completed = subprocess.run(
            argv,
            stdin=stdin,
            stdout=stdout,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            check=False,
        )
        stdout.seek(0)
        output = stdout.read(_MAX_RESPONSE_BYTES + 1)
    if len(output) > _MAX_RESPONSE_BYTES:
        raise HostUnavailable("target adapter response exceeds the size limit")
    try:
        decoded = output.decode("utf-8")
    except UnicodeDecodeError as error:
        raise HostUnavailable("target adapter returned invalid JSON") from error
    return subprocess.CompletedProcess(argv, completed.returncode, stdout=decoded, stderr="")


def _invoke(
    argv: list[str],
    request: dict[str, Any],
    *,
    runner: Runner | None,
    timeout: float,
) -> dict[str, Any]:
    payload = json.dumps(request, separators=(",", ":"), sort_keys=True) + "\n"
    try:
        if runner is None:
            completed = _bounded_run(argv, input=payload, timeout=timeout)
        else:
            completed = runner(
                argv,
                input=payload,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise HostUnavailable("target adapter could not be reached") from error
    if completed.returncode != 0:
        raise HostUnavailable("target adapter failed")
    if len(completed.stdout.encode("utf-8")) > _MAX_RESPONSE_BYTES:
        raise HostUnavailable("target adapter response exceeds the size limit")
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise HostUnavailable("target adapter returned invalid JSON") from error
    if not isinstance(result, dict):
        raise HostUnavailable("target adapter returned invalid JSON")
    return result


class LocalTransport:
    def __init__(
        self,
        *,
        runner: Runner | None = None,
        timeout: float = 45,
    ) -> None:
        self._runner = runner
        self._timeout = timeout

    def call(self, host: HostConfig, request: dict[str, Any]) -> dict[str, Any]:
        return _invoke(
            list(host.agent_command),
            request,
            runner=self._runner,
            timeout=self._timeout,
        )


class SshTransport:
    def __init__(
        self,
        *,
        ssh_command: tuple[str, ...] = ("ssh",),
        runner: Runner | None = None,
        timeout: float = 45,
    ) -> None:
        self._ssh_command = ssh_command
        self._runner = runner
        self._timeout = timeout

    def call(self, host: HostConfig, request: dict[str, Any]) -> dict[str, Any]:
        if host.ssh_alias is None:
            raise HostUnavailable("SSH transport requires a configured alias")
        if host.platform == "windows":
            remote_command = subprocess.list2cmdline(list(host.agent_command))
        else:
            remote_command = shlex.join(host.agent_command)
        argv = [
            *self._ssh_command,
            "-o",
            "BatchMode=yes",
            "--",
            host.ssh_alias,
            remote_command,
        ]
        return _invoke(
            argv,
            request,
            runner=self._runner,
            timeout=self._timeout,
        )


class FleetTransport:
    def __init__(
        self,
        *,
        local: LocalTransport | None = None,
        ssh: SshTransport | None = None,
        ssh_command: tuple[str, ...] = ("ssh",),
    ) -> None:
        self._local = local or LocalTransport()
        self._ssh = ssh or SshTransport(ssh_command=ssh_command)

    def call(self, host: HostConfig, request: dict[str, Any]) -> dict[str, Any]:
        if host.transport == "local":
            return self._local.call(host, request)
        if host.transport == "ssh":
            return self._ssh.call(host, request)
        raise HostUnavailable(f"unsupported transport for host {host.host_id}")
