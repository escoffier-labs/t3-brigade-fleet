from __future__ import annotations

import json
import subprocess
import sys
import unittest
from dataclasses import replace
from unittest.mock import patch

from t3_fleet.config import HostConfig
from t3_fleet.errors import HostUnavailable
from t3_fleet.transport import LocalTransport, SshTransport


def host() -> HostConfig:
    return HostConfig(
        host_id="linux-worker",
        enabled=True,
        platform="linux",
        transport="ssh",
        ssh_alias="linux-worker",
        agent_command=("/opt/t3-fleet-agent",),
        active_limit=2,
        expected_hostname="linux-worker",
        t3_url="http://127.0.0.1:3773",
        t3_command=("t3",),
        t3_base_dir=None,
        agent_state_dir=None,
    )


def windows_host() -> HostConfig:
    return HostConfig(
        host_id="windows-worker",
        enabled=True,
        platform="windows",
        transport="ssh",
        ssh_alias="windows-worker",
        agent_command=(
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-File",
            r"C:\Users\Example Operator\bin\t3-fleet-agent.ps1",
        ),
        active_limit=2,
        expected_hostname="windows-worker",
        t3_url="http://127.0.0.1:3773",
        t3_command=("t3",),
        t3_base_dir=None,
        agent_state_dir=None,
    )


def local_host() -> HostConfig:
    return HostConfig(
        host_id="local-worker",
        enabled=True,
        platform="linux",
        transport="local",
        ssh_alias=None,
        agent_command=(sys.executable,),
        active_limit=1,
        expected_hostname="local-worker",
        t3_url="http://127.0.0.1:3773",
        t3_command=("t3",),
        t3_base_dir=None,
        agent_state_dir=None,
    )


class FakeRunner:
    def __init__(self, *, stdout: str = '{"state":"running"}', returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode
        self.argv = None
        self.stdin = None

    def __call__(self, argv, **kwargs):
        self.argv = argv
        self.stdin = kwargs["input"]
        return subprocess.CompletedProcess(argv, self.returncode, stdout=self.stdout, stderr="")


class TransportTests(unittest.TestCase):
    def test_ssh_uses_fixed_command_and_json_stdin(self) -> None:
        runner = FakeRunner()
        transport = SshTransport(runner=runner)
        request = {
            "action": "status",
            "thread_id": "thread-1",
            "prompt": "caller text; touch /tmp/not-an-argv",
        }
        result = transport.call(host(), request)
        self.assertEqual(
            runner.argv,
            ["ssh", "-o", "BatchMode=yes", "--", "linux-worker", "/opt/t3-fleet-agent"],
        )
        self.assertNotIn("caller text", " ".join(runner.argv))
        self.assertTrue(runner.stdin.endswith("\n"))
        self.assertEqual(runner.stdin.count("\n"), 1)
        self.assertEqual(json.loads(runner.stdin)["action"], "status")
        self.assertEqual(result["state"], "running")

    def test_ssh_quotes_each_posix_remote_command_argument(self) -> None:
        configured = replace(
            host(),
            agent_command=(
                "/opt/Fleet Agent/t3-fleet-agent",
                "--state-dir",
                "/var/lib/t3 fleet",
            ),
        )
        runner = FakeRunner()
        SshTransport(runner=runner).call(configured, {"action": "doctor"})
        self.assertEqual(
            runner.argv,
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "--",
                "linux-worker",
                "'/opt/Fleet Agent/t3-fleet-agent' --state-dir '/var/lib/t3 fleet'",
            ],
        )

    def test_ssh_quotes_windows_remote_command_for_create_process(self) -> None:
        runner = FakeRunner()
        SshTransport(runner=runner).call(windows_host(), {"action": "doctor"})
        self.assertEqual(
            runner.argv,
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "--",
                "windows-worker",
                (
                    "powershell.exe -NoProfile -NonInteractive -File "
                    '"C:\\Users\\Example Operator\\bin\\t3-fleet-agent.ps1"'
                ),
            ],
        )

    def test_ssh_uses_configured_client_executable_as_one_argument(self) -> None:
        runner = FakeRunner()
        SshTransport(
            ssh_command=(r"C:\Program Files\Git\usr\bin\ssh.exe",),
            runner=runner,
        ).call(host(), {"action": "doctor"})
        self.assertEqual(
            runner.argv,
            [
                r"C:\Program Files\Git\usr\bin\ssh.exe",
                "-o",
                "BatchMode=yes",
                "--",
                "linux-worker",
                "/opt/t3-fleet-agent",
            ],
        )

    def test_default_runner_spools_output_before_bounded_read(self) -> None:
        observed = {}

        def emit_oversized_response(argv, **kwargs):
            observed.update(kwargs)
            kwargs["stdout"].write(b"x" * (1024 * 1024 + 1))
            return subprocess.CompletedProcess(argv, 0)

        with patch("t3_fleet.transport.subprocess.run", side_effect=emit_oversized_response):
            with self.assertRaisesRegex(HostUnavailable, "response exceeds"):
                LocalTransport().call(local_host(), {"action": "doctor"})

        self.assertNotEqual(observed["stdin"], subprocess.PIPE)
        self.assertNotEqual(observed["stdout"], subprocess.PIPE)
        self.assertEqual(observed["stderr"], subprocess.DEVNULL)
        self.assertNotIn("input", observed)
        self.assertNotIn("capture_output", observed)

    def test_nonzero_exit_is_unavailable_without_stderr_leak(self) -> None:
        runner = FakeRunner(returncode=255)
        transport = SshTransport(runner=runner)
        with self.assertRaisesRegex(HostUnavailable, "target adapter failed"):
            transport.call(host(), {"action": "doctor"})

    def test_invalid_json_is_rejected(self) -> None:
        transport = SshTransport(runner=FakeRunner(stdout="not-json"))
        with self.assertRaisesRegex(HostUnavailable, "invalid JSON"):
            transport.call(host(), {"action": "doctor"})


if __name__ == "__main__":
    unittest.main()
