from __future__ import annotations

import contextlib
import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from typing import Any

from .errors import T3AuthError, T3HttpError

MAX_RESPONSE_BYTES = 1024 * 1024

Runner = Callable[[list[str], float], subprocess.CompletedProcess[str]]
Opener = Callable[..., Any]


def _run(argv: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        shell=False,
    )


class T3SessionIssuer:
    def __init__(
        self,
        t3_command: tuple[str, ...],
        *,
        base_dir: str | None = None,
        runner: Runner = _run,
    ) -> None:
        if not t3_command:
            raise T3AuthError("T3 command is empty")
        self._command = list(t3_command)
        self._base_dir = base_dir
        self._runner = runner

    def _base_args(self) -> list[str]:
        return ["--base-dir", self._base_dir] if self._base_dir else []

    @contextlib.contextmanager
    def session(self) -> Iterator[str]:
        issue_argv = [
            *self._command,
            "auth",
            "session",
            "issue",
            "--ttl",
            "2m",
            "--label",
            "t3-fleet-agent",
            "--subject",
            "t3-fleet-agent",
            "--json",
            *self._base_args(),
        ]
        try:
            completed = self._runner(issue_argv, 15)
        except (OSError, subprocess.SubprocessError) as error:
            raise T3AuthError("could not issue a target-local T3 session") from error
        if completed.returncode != 0:
            raise T3AuthError("T3 rejected target-local session issuance")
        try:
            payload = json.loads(completed.stdout)
            session_id = payload["sessionId"]
            token = payload["token"]
            if not isinstance(session_id, str) or not session_id:
                raise ValueError("missing session id")
            if not isinstance(token, str) or not token:
                raise ValueError("missing token")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            raise T3AuthError("T3 returned an invalid session response") from error

        try:
            yield token
        finally:
            revoke_argv = [
                *self._command,
                "auth",
                "session",
                "revoke",
                session_id,
                *self._base_args(),
            ]
            try:
                self._runner(revoke_argv, 15)
            except (OSError, subprocess.SubprocessError):
                pass


class T3Client:
    def __init__(
        self,
        base_url: str,
        issuer: T3SessionIssuer,
        *,
        opener: Opener = urllib.request.urlopen,
    ) -> None:
        self._base_url = base_url.rstrip("/") + "/"
        self._issuer = issuer
        self._opener = opener

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        authenticated: bool = True,
        timeout: float = 30,
    ) -> Any:
        if not path.startswith("/"):
            raise T3HttpError("T3 API path must be absolute")
        body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        session = self._issuer.session() if authenticated else contextlib.nullcontext(None)
        with session as token:
            headers = {"Accept": "application/json"}
            if body is not None:
                headers["Content-Type"] = "application/json"
            if token is not None:
                headers["Authorization"] = f"Bearer {token}"
            url = urllib.parse.urljoin(self._base_url, path.lstrip("/"))
            request = urllib.request.Request(url, data=body, headers=headers, method=method)
            try:
                with self._opener(request, timeout=timeout) as response:
                    raw = response.read(MAX_RESPONSE_BYTES + 1)
            except urllib.error.HTTPError as error:
                raise T3HttpError(f"T3 API returned HTTP {error.code} for {path}") from error
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                raise T3HttpError(f"T3 API request failed for {path}") from error
            if len(raw) > MAX_RESPONSE_BYTES:
                raise T3HttpError(f"T3 API response exceeded {MAX_RESPONSE_BYTES} bytes")
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise T3HttpError(f"T3 API returned invalid JSON for {path}") from error

    def descriptor(self) -> dict[str, Any]:
        return self._request(
            "/.well-known/t3/environment",
            authenticated=False,
            timeout=5,
        )

    def get_json(self, path: str) -> Any:
        return self._request(path)

    def post_json(self, path: str, payload: dict[str, Any]) -> Any:
        return self._request(path, method="POST", payload=payload)

    def snapshot(self) -> dict[str, Any]:
        return self._request("/api/orchestration/snapshot")

    def shell_snapshot(self) -> dict[str, Any]:
        return self._request("/api/orchestration/shell")

    def thread_snapshot(self, thread_id: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(thread_id, safe="")
        return self._request(f"/api/orchestration/threads/{encoded}")

    def dispatch(self, command: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "/api/orchestration/dispatch",
            method="POST",
            payload=command,
        )
