from __future__ import annotations

import io
import json
import subprocess
import unittest
import urllib.error

from t3_fleet.errors import T3HttpError
from t3_fleet.t3_api import MAX_RESPONSE_BYTES, MAX_SHELL_RESPONSE_BYTES, T3Client, T3SessionIssuer


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
        self.calls.append(argv)
        if "issue" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "sessionId": "session-1",
                        "token": "secret-token",
                        "method": "bearer-access-token",
                    }
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")


class FakeResponse:
    def __init__(self, payload: object) -> None:
        self.body = io.BytesIO(json.dumps(payload).encode())

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, amount: int) -> bytes:
        return self.body.read(amount)


class RecordingOpener:
    def __init__(self, *, failure: bool = False) -> None:
        self.failure = failure
        self.requests = []

    def __call__(self, request, timeout: float):
        self.requests.append((request, timeout))
        if self.failure:
            raise urllib.error.URLError("server included secret-token")
        return FakeResponse({"ok": True})


class T3ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = FakeRunner()
        self.issuer = T3SessionIssuer(("t3",), runner=self.runner)

    def test_session_is_revoked_after_success(self) -> None:
        with self.issuer.session() as token:
            self.assertEqual(token, "secret-token")
        self.assertEqual(self.runner.calls[-1][-2:], ["revoke", "session-1"])

    def test_session_is_revoked_after_http_error(self) -> None:
        client = T3Client(
            "http://127.0.0.1:3773",
            self.issuer,
            opener=RecordingOpener(failure=True),
        )
        with self.assertRaises(T3HttpError):
            client.get_json("/fail")
        self.assertEqual(self.runner.calls[-1][-2:], ["revoke", "session-1"])

    def test_authorization_value_never_enters_error(self) -> None:
        client = T3Client(
            "http://127.0.0.1:3773",
            self.issuer,
            opener=RecordingOpener(failure=True),
        )
        with self.assertRaises(T3HttpError) as caught:
            client.get_json("/fail")
        self.assertNotIn("secret-token", str(caught.exception))

    def test_authenticated_request_uses_bearer_header(self) -> None:
        opener = RecordingOpener()
        client = T3Client("http://127.0.0.1:3773", self.issuer, opener=opener)
        self.assertEqual(client.get_json("/api/orchestration/snapshot"), {"ok": True})
        request, timeout = opener.requests[0]
        self.assertEqual(request.get_header("Authorization"), "Bearer secret-token")
        self.assertEqual(timeout, 30)

    def test_descriptor_does_not_issue_session(self) -> None:
        opener = RecordingOpener()
        client = T3Client("http://127.0.0.1:3773", self.issuer, opener=opener)
        self.assertEqual(client.descriptor(), {"ok": True})
        self.assertEqual(self.runner.calls, [])

    def test_shell_snapshot_may_exceed_default_response_limit(self) -> None:
        payload = {"threads": [], "padding": "x" * (MAX_RESPONSE_BYTES + 1)}
        opener = lambda request, timeout: FakeResponse(payload)
        client = T3Client("http://127.0.0.1:3773", self.issuer, opener=opener)
        self.assertEqual(client.get_json("/api/orchestration/shell"), payload)

    def test_shell_snapshot_has_its_own_upper_limit(self) -> None:
        payload = {"padding": "x" * (MAX_SHELL_RESPONSE_BYTES + 1)}
        opener = lambda request, timeout: FakeResponse(payload)
        client = T3Client("http://127.0.0.1:3773", self.issuer, opener=opener)
        with self.assertRaises(T3HttpError):
            client.get_json("/api/orchestration/shell")

    def test_thread_projection_may_exceed_default_response_limit(self) -> None:
        payload = {"padding": "x" * (MAX_RESPONSE_BYTES + 1)}
        opener = lambda request, timeout: FakeResponse(payload)
        client = T3Client("http://127.0.0.1:3773", self.issuer, opener=opener)
        self.assertEqual(client.get_json("/api/orchestration/threads/thread-1"), payload)
        # Only the single-thread projection path, not anything nested below it.
        with self.assertRaises(T3HttpError):
            client.get_json("/api/orchestration/threads/thread-1/extra")

    def test_other_paths_keep_default_response_limit(self) -> None:
        payload = {"padding": "x" * (MAX_RESPONSE_BYTES + 1)}
        opener = lambda request, timeout: FakeResponse(payload)
        client = T3Client("http://127.0.0.1:3773", self.issuer, opener=opener)
        with self.assertRaises(T3HttpError):
            client.get_json("/api/orchestration/snapshot")


if __name__ == "__main__":
    unittest.main()
