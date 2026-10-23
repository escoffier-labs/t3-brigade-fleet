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


INVALID_ORIGINS = (
    None, 42, [], {}, "", "http://example.invalid:3773", "https://example.invalid",
    "http://localhost:3773", "http://127.0.0.1.example.invalid", "ftp://127.0.0.1",
    "http://user:private@127.0.0.1", "http://@127.0.0.1", "http://127.0.0.1/path",
    "http://127.0.0.1?private", "http://127.0.0.1#private", "http://127.0.0.1?", "http://127.0.0.1#",
    "http://127.0.0.1:bad", "http://127.0.0.1:65536", "http://127.0.0.1:0", "http://127.0.0.1:",
    "http://[::1", "http://[::1%25lo]:3773", "http://127.1", "http://2130706433",
    " http://127.0.0.1", "http://127.0.0.1\n", "http://127.0.0.1\t:3773",
)
VALID_ORIGINS = ("http://127.0.0.1:3773", "https://127.12.34.56/", "http://[::1]:3773/", "https://[::1]")


class T3ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = FakeRunner()
        self.issuer = T3SessionIssuer(("t3",), runner=self.runner)

    def test_invalid_origin_never_issues_token_or_opens(self) -> None:
        for origin in INVALID_ORIGINS:
            with self.subTest(origin=origin):
                opener = RecordingOpener()
                with self.assertRaises(T3HttpError) as caught:
                    T3Client(origin, self.issuer, opener=opener).get_json("/data")
                self.assertEqual(str(caught.exception), "invalid T3 origin")
                self.assertEqual(self.runner.calls, [])
                self.assertEqual(opener.requests, [])

    def test_literal_loopback_origins_are_supported(self) -> None:
        for origin in VALID_ORIGINS:
            with self.subTest(origin=origin):
                opener = RecordingOpener()
                client = T3Client(origin, self.issuer, opener=opener)
                self.assertEqual(client.get_json("/data"), {"ok": True})
                self.assertEqual(opener.requests[0][0].get_header("Authorization"), "Bearer secret-token")

    def test_production_opener_does_not_forward_bearer_on_redirect(self) -> None:
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from threading import Thread
        received = []
        class Destination(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append(self.headers.get("Authorization"))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{}')
            def log_message(self, *args):
                pass
        destination = ThreadingHTTPServer(("127.0.0.1", 0), Destination)
        class Redirect(Destination):
            def do_GET(self):
                self.server.authorization = self.headers.get("Authorization")
                self.send_response(self.server.redirect_code)
                self.send_header("Location", f"http://127.0.0.1:{destination.server_port}/data")
                self.end_headers()
        redirect = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
        servers = (destination, redirect)
        workers = [Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True) for server in servers]
        for worker in workers:
            worker.start()
        try:
            for code in (301, 302, 303, 307, 308):
                with self.subTest(code=code):
                    redirect.redirect_code = code
                    client = T3Client(f"http://127.0.0.1:{redirect.server_port}", self.issuer)
                    with self.assertRaises(T3HttpError):
                        client.get_json("/data")
                    self.assertEqual(redirect.authorization, "Bearer secret-token")
                    self.assertEqual(received, [])
                    self.assertEqual(self.runner.calls[-1][-2:], ["revoke", "session-1"])
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()
            for worker in workers:
                worker.join(2)

    def test_production_opener_connects_directly_despite_proxy_environment(self):
        import os
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from threading import Thread
        from unittest.mock import patch

        destination_seen, proxy_seen = [], []

        class Destination(BaseHTTPRequestHandler):
            def do_GET(self):
                destination_seen.append((self.path, self.headers.get("Authorization")))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"ok": true}')

            def log_message(self, *args):
                pass

        class Proxy(Destination):
            def do_GET(self):
                proxy_seen.append((self.path, self.headers.get("Authorization")))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"ok": true}')

        destination = ThreadingHTTPServer(("127.0.0.1", 0), Destination)
        proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        servers = (destination, proxy)
        workers = [
            Thread(
                target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
            )
            for server in servers
        ]
        for worker in workers:
            worker.start()
        try:
            proxy_url = f"http://127.0.0.1:{proxy.server_port}"
            with patch.dict(
                os.environ,
                {
                    "http_proxy": proxy_url,
                    "https_proxy": proxy_url,
                    "all_proxy": proxy_url,
                },
                clear=True,
            ):
                client = T3Client(
                    f"http://127.0.0.1:{destination.server_port}", self.issuer
                )
                self.assertEqual(client.get_json("/data"), {"ok": True})
            self.assertEqual(proxy_seen, [])
            self.assertEqual(destination_seen, [("/data", "Bearer secret-token")])
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()
            for worker in workers:
                worker.join(2)

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
