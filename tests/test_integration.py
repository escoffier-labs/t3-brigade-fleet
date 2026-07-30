from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from t3_fleet.config import FleetConfig, HostConfig, RepositoryConfig
from t3_fleet.controller import FleetController
from t3_fleet.errors import UnknownState
from t3_fleet.store import FleetStore
from t3_fleet.transport import FleetTransport

FULL_SHA = "0123456789abcdef0123456789abcdef01234567"
PROMPT = "private integration prompt 4bbad3cf"
FOLLOWUP_PROMPT = "private follow-up prompt 9120e8ca"
TOKEN = "integration-secret-token-25b83f76"  # content-guard: allow api-key-assignment


class FakeT3State:
    def __init__(self) -> None:
        self.thread: dict[str, Any] | None = None
        self.sequence = 40
        self.dispatch_types: list[str] = []
        self.user_input_commands: list[dict[str, Any]] = []
        self.pending_input = False
        self.authenticated_requests = 0

    def next_sequence(self) -> int:
        self.sequence += 1
        return self.sequence


class FakeT3Handler(BaseHTTPRequestHandler):
    server: "FakeT3Server"

    def log_message(self, format: str, *args: object) -> None:
        return

    def _write(self, status: int, payload: object) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authenticated(self) -> bool:
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            self._write(401, {"error": "unauthorized"})
            return False
        self.server.state.authenticated_requests += 1
        return True

    @staticmethod
    def _project() -> dict[str, Any]:
        return {
            "id": "project-1",
            "workspaceRoot": "/srv/repos/sample",
            "defaultModelSelection": {
                "instanceId": "codex",
                "model": "gpt-5.6-terra",
            },
        }

    def do_GET(self) -> None:
        if self.path == "/.well-known/t3/environment":
            self._write(
                200,
                {
                    "serverVersion": "0.0.30-test",
                    "capabilities": {"threadSettlement": True},
                },
            )
            return
        if not self._authenticated():
            return
        if self.path == "/api/orchestration/snapshot":
            threads = [] if self.server.state.thread is None else [self.server.state.thread]
            self._write(200, {"projects": [self._project()], "threads": threads})
            return
        if self.path == "/api/orchestration/shell":
            threads = []
            if self.server.state.thread is not None:
                shell = dict(self.server.state.thread)
                shell["hasPendingApprovals"] = False
                shell["hasPendingUserInput"] = self.server.state.pending_input
                threads.append(shell)
            self._write(200, {"projects": [self._project()], "threads": threads})
            return
        if self.path.startswith("/api/orchestration/threads/"):
            if self.server.state.thread is None:
                self._write(404, {"error": "not found"})
                return
            self._write(
                200,
                {
                    "snapshotSequence": self.server.state.sequence,
                    "thread": self.server.state.thread,
                },
            )
            return
        self._write(404, {"error": "not found"})

    def do_POST(self) -> None:
        if not self._authenticated():
            return
        if self.path != "/api/orchestration/dispatch":
            self._write(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        command = json.loads(self.rfile.read(length))
        command_type = command["type"]
        self.server.state.dispatch_types.append(command_type)
        if command_type == "thread.create":
            self.server.state.thread = {
                "id": command["threadId"],
                "latestTurn": {"state": "ready"},
                "session": {"status": "ready"},
                "updatedAt": "2026-07-28T02:00:00Z",
                "messages": [],
                "activities": [],
            }
        elif command_type == "thread.meta.update":
            if self.server.state.thread is not None:
                self.server.state.thread["branch"] = command.get("branch")
                self.server.state.thread["worktreePath"] = command.get("worktreePath")
        elif command_type == "thread.turn.start":
            if self.server.state.thread is not None:
                self.server.state.thread["latestTurn"] = {"state": "running"}
                self.server.state.thread["session"] = {"status": "running"}
                if not self.server.state.thread.get("activities"):
                    self.server.state.thread["activities"].append(
                        {
                            "kind": "approval.requested",
                            "payload": {"requestId": "approval-1"},
                        }
                    )
        elif command_type == "thread.turn.interrupt":
            if self.server.state.thread is not None:
                self.server.state.thread["latestTurn"] = {"state": "running"}
        elif command_type == "thread.session.stop":
            if self.server.state.thread is not None:
                self.server.state.thread["session"] = {"status": "stopped"}
        elif command_type == "thread.approval.respond":
            if self.server.state.thread is not None:
                self.server.state.thread["activities"].append(
                    {
                        "kind": "approval.resolved",
                        "payload": {"requestId": command["requestId"]},
                    }
                )
        elif command_type == "thread.user-input.respond":
            self.server.state.user_input_commands.append(
                {
                    "type": command_type,
                    "requestId": command["requestId"],
                    "answers": command["answers"],
                }
            )
            self.server.state.pending_input = False
            if self.server.state.thread is not None:
                self.server.state.thread["activities"].append(
                    {
                        "kind": "user-input.resolved",
                        "payload": {"requestId": command["requestId"]},
                    }
                )
        self._write(200, {"sequence": self.server.state.next_sequence()})


class FakeT3Server(ThreadingHTTPServer):
    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), FakeT3Handler)  # content-guard: allow loopback-ipv4
        self.state = FakeT3State()


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o700)


class FleetIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.server = FakeT3Server()
        self.addCleanup(self.server.server_close)
        self.server_thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.server_thread.start()
        self.addCleanup(self.server.shutdown)

        self.ssh_audit = self.root / "ssh-audit.jsonl"
        self.t3_audit = self.root / "t3-audit.jsonl"
        self.fail_marker = self.root / "fail-submit-once"
        self.fail_marker.touch()
        self.receipt_dir = self.root / "receipts"
        self.fake_t3_cli = self.root / "fake-t3"
        self.adapter = self.root / "target-adapter.py"
        self.fake_ssh = self.root / "ssh"
        self._write_fake_t3_cli()
        self._write_adapter()
        self._write_fake_ssh()

        host = HostConfig(
            host_id="worker-one",
            enabled=True,
            platform="linux",
            transport="ssh",
            ssh_alias="worker-one",
            agent_command=(sys.executable, str(self.adapter)),
            active_limit=1,
            expected_hostname="worker-one",
            t3_url=f"http://127.0.0.1:{self.server.server_port}",  # content-guard: allow loopback-ipv4
            t3_command=(sys.executable, str(self.fake_t3_cli)),
            t3_base_dir=None,
            agent_state_dir=str(self.receipt_dir),
        )
        config = FleetConfig(
            controller_host="worker-one",
            global_active_limit=1,
            state_dir=None,
            hosts={"worker-one": host},
            repositories={
                "sample-repo": RepositoryConfig(
                    repository_id="sample-repo",
                    default_branch="main",
                    branch_prefix="codex/",
                    paths={"worker-one": "/srv/repos/sample"},
                )
            },
        )
        self.db_path = self.root / "controller.db"
        self.store = FleetStore(self.db_path)
        self.controller = FleetController(config, self.store, FleetTransport())
        self.original_path = os.environ.get("PATH", "")
        os.environ["PATH"] = f"{self.root}{os.pathsep}{self.original_path}"
        self.addCleanup(self._restore_path)

    def _restore_path(self) -> None:
        os.environ["PATH"] = self.original_path

    def _write_fake_t3_cli(self) -> None:
        _write_executable(
            self.fake_t3_cli,
            f"""#!{sys.executable}
import json
import sys
from pathlib import Path

audit = Path({str(self.t3_audit)!r})
with audit.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({{"argv": sys.argv[1:]}}) + "\\n")
if "issue" in sys.argv:
    print(json.dumps({{
        "sessionId": "session-1",
        "token": {TOKEN!r},
        "method": "bearer-access-token",
    }}))
""",
        )

    def _write_adapter(self) -> None:
        _write_executable(
            self.adapter,
            f"""#!{sys.executable}
import json
import sys
from pathlib import Path

from t3_fleet.agent import AgentPolicy, RepositoryPolicy, T3Agent
from t3_fleet.t3_api import T3Client, T3SessionIssuer

request = json.loads(sys.stdin.read())
policy = AgentPolicy(
    host_id="worker-one",
    expected_hostname="worker-one",
    platform="linux",
    repositories={{
        "sample-repo": RepositoryPolicy(
            path="/srv/repos/sample",
            default_branch="main",
            branch_prefix="codex/",
        )
    }},
    state_dir=Path({str(self.receipt_dir)!r}),
    active_limit=1,
)
client = T3Client(
    "http://127.0.0.1:{self.server.server_port}",  # content-guard: allow loopback-ipv4
    T3SessionIssuer(({sys.executable!r}, {str(self.fake_t3_cli)!r})),
)


def fake_git_runner(argv, timeout):
    import subprocess
    return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")


agent = T3Agent(
    policy,
    client,
    hostname=lambda: "worker-one",
    platform=lambda: "linux",
    git_runner=fake_git_runner,
)
result = agent.handle(request)
sys.stdout.write(json.dumps(result, separators=(",", ":"), sort_keys=True) + "\\n")
""",
        )

    def _write_fake_ssh(self) -> None:
        _write_executable(
            self.fake_ssh,
            f"""#!{sys.executable}
import json
import shlex
import subprocess
import sys
from pathlib import Path

payload = sys.stdin.read()
request = json.loads(payload)
remote_argv = shlex.split(sys.argv[-1])
completed = subprocess.run(
    remote_argv,
    input=payload,
    capture_output=True,
    text=True,
    check=False,
)
audit = {{
    "action": request.get("action"),
    "argv": sys.argv[1:],
    "child_stderr": completed.stderr,
    "returncode": completed.returncode,
}}
with Path({str(self.ssh_audit)!r}).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(audit, sort_keys=True) + "\\n")
marker = Path({str(self.fail_marker)!r})
if request.get("action") == "submit" and marker.exists():
    marker.unlink()
    raise SystemExit(255)
sys.stdout.write(completed.stdout)
sys.stderr.write(completed.stderr)
raise SystemExit(completed.returncode)
""",
        )

    @staticmethod
    def _json_lines(path: Path) -> list[dict[str, Any]]:
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line
        ]

    def test_real_stack_recovers_unknown_submit_and_redacts_secrets(self) -> None:
        request = {
            "request_id": "req-12345678",
            "host": "worker-one",
            "repository": "sample-repo",
            "source_revision": FULL_SHA,
            "title": "Integration canary",
            "runtime_mode": "approval-required",
            "interaction_mode": "default",
        }

        with self.assertRaises(UnknownState) as caught:
            self.controller.submit(request, prompt=PROMPT)
        unknown_record = self.store.get("req-12345678")
        self.assertEqual(unknown_record.state, "unknown")
        self.assertIsNone(unknown_record.thread_id)

        running_results = [
            self.controller.recover("req-12345678"),
            self.controller.status("req-12345678"),
            self.controller.followup(
                "req-12345678",
                prompt=FOLLOWUP_PROMPT,
            ),
            self.controller.approve(
                "req-12345678",
                "approval-1",
                "accept",
            ),
        ]
        self.server.state.pending_input = True
        thread = self.server.state.thread
        if thread is None:
            self.fail("submit did not create the fake T3 thread")
        thread["activities"].append(
            {
                "kind": "user-input.requested",
                "payload": {"requestId": "input-1"},
            }
        )
        attention = self.controller.status("req-12345678")
        self.assertEqual(attention["state"], "needs_attention")
        self.assertTrue(attention["needs_attention"])

        structured_answers = {
            "question-1": ["Option A"],
            "question-2": ["A typed response"],
        }
        answered = self.controller.answer(
            "req-12345678",
            "input-1",
            structured_answers,
        )
        self.assertEqual(answered["state"], "running")
        self.assertFalse(answered["needs_attention"])
        self.assertEqual(self.store.get("req-12345678").state, "running")
        self.assertEqual(
            self.server.state.user_input_commands,
            [
                {
                    "type": "thread.user-input.respond",
                    "requestId": "input-1",
                    "answers": structured_answers,
                }
            ],
        )

        # Interrupt after the user-input is resolved. Once the session is
        # stopped, status normalizes to interrupted even if T3 leaves a stale
        # pending marker, so the stored active request releases.
        interrupted_result = self.controller.interrupt("req-12345678")
        self.assertEqual(interrupted_result["state"], "running")

        recovered = self.controller.recover("req-12345678")
        results = [*running_results, attention, answered, interrupted_result, recovered]
        self.assertTrue(
            all(result["state"] == "running" for result in running_results)
        )
        self.assertEqual(recovered["state"], "interrupted")
        self.assertTrue(all(result["thread_id"] for result in results))
        self.assertEqual(
            self.server.state.dispatch_types,
            [
                "thread.create",
                "thread.meta.update",
                "thread.turn.start",
                "thread.turn.start",
                "thread.approval.respond",
                "thread.user-input.respond",
                "thread.turn.interrupt",
                "thread.session.stop",
            ],
        )
        self.assertGreater(self.server.state.authenticated_requests, 0)

        ssh_audit = self._json_lines(self.ssh_audit)
        self.assertEqual(
            [entry["action"] for entry in ssh_audit],
            [
                "doctor",
                "submit",
                "recover",
                "status",
                "followup",
                "approve",
                "status",
                "answer",
                "interrupt",
                "recover",
            ],
        )
        self.assertTrue(all(entry["child_stderr"] == "" for entry in ssh_audit))
        t3_audit = self._json_lines(self.t3_audit)
        issue_count = sum("issue" in entry["argv"] for entry in t3_audit)
        revoke_count = sum("revoke" in entry["argv"] for entry in t3_audit)
        self.assertEqual(issue_count, revoke_count)
        self.assertGreater(issue_count, 0)

        with sqlite3.connect(self.db_path) as connection:
            rows = {
                table: connection.execute(f"SELECT * FROM {table}").fetchall()
                for table in ("requests", "events", "leases", "host_failures")
            }
        controller_db = repr(rows)
        receipts = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted(self.receipt_dir.glob("*.json"))
        )
        argv_and_stderr = json.dumps(
            {
                "ssh": ssh_audit,
                "t3": t3_audit,
            },
            sort_keys=True,
        )
        json_results = json.dumps(results, sort_keys=True)
        exception_text = str(caught.exception)
        for secret in (PROMPT, FOLLOWUP_PROMPT, TOKEN):
            self.assertNotIn(secret, controller_db)
            self.assertNotIn(secret, receipts)
            self.assertNotIn(secret, argv_and_stderr)
            self.assertNotIn(secret, json_results)
            self.assertNotIn(secret, exception_text)


if __name__ == "__main__":
    unittest.main()
