from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

from t3_fleet.agent import AgentPolicy, RepositoryPolicy, T3Agent
from t3_fleet.errors import UnknownState, ValidationError

FULL_SHA = "0123456789abcdef0123456789abcdef01234567"


class FakeT3:
    def __init__(self) -> None:
        self.dispatched: list[dict] = []
        self.server_version = "0.0.30"
        self.capabilities = {"threadSettlement": True}
        self.projects = [
            {
                "id": "project-1",
                "workspaceRoot": "/srv/repos/example",
                "defaultModelSelection": {"instanceId": "codex", "model": "gpt-5.6-terra"},
            }
        ]
        self.thread = {
            "id": "thread-1",
            "latestTurn": {"state": "running"},
            "session": {"status": "running"},
            "updatedAt": "2026-07-28T01:00:00Z",
            "messages": [],
        }
        self.pending_approval = False
        self.pending_input = False
        self.fail_after: int | None = None
        self.worktree_paths: list[str] = []

    def descriptor(self) -> dict:
        return {
            "serverVersion": self.server_version,
            "capabilities": self.capabilities,
        }

    def snapshot(self) -> dict:
        raise AssertionError("agent must use the bounded shell snapshot")

    def shell_snapshot(self) -> dict:
        shell = dict(self.thread)
        shell["hasPendingApprovals"] = self.pending_approval
        shell["hasPendingUserInput"] = self.pending_input
        return {"projects": self.projects, "threads": [shell]}

    def thread_snapshot(self, thread_id: str) -> dict:
        return {"snapshotSequence": 7, "thread": self.thread}

    def dispatch(self, command: dict) -> dict:
        if self.fail_after is not None and len(self.dispatched) >= self.fail_after:
            raise RuntimeError("dispatch failed mid-sequence")
        self.dispatched.append(command)
        return {"sequence": 8}


class StuckApprovalT3:
    """Models the live stuck-canary defect: thread.turn.interrupt plus
    thread.session.stop cannot settle a turn while an open approval.requested
    activity remains unresolved. A thread.approval.respond decision=cancel
    resolves the approval and lets a subsequent interrupt/stop settle the turn.
    A stale already-resolved approval is included to prove it is not re-cancelled."""

    def __init__(self, *, open_approval_ids: tuple[str, ...] = ()) -> None:
        self.dispatched: list[dict] = []
        self.open_approvals: set[str] = set(open_approval_ids)
        activities: list[dict] = [
            {
                "kind": "approval.requested",
                "payload": {"requestId": approval_id},
            }
            for approval_id in sorted(self.open_approvals)
        ]
        activities.extend(
            [
                {
                    "kind": "approval.requested",
                    "payload": {"requestId": "approval-stale"},
                },
                {
                    "kind": "approval.resolved",
                    "payload": {"requestId": "approval-stale"},
                },
            ]
        )
        self.thread = {
            "id": "thread-1",
            "latestTurn": {"state": "running"},
            "session": {"status": "running"},
            "updatedAt": "2026-07-28T01:00:00Z",
            "messages": [],
            "activities": activities,
        }

    def descriptor(self) -> dict:
        return {
            "serverVersion": "0.0.30",
            "capabilities": {"threadSettlement": True},
        }

    def shell_snapshot(self) -> dict:
        shell = dict(self.thread)
        shell["hasPendingApprovals"] = bool(self.open_approvals)
        shell["hasPendingUserInput"] = False
        return {"projects": [], "threads": [shell]}

    def thread_snapshot(self, thread_id: str) -> dict:
        return {"snapshotSequence": 7, "thread": self.thread}

    def dispatch(self, command: dict) -> dict:
        self.dispatched.append(command)
        ctype = command["type"]
        if ctype == "thread.approval.respond":
            approval_id = command["requestId"]
            if approval_id in self.open_approvals and command["decision"] == "cancel":
                self.open_approvals.discard(approval_id)
                self.thread["activities"].append(
                    {
                        "kind": "approval.resolved",
                        "payload": {"requestId": approval_id},
                    }
                )
        elif ctype == "thread.turn.interrupt":
            if not self.open_approvals:
                self.thread["latestTurn"] = {"state": "interrupted"}
        elif ctype == "thread.session.stop":
            if not self.open_approvals:
                self.thread["session"] = {"status": "stopped"}
        return {"sequence": 8}


class TurnIdProjectionT3:
    """Models T3 ProjectionPipeline's "thread.turn-interrupt-requested"
    handler: it returns early when event.payload.turnId is undefined, so
    thread.turn.interrupt without a turnId leaves latestTurn projected as
    running. When the command carries the active latestTurn.turnId the
    projection settles the turn to interrupted and a subsequent session stop
    takes effect."""

    def __init__(self, *, turn_id: str) -> None:
        self.dispatched: list[dict] = []
        self.thread = {
            "id": "thread-1",
            "latestTurn": {"turnId": turn_id, "state": "running"},
            "session": {"status": "running"},
            "updatedAt": "2026-07-28T01:00:00Z",
            "messages": [],
            "activities": [],
        }

    def descriptor(self) -> dict:
        return {
            "serverVersion": "0.0.30",
            "capabilities": {"threadSettlement": True},
        }

    def shell_snapshot(self) -> dict:
        shell = dict(self.thread)
        shell["hasPendingApprovals"] = False
        shell["hasPendingUserInput"] = False
        return {"projects": [], "threads": [shell]}

    def thread_snapshot(self, thread_id: str) -> dict:
        return {"snapshotSequence": 7, "thread": self.thread}

    def dispatch(self, command: dict) -> dict:
        self.dispatched.append(command)
        ctype = command["type"]
        if ctype == "thread.turn.interrupt":
            turn_id = command.get("turnId")
            if turn_id == self.thread["latestTurn"].get("turnId"):
                self.thread["latestTurn"] = {
                    "turnId": turn_id,
                    "state": "interrupted",
                }
        elif ctype == "thread.session.stop":
            if self.thread["latestTurn"]["state"] == "interrupted":
                self.thread["session"] = {"status": "stopped"}
        return {"sequence": 8}


class StaleShellProjectionT3(FakeT3):
    """Shell and detail thread snapshots diverge: shell keeps a stale running
    latestTurn while the authoritative detail thread has settled."""

    def __init__(
        self,
        *,
        shell_latest_turn: dict[str, Any],
        detail_latest_turn: dict[str, Any],
        session: dict[str, Any] | None = None,
        messages: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__()
        self.shell_latest_turn = shell_latest_turn
        self.detail_latest_turn = detail_latest_turn
        if session is not None:
            self.thread["session"] = session
        if messages is not None:
            self.thread["messages"] = messages

    def shell_snapshot(self) -> dict:
        shell = dict(self.thread)
        shell["latestTurn"] = self.shell_latest_turn
        shell["hasPendingApprovals"] = self.pending_approval
        shell["hasPendingUserInput"] = self.pending_input
        return {"projects": self.projects, "threads": [shell]}

    def thread_snapshot(self, thread_id: str) -> dict:
        detail = dict(self.thread)
        detail["latestTurn"] = self.detail_latest_turn
        return {"snapshotSequence": 7, "thread": detail}


class MalformedDetailProjectionT3(StaleShellProjectionT3):
    def __init__(self, detail_snapshot: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.detail_snapshot = detail_snapshot

    def thread_snapshot(self, thread_id: str) -> Any:
        return self.detail_snapshot


class AgentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.fake_t3 = FakeT3()
        self.git_calls: list[list[str]] = []
        self.git_fail_after: int | None = None

        def fake_git_runner(argv: list[str], timeout: float) -> Any:
            self.git_calls.append(list(argv))
            if self.git_fail_after is not None and len(self.git_calls) > self.git_fail_after:
                return subprocess.CompletedProcess(argv, 128, stdout="", stderr="worktree boom")
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        uuid_pool = iter(
            [
                "turn-cmd-1",
                "thread-1",
                "message-1",
                "create-cmd-1",
                "update-cmd-1",
                "turn-cmd-2",
                "thread-2",
                "message-2",
                "create-cmd-2",
                "update-cmd-2",
            ]
        )

        def uuid_factory() -> str:
            return next(uuid_pool)

        policy = AgentPolicy(
            host_id="linux-worker",
            expected_hostname="linux-worker",
            platform="linux",
            repositories={
                "example-repo": RepositoryPolicy(
                    path="/srv/repos/example",
                    default_branch="main",
                    branch_prefix="codex/",
                )
            },
            state_dir=Path(self.directory.name),
            active_limit=2,
        )
        self.agent = T3Agent(
            policy,
            self.fake_t3,
            hostname=lambda: "linux-worker",
            uuid_factory=uuid_factory,
            now=lambda: "2026-07-28T01:00:00Z",
            git_runner=fake_git_runner,
        )
        self.submit_request = {
            "action": "submit",
            "request_id": "req-12345678",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "prompt": "private prompt text",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
        }

    def test_repository_policy_carries_default_branch(self) -> None:
        repository = self.agent.policy.repositories["example-repo"]
        self.assertEqual(repository.default_branch, "main")

    def test_submit_sequences_thread_worktree_metadata_and_turn(self) -> None:
        result = self.agent.handle(self.submit_request)
        types = [command["type"] for command in self.fake_t3.dispatched]
        self.assertEqual(
            types,
            ["thread.create", "thread.meta.update", "thread.turn.start"],
        )
        create_command = self.fake_t3.dispatched[0]
        self.assertEqual(create_command["threadId"], "thread-1")
        self.assertEqual(create_command["projectId"], "project-1")
        self.assertEqual(create_command["title"], "Fix issue 123")
        self.assertEqual(create_command["runtimeMode"], "full-access")
        self.assertIsNone(create_command["branch"])
        self.assertIsNone(create_command["worktreePath"])
        update_command = self.fake_t3.dispatched[1]
        self.assertEqual(update_command["threadId"], "thread-1")
        self.assertEqual(update_command["branch"], "codex/req-12345678")
        self.assertNotIn("private prompt text", json.dumps(update_command))
        turn_command = self.fake_t3.dispatched[2]
        self.assertEqual(turn_command["type"], "thread.turn.start")
        self.assertEqual(turn_command["threadId"], "thread-1")
        self.assertEqual(turn_command["message"]["text"], "private prompt text")
        self.assertNotIn("bootstrap", turn_command)
        self.assertEqual(len(self.git_calls), 2)
        git_argv = self.git_calls[0]
        self.assertEqual(git_argv[0], "git")
        self.assertEqual(git_argv[1], "-C")
        self.assertEqual(git_argv[2], "/srv/repos/example")
        self.assertEqual(git_argv[3], "worktree")
        self.assertEqual(git_argv[4], "add")
        self.assertEqual(git_argv[5], "-b")
        self.assertEqual(git_argv[6], "codex/req-12345678")
        worktree_path = git_argv[7]
        self.assertIn("req-12345678", worktree_path)
        self.assertEqual(git_argv[8], FULL_SHA)
        self.assertEqual(
            self.git_calls[1],
            [
                "git",
                "-C",
                "/srv/repos/example",
                "config",
                "branch.codex/req-12345678.gh-merge-base",
                "main",
            ],
        )
        self.assertEqual(update_command["worktreePath"], worktree_path)
        self.assertNotIn("private prompt text", json.dumps(result))
        self.assertNotIn("/srv/repos/example", json.dumps(result))

    def test_submit_rejects_setup_script_before_mutation(self) -> None:
        request = dict(self.submit_request)
        request["run_setup_script"] = True

        with self.assertRaisesRegex(
            ValidationError,
            "run_setup_script is unavailable",
        ):
            self.agent.handle(request)

        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.git_calls, [])

    def test_submit_receipt_omits_prompt_and_paths(self) -> None:
        self.agent.handle(self.submit_request)
        receipt = next(Path(self.directory.name).glob("*.json")).read_text(encoding="utf-8")
        self.assertNotIn("private prompt text", receipt)
        self.assertNotIn("/srv/repos/example", receipt)
        self.assertIn("req-12345678", receipt)

    def test_submit_persists_thread_identity_before_dispatch(self) -> None:
        observed_receipts: list[dict] = []

        def failing_dispatch(command: dict) -> dict:
            receipt_path = Path(self.directory.name) / "req-12345678-submit.json"
            observed_receipts.append(json.loads(receipt_path.read_text(encoding="utf-8")))
            raise RuntimeError("dispatch connection closed")

        self.fake_t3.dispatch = failing_dispatch

        with self.assertRaisesRegex(RuntimeError, "connection closed"):
            self.agent.handle(self.submit_request)

        self.assertEqual(observed_receipts[0]["thread_id"], "thread-1")
        self.assertEqual(observed_receipts[0]["outcome"], "dispatching")
        stored = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(stored["thread_id"], "thread-1")
        self.assertEqual(stored["outcome"], "unknown")
        self.assertNotIn("private prompt text", json.dumps(stored))
        self.assertNotIn("/srv/repos/example", json.dumps(stored))
        recovered = self.agent.handle(
            {
                "action": "recover",
                "request_id": "req-12345678",
            }
        )
        self.assertEqual(recovered["thread_id"], "thread-1")

    def test_submit_finalizes_receipt_after_dispatch_acceptance(self) -> None:
        self.agent.handle(self.submit_request)
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["thread_id"], "thread-1")
        self.assertEqual(receipt["sequence"], 8)
        self.assertEqual(receipt["outcome"], "accepted")

    def test_submit_cleans_up_worktree_when_thread_update_fails(self) -> None:
        self.fake_t3.fail_after = 1
        with self.assertRaises(RuntimeError):
            self.agent.handle(self.submit_request)
        types = [command["type"] for command in self.fake_t3.dispatched]
        self.assertEqual(types, ["thread.create"])
        self.assertEqual(len(self.git_calls), 3)
        add_argv = self.git_calls[0]
        self.assertEqual(add_argv[3], "worktree")
        self.assertEqual(add_argv[4], "add")
        self.assertEqual(add_argv[6], "codex/req-12345678")
        self.assertEqual(add_argv[8], FULL_SHA)
        remove_argv = self.git_calls[2]
        self.assertEqual(remove_argv[3], "worktree")
        self.assertEqual(remove_argv[4], "remove")
        self.assertEqual(remove_argv[5], "--force")
        self.assertEqual(remove_argv[6], add_argv[7])
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["outcome"], "unknown")
        self.assertEqual(receipt["thread_id"], "thread-1")
        self.assertNotIn("private prompt text", json.dumps(receipt))

    def test_submit_preserves_worktree_when_turn_response_is_lost(self) -> None:
        dispatch = self.fake_t3.dispatch

        def lose_turn_response(command: dict) -> dict:
            response = dispatch(command)
            if command["type"] == "thread.turn.start":
                raise RuntimeError("turn response lost after acceptance")
            return response

        self.fake_t3.dispatch = lose_turn_response

        with self.assertRaisesRegex(RuntimeError, "response lost"):
            self.agent.handle(self.submit_request)

        self.assertEqual(
            [command["type"] for command in self.fake_t3.dispatched],
            ["thread.create", "thread.meta.update", "thread.turn.start"],
        )
        self.assertFalse(
            any(
                call[3:5] == ["worktree", "remove"]
                for call in self.git_calls
            )
        )

    def test_submit_cleans_up_worktree_when_git_worktree_add_fails(self) -> None:
        self.git_fail_after = 0
        with self.assertRaises(UnknownState):
            self.agent.handle(self.submit_request)
        types = [command["type"] for command in self.fake_t3.dispatched]
        self.assertEqual(types, ["thread.create"])
        self.assertEqual(len(self.git_calls), 1)
        self.assertEqual(self.git_calls[0][4], "add")
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["outcome"], "unknown")
        self.assertNotIn("worktree boom", json.dumps(receipt))

    def test_submit_cleans_up_worktree_when_merge_base_configuration_fails(
        self,
    ) -> None:
        self.git_fail_after = 1

        with self.assertRaisesRegex(UnknownState, "configuration failed"):
            self.agent.handle(self.submit_request)

        self.assertEqual(
            [call[3] for call in self.git_calls],
            ["worktree", "config", "worktree"],
        )
        self.assertEqual(self.git_calls[-1][4:6], ["remove", "--force"])

    def test_status_normalizes_pending_attention(self) -> None:
        self.fake_t3.pending_approval = True
        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})
        self.assertEqual(result["state"], "needs_attention")
        self.assertTrue(result["needs_attention"])

    def test_interrupted_state_outranks_stale_pending_approval(self) -> None:
        # After interrupt is accepted, T3 leaves a stale pending approval marker
        # even though latestTurn.state=interrupted and session.status=stopped.
        # _status must normalize to interrupted so the target/controller release.
        for label, latest_turn, session in (
            (
                "latestTurn interrupted",
                {"state": "interrupted"},
                {"status": "stopped"},
            ),
            (
                "session stopped",
                {"state": "running"},
                {"status": "stopped"},
            ),
            (
                "both interrupted and stopped",
                {"state": "interrupted"},
                {"status": "stopped"},
            ),
        ):
            with self.subTest(label=label):
                self.fake_t3.pending_approval = True
                self.fake_t3.thread["latestTurn"] = latest_turn
                self.fake_t3.thread["session"] = session
                result = self.agent.handle(
                    {"action": "status", "thread_id": "thread-1"}
                )
                self.assertEqual(result["state"], "interrupted")
                self.assertFalse(result["needs_attention"])

    def test_pending_approval_still_needs_attention_for_active_session(self) -> None:
        # A genuinely active session with a pending approval stays needs_attention.
        self.fake_t3.pending_approval = True
        self.fake_t3.thread["latestTurn"] = {"state": "running"}
        self.fake_t3.thread["session"] = {"status": "running"}
        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})
        self.assertEqual(result["state"], "needs_attention")
        self.assertTrue(result["needs_attention"])

    def test_status_normalizes_completed_turn(self) -> None:
        self.fake_t3.thread["latestTurn"] = {"state": "completed"}
        self.fake_t3.thread["session"] = {"status": "ready"}
        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})
        self.assertEqual(result["state"], "succeeded")

    def test_status_uses_matching_completed_detail_turn_when_shell_is_stale(
        self,
    ) -> None:
        self.fake_t3 = StaleShellProjectionT3(
            shell_latest_turn={"turnId": "turn-1", "state": "running"},
            detail_latest_turn={"turnId": "turn-1", "state": "completed"},
            session={"status": "ready"},
        )
        self.agent.t3 = self.fake_t3

        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})

        self.assertEqual(result["state"], "succeeded")

    def test_status_final_assistant_text_alone_stays_unknown_when_turn_is_stale(
        self,
    ) -> None:
        self.fake_t3 = StaleShellProjectionT3(
            shell_latest_turn={"turnId": "turn-1", "state": "running"},
            detail_latest_turn={"turnId": "turn-1", "state": "running"},
            session={"status": "ready"},
            messages=[
                {
                    "role": "assistant",
                    "text": "finished",
                    "streaming": False,
                }
            ],
        )
        self.agent.t3 = self.fake_t3

        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})

        self.assertEqual(result["state"], "unknown")

    def test_status_mismatched_detail_turn_id_stays_unknown(self) -> None:
        self.fake_t3 = StaleShellProjectionT3(
            shell_latest_turn={"turnId": "turn-1", "state": "running"},
            detail_latest_turn={"turnId": "turn-2", "state": "completed"},
            session={"status": "ready"},
        )
        self.agent.t3 = self.fake_t3

        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})

        self.assertEqual(result["state"], "unknown")

    def test_status_malformed_detail_snapshot_returns_unknown(self) -> None:
        stale_shell = {
            "shell_latest_turn": {"turnId": "turn-1", "state": "running"},
            "detail_latest_turn": {"turnId": "turn-1", "state": "completed"},
            "session": {"status": "ready"},
        }
        for label, detail_snapshot in (
            ("missing", None),
            ("null_thread", {"thread": None}),
            ("non_mapping_thread", {"thread": "bad"}),
            ("missing_thread_key", {}),
        ):
            with self.subTest(label=label):
                self.fake_t3 = MalformedDetailProjectionT3(
                    detail_snapshot,
                    **stale_shell,
                )
                self.agent.t3 = self.fake_t3

                result = self.agent.handle({"action": "status", "thread_id": "thread-1"})

                self.assertEqual(result["state"], "unknown")

    def test_status_does_not_use_assistant_output_before_latest_user_message(
        self,
    ) -> None:
        self.fake_t3.thread["latestTurn"] = {"state": "running"}
        self.fake_t3.thread["session"] = {"status": "ready"}
        self.fake_t3.thread["messages"] = [
            {
                "role": "assistant",
                "text": "older turn finished",
                "streaming": False,
            },
            {
                "role": "user",
                "text": "new follow-up",
                "streaming": False,
            },
        ]

        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})

        self.assertEqual(result["state"], "unknown")

    def test_recover_marks_thread_without_turn_or_active_session_unknown(self) -> None:
        self.agent.handle(self.submit_request)
        self.fake_t3.thread["latestTurn"] = None
        self.fake_t3.thread["session"] = None

        result = self.agent.handle(
            {
                "action": "recover",
                "request_id": "req-12345678",
            }
        )

        self.assertEqual(result["state"], "unknown")

    def test_recover_marks_aged_unknown_partial_thread_failed(self) -> None:
        self.fake_t3.fail_after = 1
        with self.assertRaises(RuntimeError):
            self.agent.handle(self.submit_request)
        self.fake_t3.fail_after = None
        self.fake_t3.thread["latestTurn"] = None
        self.fake_t3.thread["session"] = None
        self.agent._now = lambda: "2026-07-28T01:01:00Z"

        result = self.agent.handle(
            {
                "action": "recover",
                "request_id": "req-12345678",
            }
        )

        self.assertEqual(result["state"], "failed")

    def test_recover_marks_aged_unknown_missing_thread_failed(self) -> None:
        def fail_create(command: dict) -> dict:
            raise RuntimeError("create response lost")

        self.fake_t3.dispatch = fail_create
        with self.assertRaises(RuntimeError):
            self.agent.handle(self.submit_request)
        self.fake_t3.shell_snapshot = lambda: {
            "projects": self.fake_t3.projects,
            "threads": [],
        }
        self.agent._now = lambda: "2026-07-28T01:01:00Z"

        result = self.agent.handle(
            {
                "action": "recover",
                "request_id": "req-12345678",
            }
        )

        self.assertEqual(result["state"], "failed")

    def test_host_identity_mismatch_fails_closed(self) -> None:
        self.agent._hostname = lambda: "unexpected-host"
        with self.assertRaisesRegex(ValidationError, "identity mismatch"):
            self.agent.handle({"action": "doctor"})

    def test_platform_identity_mismatch_fails_closed(self) -> None:
        self.agent._platform = lambda: "win32"
        with self.assertRaisesRegex(ValidationError, "platform mismatch"):
            self.agent.handle({"action": "doctor"})

    def test_doctor_proves_selected_repository_project_mapping(self) -> None:
        result = self.agent.handle(
            {
                "action": "doctor",
                "repository": "example-repo",
            }
        )
        self.assertTrue(result["project_registered"])
        self.fake_t3.projects = []
        with self.assertRaisesRegex(ValidationError, "project mapping"):
            self.agent.handle(
                {
                    "action": "doctor",
                    "repository": "example-repo",
                }
            )

    def test_doctor_marks_incompatible_t3_contract_ineligible(self) -> None:
        # Accepted staged cohort: exactly 0.0.30, 0.0.31, and 0.0.32, including
        # their '-' prerelease and '+' build suffix forms.
        for version in (
            "0.0.30",
            "0.0.30-nightly.20260728.928",
            "0.0.30+build.42",
            "0.0.31",
            "0.0.31-rc.1",
            "0.0.31+local.7",
            "0.0.31-nightly.20260729.948+local.1",
            "0.0.32",
            "0.0.32-nightly.20260729.951",
            "0.0.32-rc.1+build.3",
            "0.0.32+local.7",
        ):
            with self.subTest(version=version):
                self.fake_t3.server_version = version
                self.fake_t3.capabilities = {"threadSettlement": True}
                result = self.agent.handle({"action": "doctor"})
                self.assertEqual(result["state"], "eligible", version)
        # Rejected boundary versions: compatibility is a closed allowlist, so
        # arbitrary versions outside the staged cohort must be incompatible.
        for version in (
            "0.0.29",
            "0.0.33",
            "0.0.300",
            "0.0.310",
            "0.0.320",
            "0.0.30-",
            "0.0.31+",
            "0.0.32-",
            "0.0.32+",
            "0.0.30-@",
            "0.0.31+ ",
            "0.0.32-@",
        ):
            with self.subTest(version=version):
                self.fake_t3.server_version = version
                self.fake_t3.capabilities = {"threadSettlement": True}
                result = self.agent.handle({"action": "doctor"})
                self.assertEqual(result["state"], "incompatible", version)
        # Missing capability: descriptor.capabilities.threadSettlement must be
        # exactly true; absent, falsy, or non-bool truthy values are incompatible.
        self.fake_t3.server_version = "0.0.30"
        for capabilities, label in (
            ({}, "absent"),
            ({"threadSettlement": False}, "false"),
            ({"threadSettlement": "true"}, "string"),
            ({"threadSettlement": 1}, "integer"),
            (None, "non-dict"),
        ):
            with self.subTest(label=label):
                self.fake_t3.capabilities = capabilities
                result = self.agent.handle({"action": "doctor"})
                self.assertEqual(result["state"], "incompatible", label)

    def test_doctor_marks_saturated_target_ineligible(self) -> None:
        self.agent.policy = AgentPolicy(
            host_id=self.agent.policy.host_id,
            expected_hostname=self.agent.policy.expected_hostname,
            platform=self.agent.policy.platform,
            repositories=self.agent.policy.repositories,
            state_dir=self.agent.policy.state_dir,
            active_limit=1,
        )
        result = self.agent.handle({"action": "doctor"})
        self.assertEqual(result["state"], "at_capacity")

    def test_doctor_and_capacity_exclude_terminal_turn_with_stale_running_session(
        self,
    ) -> None:
        # Live canary defect: after thread.turn.interrupt and thread.session.stop,
        # T3 leaves latestTurn.state terminal (interrupted/completed/error) while
        # session.status stays running. doctor and capacity must not count such a
        # terminal turn as active, or the stale-running provider session falsely
        # consumes the host slot and blocks replacement canaries.
        for label, turn_state in (
            ("interrupted", "interrupted"),
            ("completed", "completed"),
            ("error", "error"),
        ):
            with self.subTest(label=label):
                self.fake_t3.thread["latestTurn"] = {"state": turn_state}
                self.fake_t3.thread["session"] = {"status": "running"}
                doctor = self.agent.handle({"action": "doctor"})
                self.assertEqual(doctor["active"], 0)
                self.assertEqual(doctor["state"], "eligible")
                capacity = self.agent.handle({"action": "capacity"})
                self.assertEqual(capacity["active"], 0)

    def test_doctor_and_capacity_still_count_starting_session_without_latest_turn(
        self,
    ) -> None:
        # A starting session with no latestTurn yet is still genuinely active.
        self.fake_t3.thread["latestTurn"] = None
        self.fake_t3.thread["session"] = {"status": "starting"}
        doctor = self.agent.handle({"action": "doctor"})
        self.assertEqual(doctor["active"], 1)
        capacity = self.agent.handle({"action": "capacity"})
        self.assertEqual(capacity["active"], 1)

    def test_doctor_and_capacity_still_count_running_latest_turn(self) -> None:
        # A running latestTurn with a running session is the canonical active case.
        self.fake_t3.thread["latestTurn"] = {"state": "running"}
        self.fake_t3.thread["session"] = {"status": "running"}
        doctor = self.agent.handle({"action": "doctor"})
        self.assertEqual(doctor["active"], 1)
        capacity = self.agent.handle({"action": "capacity"})
        self.assertEqual(capacity["active"], 1)

    def test_answer_requires_current_user_input_and_dispatches_answers(self) -> None:
        self.fake_t3.thread["activities"] = [
            {
                "kind": "user-input.requested",
                "payload": {"requestId": "input-current"},
            }
        ]
        result = self.agent.handle(
            {
                "action": "answer",
                "thread_id": "thread-1",
                "input_id": "input-current",
                "answers": {"question-1": ["Option A"]},
            }
        )
        command = self.fake_t3.dispatched[-1]
        self.assertEqual(command["type"], "thread.user-input.respond")
        self.assertEqual(command["requestId"], "input-current")
        self.assertEqual(command["answers"], {"question-1": ["Option A"]})
        self.assertEqual(result["state"], "running")

    def test_followup_rejects_invalid_modes_before_dispatch(self) -> None:
        with self.assertRaisesRegex(ValidationError, "runtime_mode"):
            self.agent.handle(
                {
                    "action": "followup",
                    "request_id": "req-12345678",
                    "thread_id": "thread-1",
                    "prompt": "continue",
                    "runtime_mode": "invalid-mode",
                }
            )
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_approve_requires_current_open_request_id(self) -> None:
        self.fake_t3.thread["activities"] = [
            {
                "kind": "approval.requested",
                "payload": {"requestId": "approval-current"},
            }
        ]
        with self.assertRaisesRegex(ValidationError, "not currently pending"):
            self.agent.handle(
                {
                    "action": "approve",
                    "thread_id": "thread-1",
                    "approval_id": "approval-stale",
                    "decision": "accept",
                }
            )

    def test_project_mapping_requires_one_exact_workspace_root(self) -> None:
        self.fake_t3.projects = []
        with self.assertRaisesRegex(ValidationError, "project mapping"):
            self.agent.handle(self.submit_request)

    def test_recover_finds_thread_from_target_receipt(self) -> None:
        self.agent.handle(self.submit_request)
        result = self.agent.handle(
            {
                "action": "recover",
                "request_id": "req-12345678",
            }
        )
        self.assertEqual(result["thread_id"], "thread-1")
        self.assertEqual(result["state"], "running")

    def test_interrupt_acceptance_remains_running_until_status_confirms(self) -> None:
        result = self.agent.handle(
            {
                "action": "interrupt",
                "thread_id": "thread-1",
            }
        )
        self.assertEqual(result["state"], "running")
        self.assertEqual(result["sequence"], 8)
        self.assertEqual(
            [command["type"] for command in self.fake_t3.dispatched],
            ["thread.turn.interrupt", "thread.session.stop"],
        )

    def test_interrupt_cancels_open_approvals_before_settling_turn(self) -> None:
        # Live canary defect (attempt two): thread.turn.interrupt plus
        # thread.session.stop do not settle a turn while open approval.requested
        # activities remain unresolved. The operator already authorized stopping
        # the active turn, so the fleet interrupt must cancel every open
        # approval with decision=cancel (never accept) before turn interrupt and
        # session stop. The mutation response remains running until a later
        # status confirms the terminal interrupted state and releases capacity.
        for label, open_ids, expected_cancels in (
            ("zero open approvals", (), []),
            ("one open approval", ("approval-current",), ["approval-current"]),
            (
                "multiple open approvals",
                ("approval-c", "approval-a", "approval-b"),
                ["approval-a", "approval-b", "approval-c"],
            ),
        ):
            with self.subTest(label=label):
                fake = StuckApprovalT3(open_approval_ids=open_ids)
                original = self.agent.t3
                self.agent.t3 = fake
                try:
                    result = self.agent.handle(
                        {"action": "interrupt", "thread_id": "thread-1"}
                    )
                finally:
                    self.agent.t3 = original
                # The mutation response stays running until status confirms.
                self.assertEqual(result["state"], "running")
                cancel_commands = [
                    command
                    for command in fake.dispatched
                    if command["type"] == "thread.approval.respond"
                ]
                # Every open approval is cancelled exactly once, in a stable
                # deterministic order. Stale resolved approvals are not touched.
                self.assertEqual(
                    [command["requestId"] for command in cancel_commands],
                    expected_cancels,
                )
                self.assertNotIn(
                    "approval-stale",
                    {command["requestId"] for command in cancel_commands},
                )
                for command in cancel_commands:
                    self.assertEqual(command["decision"], "cancel")
                # Sequence: cancellations, then turn interrupt, then session stop.
                non_cancel_types = [
                    command["type"]
                    for command in fake.dispatched
                    if command["type"] != "thread.approval.respond"
                ]
                self.assertEqual(
                    non_cancel_types,
                    ["thread.turn.interrupt", "thread.session.stop"],
                )
                # After cancellation the turn settles; a later status must
                # normalize to interrupted so the stored active request releases.
                self.agent.t3 = fake
                try:
                    status = self.agent.handle(
                        {"action": "status", "thread_id": "thread-1"}
                    )
                finally:
                    self.agent.t3 = original
                self.assertEqual(status["state"], "interrupted")
                self.assertFalse(status["needs_attention"])

    def test_interrupt_includes_active_turn_id_so_projection_settles(self) -> None:
        # Live canary defect (attempt three): T3 ProjectionPipeline's
        # "thread.turn-interrupt-requested" handler returns early when
        # event.payload.turnId is undefined, so thread.turn.interrupt without
        # a turnId leaves latestTurn projected as running. The fleet interrupt
        # must extract the active orchestration turn id from the thread snapshot
        # (latestTurn.turnId) and include it as turnId on thread.turn.interrupt
        # so the projection settles the turn and a later status releases.
        fake = TurnIdProjectionT3(turn_id="turn-active-1")
        original = self.agent.t3
        self.agent.t3 = fake
        try:
            result = self.agent.handle(
                {"action": "interrupt", "thread_id": "thread-1"}
            )
        finally:
            self.agent.t3 = original
        # The mutation response stays running until status confirms.
        self.assertEqual(result["state"], "running")
        interrupt_commands = [
            command
            for command in fake.dispatched
            if command["type"] == "thread.turn.interrupt"
        ]
        self.assertEqual(len(interrupt_commands), 1)
        self.assertEqual(interrupt_commands[0]["turnId"], "turn-active-1")
        # Carrying the active turn id settles the projection; a later status
        # must normalize to interrupted so the stored active request releases.
        self.agent.t3 = fake
        try:
            status = self.agent.handle(
                {"action": "status", "thread_id": "thread-1"}
            )
        finally:
            self.agent.t3 = original
        self.assertEqual(status["state"], "interrupted")
        self.assertFalse(status["needs_attention"])

    def test_interrupt_omits_turn_id_when_snapshot_lacks_latest_turn_turn_id(self) -> None:
        # Older snapshots (and the existing stuck-approval fixture) project
        # latestTurn without a turnId. The interrupt command must omit turnId
        # rather than synthesize one, preserving the prior behavior shape.
        fake = FakeT3()
        original = self.agent.t3
        self.agent.t3 = fake
        try:
            self.agent.handle({"action": "interrupt", "thread_id": "thread-1"})
        finally:
            self.agent.t3 = original
        interrupt_commands = [
            command
            for command in fake.dispatched
            if command["type"] == "thread.turn.interrupt"
        ]
        self.assertEqual(len(interrupt_commands), 1)
        self.assertNotIn("turnId", interrupt_commands[0])

    def test_claim_rejects_invalid_envelope_before_store_or_mutation(self) -> None:
        request = dict(
            self.submit_request,
            action="claim",
            request_id="linux-worker-controller-a-12345678-1234-1234-1234-123456789abc",
            digest="d" * 64,
        )
        del request["title"]
        with self.assertRaisesRegex(ValidationError, "title"):
            self.agent.handle(request)
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.git_calls, [])
        self.assertFalse((Path(self.directory.name) / "target.db").exists())

    def test_claim_reserves_before_t3_and_matching_retry_does_not_start_second_turn(self) -> None:
        request = dict(
            self.submit_request,
            action="claim",
            request_id="linux-worker-controller-a-12345678-1234-1234-1234-123456789abc",
            digest="d" * 64,
        )
        first = self.agent.handle(request)
        second = self.agent.handle(request)
        self.assertEqual(first["thread_id"], "thread-1")
        self.assertEqual(second["thread_id"], "thread-1")
        self.assertEqual(
            [item["type"] for item in self.fake_t3.dispatched],
            ["thread.create", "thread.meta.update", "thread.turn.start"],
        )

    def test_claim_uses_single_envelope_snapshot_and_preserves_uuid_order(self) -> None:
        request = dict(
            self.submit_request,
            action="claim",
            request_id="linux-worker-controller-a-12345678-1234-1234-1234-123456789abc",
            digest="d" * 64,
        )
        original_prepare = self.agent._prepare_submit_envelope
        prepare_calls: list[dict[str, Any]] = []

        def tracking_prepare(req: dict[str, Any]) -> Any:
            prepare_calls.append(req)
            return original_prepare(req)

        self.agent._prepare_submit_envelope = tracking_prepare  # type: ignore[method-assign]
        result = self.agent.handle(request)
        self.assertEqual(len(prepare_calls), 1)
        self.assertEqual(result["thread_id"], "thread-1")
        self.assertEqual(
            [item["type"] for item in self.fake_t3.dispatched],
            ["thread.create", "thread.meta.update", "thread.turn.start"],
        )
        create_command, update_command, turn_command = self.fake_t3.dispatched
        # UUIDs are allocated in dispatch order: turn command id, thread id, message id,
        # create command id, update command id. Commands are dispatched create/update/turn.
        self.assertEqual(create_command["threadId"], "thread-1")
        self.assertEqual(create_command["commandId"], "create-cmd-1")
        self.assertEqual(update_command["commandId"], "update-cmd-1")
        self.assertEqual(turn_command["commandId"], "turn-cmd-1")
        self.assertEqual(turn_command["message"]["messageId"], "message-1")

    def test_reconcile_matching_completed_detail_releases_capacity_without_interrupt(
        self,
    ) -> None:
        request_id = "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        self.fake_t3 = StaleShellProjectionT3(
            shell_latest_turn={"turnId": "turn-1", "state": "running"},
            detail_latest_turn={"turnId": "turn-1", "state": "completed"},
            session={"status": "ready"},
        )
        self.agent.t3 = self.fake_t3
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=request_id,
                digest="d" * 64,
            )
        )
        receipt = self.agent.handle({"action": "reconcile", "request_id": request_id})
        self.assertEqual(receipt["state"], "succeeded")
        self.assertEqual(
            self.agent.handle({"action": "capacity"})["active"],
            0,
        )
        self.assertEqual(
            [item["type"] for item in self.fake_t3.dispatched],
            ["thread.create", "thread.meta.update", "thread.turn.start"],
        )

    def test_reconcile_matching_completed_detail_is_idempotent(self) -> None:
        request_id = "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        self.fake_t3 = StaleShellProjectionT3(
            shell_latest_turn={"turnId": "turn-1", "state": "running"},
            detail_latest_turn={"turnId": "turn-1", "state": "completed"},
            session={"status": "ready"},
        )
        self.agent.t3 = self.fake_t3
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=request_id,
                digest="d" * 64,
            )
        )
        first = self.agent.handle({"action": "reconcile", "request_id": request_id})
        second = self.agent.handle({"action": "reconcile", "request_id": request_id})
        self.assertEqual(first["state"], "succeeded")
        self.assertEqual(second["state"], "succeeded")
        self.assertEqual(first["updated_at"], second["updated_at"])

    def test_reconcile_terminal_survives_stale_projection_mutations(self) -> None:
        request_id = "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        self.fake_t3 = StaleShellProjectionT3(
            shell_latest_turn={"turnId": "turn-1", "state": "running"},
            detail_latest_turn={"turnId": "turn-1", "state": "completed"},
            session={"status": "ready"},
        )
        self.agent.t3 = self.fake_t3
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=request_id,
                digest="d" * 64,
            )
        )
        first = self.agent.handle({"action": "reconcile", "request_id": request_id})
        self.assertEqual(first["state"], "succeeded")

        mutations: list[tuple[str, Any]] = [
            (
                "stale_running_shell",
                StaleShellProjectionT3(
                    shell_latest_turn={"turnId": "turn-1", "state": "running"},
                    detail_latest_turn={"turnId": "turn-1", "state": "running"},
                    session={"status": "ready"},
                ),
            ),
            (
                "mismatched_detail_turn",
                StaleShellProjectionT3(
                    shell_latest_turn={"turnId": "turn-1", "state": "running"},
                    detail_latest_turn={"turnId": "turn-2", "state": "completed"},
                    session={"status": "ready"},
                ),
            ),
            (
                "missing_detail_thread",
                MalformedDetailProjectionT3(
                    None,
                    shell_latest_turn={"turnId": "turn-1", "state": "running"},
                    detail_latest_turn={"turnId": "turn-1", "state": "completed"},
                    session={"status": "ready"},
                ),
            ),
            (
                "malformed_detail_thread",
                MalformedDetailProjectionT3(
                    {"thread": "bad"},
                    shell_latest_turn={"turnId": "turn-1", "state": "running"},
                    detail_latest_turn={"turnId": "turn-1", "state": "completed"},
                    session={"status": "ready"},
                ),
            ),
        ]
        for label, projection in mutations:
            with self.subTest(label=label):
                self.agent.t3 = projection
                receipt = self.agent.handle(
                    {"action": "reconcile", "request_id": request_id}
                )
                self.assertEqual(receipt["state"], "succeeded")
                self.assertEqual(receipt["updated_at"], first["updated_at"])
                self.assertEqual(
                    self.agent.handle({"action": "capacity"})["active"],
                    0,
                )

    def test_reconcile_terminal_state_releases_target_lease(self) -> None:
        request_id = "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=request_id,
                digest="d" * 64,
            )
        )
        self.fake_t3.thread["latestTurn"] = {"state": "completed"}
        self.fake_t3.thread["session"] = {"status": "ready"}
        receipt = self.agent.handle({"action": "reconcile", "request_id": request_id})
        self.assertEqual(receipt["state"], "succeeded")
        self.assertEqual(
            self.agent.handle({"action": "capacity"})["active"],
            0,
        )
        second = self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id="linux-worker-controller-a-87654321-1234-1234-1234-123456789abc",
                digest="e" * 64,
            )
        )
        self.assertEqual(second["thread_id"], "thread-2")

    def test_reconcile_after_interrupt_releases_capacity_despite_stale_pending(self) -> None:
        # Live canary defect: after interrupt is accepted, T3 leaves a stale
        # pending approval marker. _status must normalize to interrupted so the
        # stored active request releases and capacity drops to 0.
        request_id = "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=request_id,
                digest="d" * 64,
            )
        )
        self.agent.handle({"action": "interrupt", "thread_id": "thread-1"})
        self.fake_t3.pending_approval = True
        self.fake_t3.thread["latestTurn"] = {"state": "interrupted"}
        self.fake_t3.thread["session"] = {"status": "stopped"}
        receipt = self.agent.handle({"action": "reconcile", "request_id": request_id})
        self.assertEqual(receipt["state"], "interrupted")
        self.assertEqual(
            self.agent.handle({"action": "capacity"})["active"],
            0,
        )

    def test_lookup_returns_redacted_receipt_and_not_found_without_mutation(self) -> None:
        missing = self.agent.handle({"action": "lookup", "request_id": "req-87654321"})
        self.assertEqual(missing, {"schema": "t3-fleet.agent-result.v1", "state": "not_found"})


if __name__ == "__main__":
    unittest.main()
