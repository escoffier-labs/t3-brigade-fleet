from __future__ import annotations

import errno
import json
import os
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from t3_fleet.agent import AgentPolicy, RepositoryPolicy, T3Agent, _agent_error
from t3_fleet.errors import (
    ConfigError,
    HostUnavailable,
    PolicyRejected,
    PolicyRevisionConflict,
    TargetBusy,
    UnknownState,
    ValidationError,
)
from t3_fleet.target_store import TargetClaim

FULL_SHA = "0123456789abcdef0123456789abcdef01234567"


# The Fleet Hub's canonical model identity, and the native model string the
# provider is actually launched on. They differ on purpose: the roster's
# explicit binding is the only thing allowed to make them differ, and nothing in
# the client may guess one from the other.
CANONICAL_MODEL = "gpt-5.6-terra"
NATIVE_MODEL = "gpt-5.6-terra-2026-08-30"


class FakeAdmission:
    """The Brigade `fleet models` adapter, in both of its distinct roles.

    `plan` is read-only planning and says `launch_authorized: false`. `admit` is
    the act that spends fleet capacity. The fake keeps them in separate call
    logs so a test can prove which one a code path actually took.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.plan_calls: list[dict[str, object]] = []
        self.admit_error: Exception | None = None
        self.plan_error: Exception | None = None
        self.admitted_model = CANONICAL_MODEL
        self.admitted_seat = "coder"
        # The explicit native model the Hub binding named, when it named one.
        self.admitted_launch_model: str | None = None

    def admit(self, request: dict[str, object]) -> dict[str, object]:
        self.calls.append(dict(request))
        if self.admit_error is not None:
            raise self.admit_error
        return {
            **(
                {"launch_model": self.admitted_launch_model}
                if self.admitted_launch_model is not None
                else {}
            ),
            "roster_revision": 42,
            "roster_digest": "sha256:" + "a" * 64,
            "seat": self.admitted_seat,
            "provider": "openai",
            "model": self.admitted_model,
            "reasoning": "high",
            "provider_instance_id": "codex",
            "service_tier": "priority",
            "source": "hub",
            "expires_at": "2026-08-30T14:00:00Z",
        }

    def plan(self, request: dict[str, object]) -> dict[str, object]:
        self.plan_calls.append(dict(request))
        if self.plan_error is not None:
            raise self.plan_error
        return dict(PLAN)


class ChangingExpiryAdmission(FakeAdmission):
    def admit(self, request: dict[str, object]) -> dict[str, object]:
        result = super().admit(request)
        result["expires_at"] = "2026-08-30T14:01:00Z"
        return result


ADMISSION = FakeAdmission().admit({})

# The normalized plan record, as the transport adapter hands it back. The native
# launch model is stated explicitly, so no test can pass by guessing.
PLAN: dict[str, object] = {
    "state": "planned",
    "roster_revision": 42,
    "roster_digest": "sha256:" + "a" * 64,
    "seat": "coder",
    "provider": "openai",
    "model": CANONICAL_MODEL,
    "reasoning": "high",
    "provider_instance_id": "codex",
    "service_tier": "priority",
    "launch_model": NATIVE_MODEL,
    "launch_model_explicit": True,
    "source": "hub",
    "expires_at": "2026-07-28T01:10:00Z",
}


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
            "modelSelection": {
                "instanceId": "codex",
                "model": "gpt-5.6-terra",
                "options": {
                    "reasoningEffort": "high",
                    "serviceTier": "priority",
                },
            },
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
        # A created thread really does carry the selection it was created with,
        # so the snapshot a later continuation reads reports the model that was
        # applied rather than the fixture's default.
        if command["type"] == "thread.create" and "modelSelection" in command:
            self.thread["modelSelection"] = command["modelSelection"]
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
            "latestTurn": {"state": "running", "turnId": "turn-1"},
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
            if not self.open_approvals and command.get("turnId") == "turn-1":
                self.thread["latestTurn"] = {"state": "interrupted", "turnId": "turn-1"}
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
        from unittest.mock import patch
        reporting = patch("t3_fleet.agent.report_event", return_value="unavailable")
        reporting.start()
        self.addCleanup(reporting.stop)
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
                "archive-cmd-1",
                "turn-cmd-3",
                "thread-3",
                "message-3",
                "create-cmd-3",
                "update-cmd-3",
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
                    path=CHECKOUT_PATH,
                    default_branch="main",
                    branch_prefix="codex/",
                    policy_identity=CANONICAL_REPO,
                )
            },
            state_dir=Path(self.directory.name),
            active_limit=2,
        )
        self.admission = FakeAdmission()
        self.agent = T3Agent(
            policy,
            self.fake_t3,
            hostname=lambda: "linux-worker",
            uuid_factory=uuid_factory,
            now=lambda: "2026-07-28T01:00:00Z",
            git_runner=fake_git_runner,
            admission=self.admission,
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
            "model_admission": ADMISSION,
        }

    def test_wrong_selected_host_is_rejected_before_any_mutation(self) -> None:
        for action in ("submit", "claim", "reserve_followup", "followup", "interrupt", "approve", "answer", "recover", "release", "lookup"):
            with self.subTest(action=action):
                with self.assertRaisesRegex(ValidationError, "host"):
                    self.agent.handle({**self.submit_request, "action": action, "host": "other-worker"})
                self.assertEqual(self.git_calls, [])
                self.assertEqual(self.fake_t3.dispatched, [])
                self.assertEqual(list(Path(self.directory.name).iterdir()), [])
                self.assertIsNone(self.agent._store)

    def test_valid_claim_for_wrong_host_does_not_reserve_or_launch(self) -> None:
        request = {**self.submit_request, "action": "claim", "host": "other-worker",
                   "request_id": "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc", "digest": "d" * 64}
        with self.assertRaisesRegex(ValidationError, "host"):
            self.agent.handle(request)
        self.assertEqual(self.git_calls, [])
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertIsNone(self.agent._store)
        self.assertEqual(list(Path(self.directory.name).iterdir()), [])

    def test_valid_followup_for_wrong_host_does_not_send_turn(self) -> None:
        self.agent.handle(self.submit_request)
        before_commands = list(self.fake_t3.dispatched)
        before_receipt = self.agent._target_store().lookup(self.submit_request["request_id"])
        with self.assertRaisesRegex(ValidationError, "host"):
            self.agent.handle({**self.submit_request, "action": "followup", "thread_id": "thread-1", "host": "other-worker"})
        self.assertEqual(self.fake_t3.dispatched, before_commands)
        self.assertEqual(self.agent._target_store().lookup(self.submit_request["request_id"]), before_receipt)

    def test_success_boundary_attests_not_found_and_action_request(self) -> None:
        result = self.agent.handle({"action": "lookup", "request_id": "req-12345678"})
        self.assertEqual(result["state"], "not_found")
        self.assertEqual(result.get("host"), "linux-worker")
        self.assertEqual(result.get("request_id"), "req-12345678")
        self.agent.handle(self.submit_request)
        result = self.agent.handle({"action": "status", "thread_id": "thread-1", "request_id": "req-12345678"})
        self.assertEqual(result.get("request_id"), "req-12345678")
        self.assertEqual(result.get("host"), "linux-worker")

    def _seed_admitted_request(self) -> None:
        store = self.agent._target_store()
        store.claim(
            TargetClaim(
                request_id="req-12345678",
                digest="a" * 64,
                target_id="linux-worker",
                repository="example-repo",
                source_revision=FULL_SHA,
                prompt_sha256="b" * 64,
                prompt_bytes=1,
                admission=ADMISSION,
            ),
            active_limit=2,
            reservation_ttl=60,
        )
        store.attach_thread("req-12345678", "thread-1")

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
        self.assertEqual(len(self.git_calls), 3)
        self.assertEqual(
            self.git_calls[0][3:],
            ["cat-file", "-e", f"{FULL_SHA}^{{commit}}"],
        )
        git_argv = self.git_calls[1]
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
            self.git_calls[2],
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

    def test_submit_accepts_newer_target_admission_expiry_with_same_identity(self) -> None:
        self.agent._admission = ChangingExpiryAdmission()

        result = self.agent.handle(self.submit_request)

        self.assertEqual(result["state"], "running")
        self.assertEqual(
            self.fake_t3.dispatched[-1]["modelSelection"]["model"],
            "gpt-5.6-terra",
        )

    def test_submit_launches_the_native_binding_model_of_an_admission(self) -> None:
        self.admission.admitted_launch_model = NATIVE_MODEL
        request = dict(
            self.submit_request,
            model_admission={**ADMISSION, "launch_model": NATIVE_MODEL},
        )

        result = self.agent.handle(request)

        self.assertEqual(result["state"], "running")
        create = self.fake_t3.dispatched[0]
        turn = self.fake_t3.dispatched[-1]
        self.assertEqual(create["type"], "thread.create")
        self.assertEqual(create["modelSelection"]["model"], NATIVE_MODEL)
        self.assertEqual(turn["type"], "thread.turn.start")
        self.assertEqual(turn["modelSelection"]["model"], NATIVE_MODEL)
        # The canonical identity is what the ledger and receipt keep.
        stored = self.agent._target_store().lookup("req-12345678")
        self.assertEqual(stored.admission["model"], CANONICAL_MODEL)
        self.assertEqual(stored.admission["launch_model"], NATIVE_MODEL)

    def test_submit_without_a_binding_model_launches_the_canonical_model(self) -> None:
        self.agent.handle(self.submit_request)

        for command in self.fake_t3.dispatched:
            if "modelSelection" in command:
                self.assertEqual(command["modelSelection"]["model"], CANONICAL_MODEL)
        stored = self.agent._target_store().lookup("req-12345678")
        self.assertNotIn("launch_model", stored.admission)

    def test_submit_refuses_a_target_admission_for_another_launch_model(self) -> None:
        self.admission.admitted_launch_model = "gpt-5.6-terra-2026-01-01"
        request = dict(
            self.submit_request,
            model_admission={**ADMISSION, "launch_model": NATIVE_MODEL},
        )

        with self.assertRaises(ValidationError):
            self.agent.handle(request)

        self.assertEqual(self.fake_t3.dispatched, [])

    def test_retired_matcher_allows_version_with_additional_digit(self) -> None:
        self.assertFalse(
            self.agent._is_retired_model_selection(
                {"instanceId": "codex", "model": "gpt-5.40"}
            )
        )

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

    def test_submit_uses_admission_when_project_default_is_retired(self) -> None:
        self.fake_t3.projects[0]["defaultModelSelection"] = {
            "instanceId": "codex",
            "model": "gpt-5.5",
        }

        result = self.agent.handle(self.submit_request)

        self.assertEqual(result["state"], "running")
        selection = self.fake_t3.dispatched[0]["modelSelection"]
        self.assertEqual(selection["instanceId"], ADMISSION["provider_instance_id"])
        self.assertEqual(selection["model"], ADMISSION["model"])
        self.assertEqual(
            selection["options"]["reasoningEffort"], ADMISSION["reasoning"]
        )
        self.assertEqual(selection["options"]["serviceTier"], ADMISSION["service_tier"])

    def test_submit_rejects_retired_explicit_model_before_mutation(self) -> None:
        request = dict(self.submit_request)
        request["model_selection"] = {
            "instanceId": "codex",
            "model": "gpt-5.5-codex",
        }

        with self.assertRaisesRegex(ValidationError, "retired T3 model"):
            self.agent.handle(request)

        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.git_calls, [])

    def test_continuation_actions_reject_retired_thread_before_dispatch(self) -> None:
        requests = (
            {
                "action": "followup",
                "request_id": "req-12345678",
                "thread_id": "thread-1",
                "prompt": "continue",
            },
            {
                "action": "approve",
                "request_id": "req-12345678",
                "thread_id": "thread-1",
                "approval_id": "approval-1",
                "decision": "accept",
            },
            {
                "action": "answer",
                "request_id": "req-12345678",
                "thread_id": "thread-1",
                "input_id": "input-1",
                "answers": {"question": "answer"},
            },
        )
        for model in ("gpt-5.4", "openai/gpt-5.5-codex"):
            for request in requests:
                with self.subTest(model=model, action=request["action"]):
                    self.fake_t3.thread["modelSelection"] = {
                        "instanceId": "codex",
                        "model": model,
                    }

                    with self.assertRaisesRegex(ValidationError, "retired T3 model"):
                        self.agent.handle(request)

                    self.assertEqual(self.fake_t3.dispatched, [])

    def test_followup_pins_validated_thread_model_selection(self) -> None:
        self._seed_admitted_request()

        self.agent.handle(
            {
                "action": "followup",
                "request_id": "req-12345678",
                "thread_id": "thread-1",
                "prompt": "continue",
                "model_admission": ADMISSION,
            }
        )

        self.assertEqual(
            self.fake_t3.dispatched[0]["modelSelection"],
            {
                "instanceId": "codex",
                "model": "gpt-5.6-terra",
                "options": {
                    "reasoningEffort": "high",
                    "serviceTier": "priority",
                },
            },
        )
        self.assertEqual(
            self.admission.calls[-1],
            {
                "consumer": "t3-fleet",
                "phase": "target",
                "request_id": "req-12345678",
                "seat": "coder",
                "expect_revision": 42,
                "expect_digest": "sha256:" + "a" * 64,
            },
        )

    def test_followup_rejects_safe_but_unadmitted_thread_selection(self) -> None:
        self._seed_admitted_request()
        self.fake_t3.thread["modelSelection"] = {
            "instanceId": "codex",
            "model": "gpt-5.6-luna",
            "options": {"reasoningEffort": "medium", "serviceTier": "priority"},
        }

        with self.assertRaisesRegex(PolicyRejected, "drifted"):
            self.agent.handle(
                {
                    "action": "followup",
                    "request_id": "req-12345678",
                    "thread_id": "thread-1",
                    "prompt": "continue",
                    "model_admission": ADMISSION,
                }
            )

        self.assertEqual(self.fake_t3.dispatched, [])

    def test_reserve_followup_rejects_retired_thread_before_reservation(self) -> None:
        self.fake_t3.thread["modelSelection"] = {
            "instanceId": "codex",
            "model": "gpt-5.4",
        }

        with self.assertRaisesRegex(ValidationError, "retired T3 model"):
            self.agent.handle(
                {
                    "action": "reserve_followup",
                    "request_id": "req-12345678",
                    "thread_id": "thread-1",
                }
            )

        self.assertIsNone(self.agent._target_store().lookup("req-12345678"))

    def test_doctor_reports_retired_project_default_as_observed_drift(self) -> None:
        self.fake_t3.projects[0]["defaultModelSelection"] = {
            "instanceId": "codex",
            "model": "openai:gpt-5.5_codex",
        }

        result = self.agent.handle(
            {
                "action": "doctor",
                "repository": "example-repo",
            }
        )

        self.assertEqual(result["state"], "eligible")
        self.assertTrue(result["project_registered"])
        self.assertEqual(result["observed_project_default"]["model"], "openai:gpt-5.5_codex")
        self.assertTrue(result["project_default_drift"])
        self.assertIsNone(result["approved_model_admission"])
        self.assertIsNone(result["admission_source"])
        self.assertIsNone(result["admission_cache_age_seconds"])

    def test_doctor_reports_cached_admission_without_authorizing_from_default(self) -> None:
        self.fake_t3.projects[0]["defaultModelSelection"] = {
            "instanceId": "stale-instance",
            "model": "gpt-5.5",
        }
        self.agent._target_store().claim(
            TargetClaim(
                request_id=(
                    "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
                ),
                digest="a" * 64,
                target_id="linux-worker",
                repository="example-repo",
                source_revision=FULL_SHA,
                prompt_sha256="b" * 64,
                prompt_bytes=1,
                admission=ADMISSION,
            ),
            active_limit=2,
            reservation_ttl=60,
        )

        result = self.agent.handle({"action": "doctor", "repository": "example-repo"})

        self.assertEqual(result["state"], "eligible")
        self.assertEqual(result["approved_model_admission"], ADMISSION)
        self.assertEqual(result["admission_source"], "hub")
        self.assertIsInstance(result["admission_cache_age_seconds"], float)
        self.assertTrue(result["project_default_drift"])

    def test_submit_fetches_unfetched_source_revision_before_worktree_add(self) -> None:
        calls: list[list[str]] = []
        cat_file_checks = 0

        fetched = False

        def fetch_then_find_revision(argv: list[str], timeout: float) -> Any:
            nonlocal cat_file_checks, fetched
            calls.append(list(argv))
            if argv[3] == "cat-file":
                cat_file_checks += 1
                return subprocess.CompletedProcess(
                    argv,
                    0 if fetched else 128,
                    stdout="",
                    stderr="" if fetched else "missing revision",
                )
            if argv[3:] == ["fetch", "origin"]:
                fetched = True
            if argv[3:] == ["fetch", "origin", FULL_SHA]:
                return subprocess.CompletedProcess(
                    argv,
                    128,
                    stdout="",
                    stderr="server rejected targeted fetch",
                )
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        self.agent._git_runner = fetch_then_find_revision

        result = self.agent.handle(self.submit_request)

        self.assertEqual(result["state"], "running")
        # The second check runs under the fetch lock, in case another request
        # fetched the revision while this one waited.
        self.assertEqual(
            [call[3] for call in calls],
            ["cat-file", "cat-file", "fetch", "fetch", "cat-file", "worktree", "config"],
        )
        self.assertEqual(calls[2][4:], ["origin", FULL_SHA])
        self.assertEqual(calls[3][4:], ["origin"])
        self.assertEqual(cat_file_checks, 3)

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
        self.assertEqual(len(self.git_calls), 5)
        add_argv = self.git_calls[1]
        self.assertEqual(add_argv[3], "worktree")
        self.assertEqual(add_argv[4], "add")
        self.assertEqual(add_argv[6], "codex/req-12345678")
        self.assertEqual(add_argv[8], FULL_SHA)
        remove_argv = self.git_calls[3]
        self.assertEqual(remove_argv[3], "worktree")
        self.assertEqual(remove_argv[4], "remove")
        self.assertEqual(remove_argv[5], "--force")
        self.assertEqual(remove_argv[6], add_argv[7])
        # The never-used branch goes with the worktree.
        self.assertEqual(self.git_calls[4][3:], ["branch", "-D", "codex/req-12345678"])
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["outcome"], "failed")
        self.assertEqual(receipt["error"], "target dispatch failed")
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

    def test_submit_creates_no_thread_when_git_worktree_add_fails(self) -> None:
        self.git_fail_after = 1
        with self.assertRaisesRegex(UnknownState, "worktree boom"):
            self.agent.handle(self.submit_request)
        # The worktree is prepared before T3 hears about the request, so a git
        # failure leaves no thread to archive.
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(len(self.git_calls), 2)
        self.assertEqual(self.git_calls[1][4], "add")
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["outcome"], "failed")
        self.assertEqual(receipt["error"], "target worktree preparation failed")

    def test_submit_cleans_up_worktree_when_merge_base_configuration_fails(
        self,
    ) -> None:
        self.git_fail_after = 2

        with self.assertRaisesRegex(UnknownState, "configuration failed"):
            self.agent.handle(self.submit_request)

        self.assertEqual(
            [call[3] for call in self.git_calls],
            ["cat-file", "worktree", "config", "worktree", "branch"],
        )
        self.assertEqual(self.git_calls[-2][4:6], ["remove", "--force"])
        self.assertEqual(self.git_calls[-1][4:], ["-D", "codex/req-12345678"])
        self.assertEqual(self.fake_t3.dispatched, [])

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
                {"state": "interrupted", "turnId": "turn-1"},
                {"status": "stopped"},
            ),
            (
                "session stopped",
                {"state": "running"},
                {"status": "stopped"},
            ),
            (
                "both interrupted and stopped",
                {"state": "interrupted", "turnId": "turn-1"},
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
                self.assertEqual(result["state"], "unknown" if label == "session stopped" else "interrupted")
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
        self.fake_t3.thread["latestTurn"] = {"state": "completed", "turnId": "turn-1"}
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

    def test_recover_marks_thread_without_turn_or_worktree_failed(self) -> None:
        self.agent.handle(self.submit_request)
        self.fake_t3.thread["latestTurn"] = None
        self.fake_t3.thread["session"] = None

        result = self.agent.handle(
            {
                "action": "recover",
                "request_id": "req-12345678",
            }
        )

        self.assertEqual(result["state"], "failed")

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
        # Accepted staged cohort: exactly 0.0.30 through 0.0.40, including
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
            "0.0.33",
            "0.0.33-nightly.20260808.1035",
            "0.0.33-rc.1+build.3",
            "0.0.34",
            "0.0.34-nightly.20260811.1067",
            "0.0.34+local.7",
            "0.0.35",
            "0.0.35-nightly.20260826.1195",
            "0.0.35+local.1",
            "0.0.36",
            "0.0.36-nightly.20260827.1205",
            "0.0.36+build.5",
            "0.0.37",
            "0.0.37-nightly.20260829.1207",
            "0.0.37+build.5",
            "0.0.37-rc.1+build.3",
            "0.0.38",
            "0.0.38-nightly.20260831.1236",
            "0.0.38+build.5",
            "0.0.38-rc.1+build.3",
            "0.0.39",
            "0.0.39-nightly.20260907.1325",
            "0.0.39+build.5",
            "0.0.39-rc.1+build.3",
            "0.0.40",
            "0.0.40-nightly.20260907.1346",
            "0.0.40+build.5",
            "0.0.40-rc.1+build.3",
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
            "0.0.29-nightly.1",
            "0.0.41",
            "0.0.41-nightly.1",
            "0.0.41+build.1",
            "0.0.41-rc.1+build.3",
            "0.0.400",
            "0.0.400-nightly.1",
            "0.0.40-",
            "0.0.40+",
            "0.0.300",
            "0.0.310",
            "0.0.320",
            "0.0.340",
            "0.0.350",
            "0.0.360",
            "0.0.370",
            "0.0.370-nightly.1",
            "0.0.380",
            "0.0.380-nightly.1",
            "0.0.30-",
            "0.0.31+",
            "0.0.32-",
            "0.0.32+",
            "0.0.34-",
            "0.0.34+",
            "0.0.35-",
            "0.0.35+",
            "0.0.36-",
            "0.0.36+",
            "0.0.37-",
            "0.0.37+",
            "0.0.38-",
            "0.0.38+",
            "0.0.30-@",
            "0.0.31+ ",
            "0.0.32-@",
            "0.0.34-@",
            "0.0.35-@",
            "0.0.36-@",
            "0.0.36+ ",
            "0.0.37-@",
            "0.0.37+ ",
            "0.0.38-@",
            "0.0.38+ ",
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

    def test_doctor_and_capacity_retain_terminal_turn_with_live_session(
        self,
    ) -> None:
        from dataclasses import replace

        # A terminal latest turn cannot prove a live session has stopped.
        self.agent.policy = replace(self.agent.policy, active_limit=1)
        for label, turn_state in (
            ("interrupted", "interrupted"),
            ("completed", "completed"),
            ("error", "error"),
        ):
            with self.subTest(label=label):
                self.fake_t3.thread["latestTurn"] = {"state": turn_state}
                self.fake_t3.thread["session"] = {"status": "running"}
                doctor = self.agent.handle({"action": "doctor"})
                self.assertEqual(doctor["active"], 1)
                self.assertEqual(doctor["state"], "at_capacity")
                capacity = self.agent.handle({"action": "capacity"})
                self.assertEqual(capacity["active"], 1)

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
        self._seed_admitted_request()
        self.fake_t3.thread["activities"] = [
            {
                "kind": "user-input.requested",
                "payload": {"requestId": "input-current"},
            }
        ]
        result = self.agent.handle(
            {
                "action": "answer",
                "request_id": "req-12345678",
                "thread_id": "thread-1",
                "input_id": "input-current",
                "answers": {"question-1": ["Option A"]},
                "model_admission": ADMISSION,
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
        self._seed_admitted_request()
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
                    "request_id": "req-12345678",
                    "thread_id": "thread-1",
                    "approval_id": "approval-stale",
                    "decision": "accept",
                    "model_admission": ADMISSION,
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

    def test_claim_pre_turn_failure_releases_repository_lease_for_next_claim(self) -> None:
        first_request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        first = dict(
            self.submit_request,
            action="claim",
            request_id=first_request_id,
            digest="d" * 64,
        )
        self.fake_t3.fail_after = 1

        with self.assertRaisesRegex(RuntimeError, "dispatch failed mid-sequence"):
            self.agent.handle(first)

        failed = self.agent.handle({"action": "lookup", "request_id": first_request_id})
        self.assertEqual(failed["state"], "failed")
        receipt = json.loads(
            (
                Path(self.directory.name)
                / f"{first_request_id}-submit.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["outcome"], "failed")
        self.assertEqual(receipt["error"], "target dispatch failed")

        self.fake_t3.fail_after = None
        second = self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=(
                    "linux-worker-controller-b-22345678-1234-1234-1234-123456789abc"
                ),
                digest="e" * 64,
            )
        )

        self.assertEqual(second["state"], "running")
        self.assertEqual(
            [item["type"] for item in self.fake_t3.dispatched],
            ["thread.create", "thread.create", "thread.meta.update", "thread.turn.start"],
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

    def _seed_unknown_without_thread(self) -> str:
        request_id = "req-12345678"
        store = self.agent._target_store()
        store.claim(
            TargetClaim(
                request_id=request_id,
                digest="a" * 64,
                target_id="linux-worker",
                repository="example-repo",
                source_revision=FULL_SHA,
                prompt_sha256="b" * 64,
                prompt_bytes=1,
            ),
            active_limit=1,
            reservation_ttl=300,
        )
        store.reconcile(request_id, state="unknown", thread_id=None)
        return request_id

    def test_reconcile_old_unknown_without_thread_releases_capacity_and_lease(self) -> None:
        request_id = self._seed_unknown_without_thread()
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        result = self.agent.handle({"action": "reconcile", "request_id": request_id})
        self.assertEqual(result["state"], "failed")
        store = self.agent._target_store()
        self.assertEqual(store.lookup(request_id).state, "failed")
        self.assertEqual(store.active_count(), 0)
        # The reservation and repository lease must both permit the next claim.
        next_receipt = store.claim(
            TargetClaim(
                request_id="req-22345678",
                digest="c" * 64,
                target_id="linux-worker",
                repository="example-repo",
                source_revision=FULL_SHA,
                prompt_sha256="b" * 64,
                prompt_bytes=1,
            ),
            active_limit=1,
            reservation_ttl=300,
        )
        self.assertEqual(next_receipt.state, "preflight")

    def test_failed_threadless_claim_replay_cannot_launch_or_reacquire_lease(self) -> None:
        from unittest.mock import patch

        request = dict(
            self.submit_request,
            action="claim",
            request_id="linux-worker-controller-a-12345678-1234-1234-1234-123456789abc",
            digest="d" * 64,
        )
        # A process death after the durable claim leaves no allocated thread.
        with patch.object(self.agent, "_submit", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.agent.handle(request)
        self.agent._now = lambda: "2026-07-29T01:00:00Z"
        settled = self.agent.handle(dict(request, action="reconcile"))
        self.assertEqual(settled["state"], "failed")
        replay = self.agent.handle(request)
        self.assertEqual(replay["state"], "failed")
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.git_calls, [])
        self.assertEqual(self.agent._target_store().active_count(), 0)
        next_request = dict(
            request,
            request_id="linux-worker-controller-a-22345678-1234-1234-1234-123456789abc",
        )
        self.assertEqual(self.agent.handle(next_request)["state"], "running")

    def test_failed_threadless_legacy_submit_replay_cannot_launch(self) -> None:
        from unittest.mock import patch

        store = self.agent._target_store()
        with patch.object(store, "begin_turn", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.agent.handle(self.submit_request)
        self.agent._now = lambda: "2026-07-29T01:00:00Z"
        settled = self.agent.handle({"action": "reconcile", "request_id": "req-12345678"})
        self.assertEqual(settled["state"], "failed")
        replay = self.agent.handle(self.submit_request)
        self.assertEqual(replay["state"], "failed")
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.git_calls, [])
        self.assertEqual(store.active_count(), 0)

    def test_reconcile_young_unknown_without_thread_preserves_capacity(self) -> None:
        request_id = self._seed_unknown_without_thread()
        self.agent._now = lambda: "2026-07-28T01:00:29Z"
        result = self.agent.handle({"action": "reconcile", "request_id": request_id})
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_reconcile_locked_old_unknown_without_thread_preserves_capacity(self) -> None:
        request_id = self._seed_unknown_without_thread()
        self.agent._now = lambda: "2026-07-29T01:00:00Z"
        with self.agent._request_lock(request_id):
            result = self.agent.handle({"action": "reconcile", "request_id": request_id})
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_reconcile_no_turn_and_no_worktree_fails_and_releases_lease(self) -> None:
        request_id = "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=request_id,
                digest="d" * 64,
            )
        )
        self.fake_t3.thread["latestTurn"] = None
        self.fake_t3.thread["session"] = {"status": "ready"}

        receipt = self.agent.handle({"action": "reconcile", "request_id": request_id})

        self.assertEqual(receipt["state"], "failed")
        self.assertEqual(self.agent._target_store().active_count(), 0)
        next_receipt = self.agent._target_store().claim(
            TargetClaim(
                request_id=(
                    "linux-worker-controller-b-22345678-1234-1234-1234-123456789abc"
                ),
                digest="e" * 64,
                target_id="linux-worker",
                repository="example-repo",
                source_revision=FULL_SHA,
                prompt_sha256="f" * 64,
                prompt_bytes=12,
            ),
            active_limit=2,
            reservation_ttl=30,
        )
        self.assertEqual(next_receipt.state, "preflight")

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
        self.fake_t3.thread["latestTurn"] = {"state": "completed", "turnId": "turn-1"}
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
        self.fake_t3.thread["latestTurn"] = {"state": "interrupted", "turnId": "turn-1"}
        self.fake_t3.thread["session"] = {"status": "stopped"}
        receipt = self.agent.handle({"action": "reconcile", "request_id": request_id})
        self.assertEqual(receipt["state"], "interrupted")
        self.assertEqual(
            self.agent.handle({"action": "capacity"})["active"],
            0,
        )

    def test_lookup_returns_redacted_receipt_and_not_found_without_mutation(self) -> None:
        missing = self.agent.handle({"action": "lookup", "request_id": "req-87654321"})
        self.assertEqual(missing, {"schema": "t3-fleet.agent-result.v1", "state": "not_found", "host": "linux-worker", "request_id": "req-87654321"})



POLICY_DIGEST = "sha256:" + "b" * 64
PEER_REQUEST_ID = "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"


CANONICAL_REPO = "github.com/example-org/example-repo"
CHECKOUT_PATH = "/srv/repos/example"
CONTEXT_HASH = "sha256:" + "c" * 64


def policy_stamp_payload(**overrides: Any) -> dict[str, Any]:
    stamp = {
        "schema": "t3-fleet.fleet-policy.v1",
        "session_id": "t3f-" + "0" * 32,
        "origin": "controller-host",
        "workload": "linux-development",
        "repo_identity": CANONICAL_REPO,
        "version": 7,
        "digest": POLICY_DIGEST,
        "decision_id": "decision-1",
        "reservation_id": "reservation-1",
        "machine": "linux-worker",
        "seat": "coder",
        "expires_at": "2026-07-28T01:10:00Z",
    }
    stamp.update(overrides)
    return stamp


class FakeTargetPolicy:
    def __init__(self, t3: Any) -> None:
        self._t3 = t3
        self.machine = "linux-worker"
        self.seat = "coder"
        self.version = 7
        self.digest = POLICY_DIGEST
        self.resolved_version: int | None = None
        self.resolved_digest: str | None = None
        self.decision_id = "decision-1"
        self.reservation_id = "reservation-1"
        self.expires_at = "2026-07-28T01:10:00Z"
        self.renew_state = "active"
        self.renew_expires_at = "2026-07-28T02:10:00Z"
        self.resolve_error: Exception | None = None
        self.route_error: Exception | None = None
        self.prepare_error: Exception | None = None
        self.ack_error: Exception | None = None
        self.resolve_calls: list[dict[str, Any]] = []
        self.route_calls: list[dict[str, Any]] = []
        self.prepare_calls: list[dict[str, Any]] = []
        self.ack_calls: list[dict[str, Any]] = []
        # What the Hub says it prepared. A test moves one of these to prove the
        # target refuses a prepared session that is not the one it planned.
        self.prepared_repo_identity: str | None = None
        self.prepared_selection: dict[str, Any] | None = None
        # The deterministic digest of the whole prepared context. A test moves
        # or clears it to prove the acknowledgement forwards this exact value
        # and that an enrolled preparation without one fails closed.
        self.context_hash: str | None = CONTEXT_HASH
        self.delegation_error: Exception | None = None
        self.delegation_receipt: dict[str, Any] | None = None
        self.delegation_calls: list[dict[str, Any]] = []
        self.instructions = (
            "Do not widen the seat, the machine, or the model for this turn."
        )
        self.ack_dispatch_depth: list[int] = []
        self.renew_calls: list[dict[str, Any]] = []
        self.release_calls: list[dict[str, Any]] = []
        self.renew_error: Exception | None = None
        self.release_error: Exception | None = None
        # Set by the reservation suite so a lifecycle call can record the
        # durable local state the target held at the moment it was made.
        self.durable_state: Callable[[], str | None] | None = None
        self.renew_states: list[str | None] = []
        self.release_states: list[str | None] = []

    def resolve(self, **kwargs: Any) -> dict[str, Any]:
        self.resolve_calls.append(dict(kwargs))
        if self.resolve_error is not None:
            raise self.resolve_error
        return self._body()

    def _body(self) -> dict[str, Any]:
        return {
            "version": (
                self.version if self.resolved_version is None else self.resolved_version
            ),
            "digest": self.resolved_digest or self.digest,
            "roles": {"impl": "muse13", "review": "claude-standby"},
            "execution": {
                "machine": self.machine,
                "network": "tailnet-only",
            },
            "data": {
                "allow_training": False,
                "privacy": "fleet-confidential",
            },
            "sources": {
                "data.privacy": {
                    "layer": "fleet-hub",
                    "value": "fleet-confidential",
                    "chain": [
                        {
                            "layer": "org-baseline",
                            "action": "set",
                            "value": "fleet-confidential",
                        }
                    ],
                }
            },
            "ack_required": True,
        }

    def route(self, **kwargs: Any) -> dict[str, Any]:
        self.route_calls.append(dict(kwargs))
        if self.route_error is not None:
            raise self.route_error
        return {
            "decision_id": self.decision_id,
            "policy_version": self.version,
            "policy_digest": self.digest,
            "machine": self.machine,
            "seat": self.seat,
            "reason": "workload match",
            "candidates": [],
            "reservation_id": self.reservation_id,
            "expires_at": self.expires_at,
        }

    def prepare(self, **kwargs: Any) -> dict[str, Any]:
        self.prepare_calls.append(dict(kwargs))
        if self.prepare_error is not None:
            raise self.prepare_error
        return {
            **self._body(),
            "repo_identity": (
                self.prepared_repo_identity
                if self.prepared_repo_identity is not None
                else CANONICAL_REPO
            ),
            "selected": self.prepared_selection
            or {
                "seat": self.seat,
                "provider": "openai",
                # Canonical roster identity, plus the exact native provider id
                # the turn is launched on. They are separate facts.
                "model": CANONICAL_MODEL,
                "launch_model": NATIVE_MODEL,
                "reasoning": "high",
                "instance_id": "codex",
            },
            "instructions": self.instructions,
            **(
                {"context_hash": self.context_hash}
                if self.context_hash is not None
                else {}
            ),
        }

    def acknowledge(self, **kwargs: Any) -> dict[str, Any]:
        self.ack_calls.append(dict(kwargs))
        self.ack_dispatch_depth.append(len(self._t3.dispatched))
        if self.ack_error is not None:
            raise self.ack_error
        return {"acknowledged": True}

    def delegation(self, **kwargs: Any) -> dict[str, Any]:
        self.delegation_calls.append(dict(kwargs))
        if self.delegation_error is not None:
            raise self.delegation_error
        if self.delegation_receipt is None:
            raise AssertionError("this target holds no delegation receipt")
        return dict(self.delegation_receipt)

    def _observe(self) -> str | None:
        return self.durable_state() if self.durable_state is not None else None

    def renew_reservation(self, **kwargs: Any) -> dict[str, Any]:
        self.renew_calls.append(dict(kwargs))
        self.renew_states.append(self._observe())
        if self.renew_error is not None:
            raise self.renew_error
        return {
            "schema": "brigade.fleet_reservation.v1",
            "reservation_id": kwargs["reservation_id"],
            "decision_id": kwargs["decision_id"],
            "state": self.renew_state,
            "expires_at": self.renew_expires_at,
        }

    def release_reservation(self, **kwargs: Any) -> dict[str, Any]:
        self.release_calls.append(dict(kwargs))
        self.release_states.append(self._observe())
        if self.release_error is not None:
            raise self.release_error
        return {
            "schema": "brigade.fleet_reservation.v1",
            "reservation_id": kwargs["reservation_id"],
            "decision_id": kwargs["decision_id"],
            "state": "released",
            "expires_at": "2026-07-28T01:10:00Z",
        }


class _AgentPolicyFixture:
    """Shared enrolled-target fixture for the policy and reservation suites."""

    def setUp(self) -> None:
        from unittest.mock import patch
        reporting = patch("t3_fleet.agent.report_event", return_value="unavailable")
        reporting.start()
        self.addCleanup(reporting.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.fake_t3 = FakeT3()
        self.git_calls: list[list[str]] = []
        self.admission = FakeAdmission()
        self.policy = FakeTargetPolicy(self.fake_t3)
        self.agent = self.build()
        self.submit_request = {
            "action": "submit",
            "request_id": "req-12345678",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "prompt": "private prompt text",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
            "model_plan": dict(PLAN),
            "fleet_policy": policy_stamp_payload(),
        }

    def build(self, *, enrollment: str = "off") -> T3Agent:
        uuid_pool = iter(
            [
                "turn-cmd-1",
                "thread-1",
                "message-1",
                "create-cmd-1",
                "update-cmd-1",
                "archive-cmd-1",
                "turn-cmd-2",
                "thread-2",
                "message-2",
                "create-cmd-2",
                "update-cmd-2",
                "archive-cmd-2",
            ]
        )

        def fake_git_runner(argv: list[str], timeout: float) -> Any:
            self.git_calls.append(list(argv))
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        policy = AgentPolicy(
            host_id="linux-worker",
            expected_hostname="linux-worker",
            platform="linux",
            repositories={
                "example-repo": RepositoryPolicy(
                    path=CHECKOUT_PATH,
                    default_branch="main",
                    branch_prefix="codex/",
                    policy_identity=CANONICAL_REPO,
                )
            },
            state_dir=Path(self.directory.name),
            active_limit=2,
            policy_enrollment=enrollment,
        )
        return T3Agent(
            policy,
            self.fake_t3,
            hostname=lambda: "linux-worker",
            uuid_factory=lambda: next(uuid_pool),
            now=lambda: "2026-07-28T01:00:00Z",
            git_runner=fake_git_runner,
            admission=self.admission,
            fleet_policy=self.policy,
        )

    def record_order(self) -> list[str]:
        """One ledger of every central gate and every T3 command, in real order.

        Ordering is the whole contract, so it is asserted as a single sequence
        rather than as a handful of independent call counts.
        """
        order: list[str] = []

        def wrap(target: Any, name: str) -> None:
            original = getattr(target, name)

            def recorded(**kwargs: Any) -> Any:
                order.append(name)
                return original(**kwargs)

            setattr(target, name, recorded)

        for name in (
            "resolve",
            "route",
            "prepare",
            "acknowledge",
            "renew_reservation",
            "release_reservation",
        ):
            wrap(self.policy, name)

        admit = self.admission.admit

        def recorded_admit(request: dict[str, Any]) -> Any:
            order.append(f"admit:{request['phase']}")
            return admit(request)

        self.admission.admit = recorded_admit

        dispatch = self.fake_t3.dispatch

        def recorded_dispatch(command: dict[str, Any]) -> Any:
            order.append(command["type"])
            return dispatch(command)

        self.fake_t3.dispatch = recorded_dispatch
        return order

    def unstamped_request(self, **overrides: Any) -> dict[str, Any]:
        """The pre-policy contract: no stamp, no plan, a controller admission."""
        request = dict(self.submit_request, **overrides)
        request.pop("fleet_policy", None)
        request.pop("model_plan", None)
        request["model_admission"] = ADMISSION
        return request

    def dispatched_types(self) -> list[str]:
        return [item["type"] for item in self.fake_t3.dispatched]

    def turn_text(self) -> str:
        turns = [
            item for item in self.fake_t3.dispatched if item["type"] == "thread.turn.start"
        ]
        self.assertEqual(len(turns), 1)
        return turns[0]["message"]["text"]


class AgentFleetPolicyTests(_AgentPolicyFixture, unittest.TestCase):
    def test_target_validates_the_decision_against_its_own_identity(self) -> None:
        self.agent.handle(self.submit_request)
        self.assertEqual(len(self.policy.route_calls), 1)
        call = self.policy.route_calls[0]
        self.assertEqual(call["consumer"], "t3-fleet")
        self.assertEqual(call["repository"], CANONICAL_REPO)
        self.assertEqual(call["session_id"], "t3f-" + "0" * 32)
        self.assertEqual(call["origin"], "controller-host")
        self.assertEqual(call["workload"], "linux-development")
        self.assertEqual(call["machine"], "linux-worker")
        self.assertEqual(call["decision_id"], "decision-1")
        self.assertNotIn("prompt", call)

    def test_acknowledge_lands_after_thread_creation_and_before_turn_start(self) -> None:
        self.agent.handle(self.submit_request)
        self.assertEqual(
            self.dispatched_types(),
            ["thread.create", "thread.meta.update", "thread.turn.start"],
        )
        self.assertEqual(len(self.policy.ack_calls), 1)
        self.assertEqual(self.policy.ack_dispatch_depth, [2])
        ack = self.policy.ack_calls[0]
        self.assertEqual(ack["session_id"], "t3f-" + "0" * 32)
        self.assertEqual(ack["consumer"], "t3-fleet")
        self.assertEqual(ack["repository"], CANONICAL_REPO)
        self.assertEqual(ack["version"], 7)
        self.assertEqual(ack["digest"], POLICY_DIGEST)

    def test_acknowledge_failure_stops_the_turn_and_rolls_the_thread_back(self) -> None:
        self.policy.ack_error = PolicyRejected("session acknowledgement was refused")
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.submit_request)
        self.assertEqual(
            self.dispatched_types(),
            ["thread.create", "thread.meta.update", "thread.archive"],
        )
        self.assertNotIn("thread.turn.start", self.dispatched_types())
        self.assertTrue(
            any("worktree" in argv and "remove" in argv for argv in self.git_calls)
        )
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text()
        )
        self.assertEqual(receipt["outcome"], "failed")

    def test_decision_for_another_machine_is_rejected_before_any_side_effect(self) -> None:
        self.policy.machine = "second-worker"
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.git_calls, [])
        self.assertEqual(self.policy.ack_calls, [])

    def test_policy_version_drift_is_a_revision_conflict(self) -> None:
        self.policy.version = 8
        with self.assertRaises(PolicyRevisionConflict):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.fake_t3.dispatched, [])
        self.policy.version = 7
        self.policy.digest = "sha256:" + "c" * 64
        with self.assertRaises(PolicyRevisionConflict):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_returned_decision_id_must_match_the_stamped_decision(self) -> None:
        self.policy.decision_id = "decision-2"
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_policy_seat_must_match_the_planned_seat(self) -> None:
        request = dict(
            self.submit_request, fleet_policy=policy_stamp_payload(seat="muse13")
        )
        self.policy.seat = "muse13"
        with self.assertRaises(PolicyRejected):
            self.agent.handle(request)
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.policy.ack_calls, [])

    def test_expired_stamp_is_rejected_before_the_helper_runs(self) -> None:
        request = dict(
            self.submit_request,
            fleet_policy=policy_stamp_payload(expires_at="2026-07-28T00:59:00Z"),
        )
        with self.assertRaises(PolicyRejected):
            self.agent.handle(request)
        self.assertEqual(self.policy.route_calls, [])
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_malformed_stamp_is_rejected(self) -> None:
        for mutation in (
            {"version": 0},
            {"digest": "b" * 64},
            {"session_id": ""},
            {"prompt": "leaked"},
        ):
            with self.subTest(mutation=sorted(mutation)):
                agent = self.build()
                request = dict(
                    self.submit_request,
                    fleet_policy=policy_stamp_payload(**mutation),
                )
                with self.assertRaises(ValidationError):
                    agent.handle(request)
                self.assertEqual(self.fake_t3.dispatched, [])

    def test_enrolled_target_requires_a_routing_decision(self) -> None:
        agent = self.build(enrollment="required")
        request = dict(self.submit_request)
        request.pop("fleet_policy")
        with self.assertRaises(PolicyRejected):
            agent.handle(request)
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.policy.route_calls, [])

    def test_unenrolled_target_without_a_stamp_never_calls_the_helper(self) -> None:
        request = self.unstamped_request()
        result = self.agent.handle(request)
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.policy.route_calls, [])
        self.assertEqual(self.policy.ack_calls, [])
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text()
        )
        self.assertNotIn("fleet_policy", receipt)

    def test_receipt_records_safe_policy_metadata_only(self) -> None:
        self.agent.handle(self.submit_request)
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text()
        )
        policy = receipt["fleet_policy"]
        self.assertEqual(
            set(policy),
            {
                "session_id",
                "workload",
                "repo_identity",
                "version",
                "digest",
                "decision_id",
                "reservation_id",
                "machine",
                "seat",
                "acknowledged",
                # The digest of the exact prepared context that ran. It is a
                # bounded identifier, like everything else here.
                "context_hash",
            },
        )
        self.assertEqual(policy["context_hash"], CONTEXT_HASH)
        self.assertTrue(policy["acknowledged"])
        # The canonical identity is a safe identifier, and it is what lets a
        # resumed request be revalidated against the repository it was admitted
        # for. The local path and the prompt still never appear.
        self.assertEqual(policy["repo_identity"], CANONICAL_REPO)
        self.assertNotIn(CHECKOUT_PATH, json.dumps(receipt))
        self.assertNotIn("private prompt text", json.dumps(receipt))

    def test_claim_rejection_leaves_no_reservation_or_thread(self) -> None:
        self.policy.route_error = PolicyRejected("Fleet Hub denied the decision")
        request = dict(
            self.submit_request,
            action="claim",
            request_id=PEER_REQUEST_ID,
            digest="d" * 64,
        )
        with self.assertRaises(PolicyRejected):
            self.agent.handle(request)
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.git_calls, [])
        self.assertFalse((Path(self.directory.name) / "target.db").exists())

    def test_target_prepares_canonical_policy_from_its_own_node_identity(self) -> None:
        # A launch prepares, it does not plainly resolve: the Hub is told the
        # exact native selection this target is about to run, so the reply can
        # be checked against it instead of being trusted.
        self.agent.handle(self.submit_request)
        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(len(self.policy.prepare_calls), 1)
        call = self.policy.prepare_calls[0]
        self.assertEqual(call["consumer"], "t3-fleet")
        self.assertEqual(call["repository"], CHECKOUT_PATH)
        self.assertEqual(call["session_id"], "t3f-" + "0" * 32)
        self.assertEqual(call["origin"], "controller-host")
        self.assertEqual(call["decision_id"], "decision-1")
        self.assertEqual(call["provider"], "openai")
        self.assertEqual(call["instance_id"], "codex")
        self.assertEqual(call["reasoning"], "high")
        # The native model, not the canonical identity.
        self.assertEqual(call["model"], NATIVE_MODEL)
        self.assertNotIn("prompt", call)

    def test_turn_prefix_carries_the_canonical_effective_rules_and_provenance(self) -> None:
        self.agent.handle(self.submit_request)
        text = self.turn_text()
        self.assertTrue(text.endswith("private prompt text"))
        prefix = text[: -len("private prompt text")]
        self.assertIn("fleet-confidential", prefix)
        self.assertIn("tailnet-only", prefix)
        self.assertIn("org-baseline", prefix)
        self.assertIn("claude-standby", prefix)
        self.assertIn(POLICY_DIGEST, prefix)
        # The acknowledged model is the native one this target actually applied,
        # not the canonical roster identity it was planned under.
        self.assertIn(NATIVE_MODEL, prefix)
        self.assertIn("codex", prefix)
        # The instructions the Hub prepared for this exact selection reach the
        # policy-bearing turn.
        self.assertIn(self.policy.instructions, prefix)
        self.assertNotIn("private prompt text", prefix)

    def test_caller_cannot_forge_the_effective_rules(self) -> None:
        forged = policy_stamp_payload()
        forged["effective"] = {"data": {"privacy": "public", "allow_training": True}}
        with self.assertRaises(ValidationError):
            self.agent.handle(dict(self.submit_request, fleet_policy=forged))
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.policy.resolve_calls, [])

    def test_only_the_resolved_rules_reach_the_turn(self) -> None:
        request = dict(
            self.submit_request,
            fleet_policy_effective={"data": {"privacy": "public"}},
        )
        self.agent.handle(request)
        text = self.turn_text()
        self.assertNotIn('"public"', text)
        self.assertIn("fleet-confidential", text)

    def test_prepare_failure_prevents_the_thread_and_the_turn(self) -> None:
        self.policy.prepare_error = HostUnavailable("fleet policy adapter is missing")
        with self.assertRaises(HostUnavailable):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.git_calls, [])
        self.assertEqual(self.policy.ack_calls, [])

    def test_canonical_policy_that_drifted_from_the_stamp_is_a_conflict(self) -> None:
        self.policy.resolved_version = 8
        with self.assertRaises(PolicyRevisionConflict):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.fake_t3.dispatched, [])
        self.policy.resolved_version = None
        self.policy.resolved_digest = "sha256:" + "c" * 64
        with self.assertRaises(PolicyRevisionConflict):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_malformed_canonical_policy_never_reaches_the_prompt(self) -> None:
        class BrokenPolicy(FakeTargetPolicy):
            def _body(self) -> dict[str, Any]:
                envelope = super()._body()
                envelope["data"] = {"allow_training": "yes"}
                return envelope

        self.policy = BrokenPolicy(self.fake_t3)
        agent = self.build()
        with self.assertRaises(ValidationError):
            agent.handle(self.submit_request)
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_claim_replay_on_a_running_thread_does_not_acknowledge_again(self) -> None:
        request = dict(
            self.submit_request,
            action="claim",
            request_id=PEER_REQUEST_ID,
            digest="d" * 64,
        )
        first = self.agent.handle(request)
        second = self.agent.handle(request)
        self.assertEqual(first["thread_id"], "thread-1")
        self.assertEqual(second["thread_id"], "thread-1")
        self.assertEqual(len(self.policy.ack_calls), 1)
        self.assertEqual(
            self.dispatched_types(),
            ["thread.create", "thread.meta.update", "thread.turn.start"],
        )
class TargetReservationLifecycleTests(_AgentPolicyFixture, unittest.TestCase):
    """Only the authenticated target may renew or release a Hub reservation.

    The target holds the durable outcome for the work it is running, so it is
    the one node that can honestly say a reservation is still needed or has
    been handed back. The controller relays state; it never speaks for the
    target's node identity.
    """

    def setUp(self) -> None:
        super().setUp()
        self.peer_request = dict(
            self.submit_request,
            action="claim",
            request_id=PEER_REQUEST_ID,
            digest="d" * 64,
        )
        self.policy.durable_state = self.durable_state

    def durable_state(self) -> str | None:
        """The durable local state at the moment a lifecycle call was made."""
        store = self.agent._target_store()
        for request_id in (PEER_REQUEST_ID, "req-12345678"):
            receipt = store.lookup(request_id)
            if receipt is not None:
                return receipt.state
        return None

    def claim(self) -> None:
        self.agent.handle(self.peer_request)

    def reconcile(self) -> dict[str, Any]:
        return self.agent.handle(
            {"action": "reconcile", "request_id": PEER_REQUEST_ID}
        )

    def test_a_live_run_renews_from_the_target(self) -> None:
        self.claim()
        result = self.reconcile()
        self.assertEqual(result["state"], "running")
        self.assertEqual(len(self.policy.renew_calls), 1)
        renew = self.policy.renew_calls[0]
        self.assertEqual(renew["reservation_id"], "reservation-1")
        self.assertEqual(renew["decision_id"], "decision-1")
        self.assertEqual(renew["session_id"], "t3f-" + "0" * 32)
        self.assertEqual(self.policy.release_calls, [])
        self.assertEqual(self.policy.renew_states, ["running"])
        self.assertEqual(result["fleet_reservation"]["state"], "active")

    def test_release_happens_only_after_a_durable_terminal_outcome(self) -> None:
        self.claim()
        self.fake_t3.thread["latestTurn"] = {"state": "completed", "turnId": "turn-1"}
        self.fake_t3.thread["session"] = {"status": "idle"}
        result = self.reconcile()
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(len(self.policy.release_calls), 1)
        self.assertEqual(self.policy.release_states, ["succeeded"])
        self.assertEqual(self.policy.renew_calls, [])
        self.assertEqual(result["fleet_reservation"]["state"], "released")

    def test_a_repeated_terminal_reconcile_releases_exactly_once(self) -> None:
        self.claim()
        self.fake_t3.thread["latestTurn"] = {"state": "completed", "turnId": "turn-1"}
        self.fake_t3.thread["session"] = {"status": "idle"}
        self.reconcile()
        second = self.reconcile()
        self.assertEqual(len(self.policy.release_calls), 1)
        self.assertEqual(second["state"], "succeeded")
        self.assertEqual(second["fleet_reservation"]["state"], "released")

    def test_an_unknown_state_never_releases_the_reservation(self) -> None:
        self.claim()
        self.fake_t3.thread["latestTurn"] = {"state": "error"}
        self.fake_t3.thread["session"] = {"status": "running"}
        self.fake_t3.pending_input = False
        result = self.reconcile()
        if result["state"] == "unknown":
            self.assertEqual(self.policy.release_calls, [])
            self.assertEqual(self.policy.renew_calls, [])
            self.assertEqual(result["fleet_reservation"]["state"], "active")

    def test_a_failed_release_keeps_the_capacity_reserved(self) -> None:
        self.claim()
        self.policy.release_error = HostUnavailable("fleet policy adapter is missing")
        self.fake_t3.thread["latestTurn"] = {"state": "completed", "turnId": "turn-1"}
        self.fake_t3.thread["session"] = {"status": "idle"}
        result = self.reconcile()
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(len(self.policy.release_calls), 1)
        self.assertEqual(result["fleet_reservation"]["state"], "active")
        store = self.agent._target_store()
        self.assertEqual(store.lookup(PEER_REQUEST_ID).reservation_state, "active")

    def test_the_legacy_single_controller_path_settles_from_its_receipt(self) -> None:
        self.agent.handle(self.submit_request)
        result = self.agent.handle(
            {
                "action": "status",
                "request_id": "req-12345678",
                "thread_id": "thread-1",
            }
        )
        self.assertEqual(result["state"], "running")
        self.assertEqual(len(self.policy.renew_calls), 1)
        self.assertEqual(self.policy.renew_calls[0]["reservation_id"], "reservation-1")
        self.assertEqual(result["fleet_reservation"]["state"], "active")

    def test_the_legacy_recovery_path_settles_from_its_receipt(self) -> None:
        self.agent.handle(self.submit_request)
        self.fake_t3.thread["latestTurn"] = {"state": "completed", "turnId": "turn-1"}
        self.fake_t3.thread["session"] = {"status": "idle"}
        result = self.agent.handle(
            {
                "action": "recover",
                "request_id": "req-12345678",
                "thread_id": "thread-1",
            }
        )
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(len(self.policy.release_calls), 1)
        self.assertEqual(self.policy.release_states, ["succeeded"])
        self.assertEqual(result["fleet_reservation"]["state"], "released")

    def test_an_unstamped_request_never_touches_a_reservation(self) -> None:
        request = dict(self.peer_request)
        request.pop("fleet_policy")
        request.pop("model_plan")
        request["model_admission"] = ADMISSION
        self.agent.handle(request)
        result = self.reconcile()
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.policy.renew_calls, [])
        self.assertEqual(self.policy.release_calls, [])
        self.assertNotIn("fleet_reservation", result)


class TargetContinuationAdmissionTests(_AgentPolicyFixture, unittest.TestCase):
    """Resuming a request is admitted again, not inherited from the first turn.

    A follow-up, an approval, or a user-input answer starts real provider work,
    so each one re-proves the same three facts the original dispatch proved:
    the canonical policy has not moved, the decision is still the current one
    for this target, and the reservation that paid for the run is still live.
    A reservation that was released, expired, or never recorded fails closed;
    nothing here revives capacity or invents a fresh reservation protocol.
    """

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=PEER_REQUEST_ID,
                digest="d" * 64,
            )
        )
        self.fake_t3.thread["activities"] = [
            {"kind": "approval.requested", "payload": {"requestId": "approval-1"}},
            {"kind": "user-input.requested", "payload": {"requestId": "input-1"}},
        ]
        self.dispatched_before = len(self.fake_t3.dispatched)
        self.policy.resolve_calls.clear()
        self.policy.route_calls.clear()
        self.policy.prepare_calls.clear()
        self.admission.calls.clear()

    def store(self) -> Any:
        return self.agent._target_store()

    def receipt(self) -> Any:
        return self.store().lookup(PEER_REQUEST_ID)

    def followup(self, agent: T3Agent | None = None) -> dict[str, Any]:
        return (agent or self.agent).handle(
            {
                "action": "followup",
                "request_id": PEER_REQUEST_ID,
                "thread_id": "thread-1",
                "prompt": "continue",
                "model_plan": dict(PLAN),
            }
        )

    def resuming_requests(self) -> tuple[dict[str, Any], ...]:
        return (
            {
                "action": "followup",
                "request_id": PEER_REQUEST_ID,
                "thread_id": "thread-1",
                "prompt": "continue",
                "model_plan": dict(PLAN),
            },
            {
                "action": "approve",
                "request_id": PEER_REQUEST_ID,
                "thread_id": "thread-1",
                "approval_id": "approval-1",
                "decision": "accept",
                "model_plan": dict(PLAN),
            },
            {
                "action": "answer",
                "request_id": PEER_REQUEST_ID,
                "thread_id": "thread-1",
                "input_id": "input-1",
                "answers": {"question-1": ["Option A"]},
                "model_plan": dict(PLAN),
            },
            {
                "action": "reserve_followup",
                "request_id": PEER_REQUEST_ID,
                "thread_id": "thread-1",
                "model_plan": dict(PLAN),
            },
        )

    def assert_no_side_effect(self) -> None:
        self.assertEqual(len(self.fake_t3.dispatched), self.dispatched_before)
        self.assertEqual(self.policy.release_calls, [])
        self.assertEqual(self.receipt().reservation_state, "active")

    def test_a_live_reservation_admits_the_resumed_turn(self) -> None:
        result = self.followup()

        self.assertEqual(result["state"], "running")
        self.assertEqual(self.fake_t3.dispatched[-1]["type"], "thread.turn.start")
        # A continuation resolves plainly: it is not preparing a new selection,
        # it is proving the one it already launched with is still current.
        self.assertEqual(len(self.policy.resolve_calls), 1)
        self.assertEqual(self.policy.prepare_calls, [])
        self.assertEqual(len(self.policy.route_calls), 1)
        route = self.policy.route_calls[0]
        self.assertEqual(route["decision_id"], "decision-1")
        self.assertEqual(route["machine"], "linux-worker")
        self.assertEqual(route["origin"], "linux-worker")
        self.assertEqual(route["session_id"], "t3f-" + "0" * 32)
        self.assertEqual(route["workload"], "linux-development")
        self.assertIsNone(route.get("seat"))
        self.assertNotIn("prompt", route)
        self.assertEqual(len(self.policy.renew_calls), 1)
        renew = self.policy.renew_calls[0]
        self.assertEqual(renew["reservation_id"], "reservation-1")
        self.assertEqual(renew["decision_id"], "decision-1")
        self.assertEqual(renew["session_id"], "t3f-" + "0" * 32)
        self.assertEqual(self.policy.release_calls, [])

    def test_a_released_reservation_refuses_every_resuming_action(self) -> None:
        self.store().set_reservation_state(PEER_REQUEST_ID, "released")

        for request in self.resuming_requests():
            with self.subTest(action=request["action"]):
                with self.assertRaisesRegex(
                    PolicyRejected, "continuation-admission-required"
                ):
                    self.agent.handle(request)
                self.assertEqual(
                    len(self.fake_t3.dispatched), self.dispatched_before
                )
        self.assertEqual(self.policy.renew_calls, [])
        self.assertEqual(self.policy.release_calls, [])
        self.assertEqual(self.receipt().reservation_state, "released")

    def test_an_expired_reservation_window_refuses_to_resume(self) -> None:
        self.policy.renew_expires_at = "2026-07-28T00:59:00Z"

        with self.assertRaisesRegex(
            PolicyRejected, "continuation-admission-required"
        ):
            self.followup()

        self.assertEqual(len(self.policy.renew_calls), 1)
        self.assert_no_side_effect()

    def test_a_reservation_the_hub_refuses_to_renew_is_gone(self) -> None:
        self.policy.renew_state = "released"

        with self.assertRaisesRegex(
            PolicyRejected, "continuation-admission-required"
        ):
            self.followup()

        self.assert_no_side_effect()

    def test_a_denied_renewal_never_revives_the_old_capacity(self) -> None:
        self.policy.renew_error = PolicyRejected("fleet reservation was denied")

        with self.assertRaisesRegex(
            PolicyRejected, "continuation-admission-required"
        ):
            self.followup()

        self.assert_no_side_effect()

    def test_an_expired_decision_refuses_to_resume(self) -> None:
        self.policy.expires_at = "2026-07-28T00:59:00Z"

        with self.assertRaisesRegex(
            PolicyRejected, "continuation-admission-required"
        ):
            self.followup()

        self.assertEqual(self.policy.renew_calls, [])
        self.assert_no_side_effect()

    def test_a_missing_policy_receipt_refuses_to_resume(self) -> None:
        # One repository lease per target: hand it back before seeding the
        # second, unstamped request this test resumes.
        self.store().release(PEER_REQUEST_ID)
        self.store().claim(
            TargetClaim(
                request_id="req-12345678",
                digest="a" * 64,
                target_id="linux-worker",
                repository="example-repo",
                source_revision=FULL_SHA,
                prompt_sha256="b" * 64,
                prompt_bytes=1,
                plan=dict(PLAN),
            ),
            active_limit=4,
            reservation_ttl=60,
        )
        self.store().attach_thread("req-12345678", "thread-1")

        with self.assertRaisesRegex(
            PolicyRejected, "continuation-admission-required"
        ):
            self.agent.handle(
                {
                    "action": "followup",
                    "request_id": "req-12345678",
                    "thread_id": "thread-1",
                    "prompt": "continue",
                    "model_plan": dict(PLAN),
                }
            )

        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(self.policy.renew_calls, [])
        self.assertEqual(len(self.fake_t3.dispatched), self.dispatched_before)

    def test_an_unreachable_helper_holds_the_reservation_and_refuses(self) -> None:
        self.policy.renew_error = HostUnavailable("fleet policy adapter is missing")

        with self.assertRaises(HostUnavailable):
            self.followup()

        self.assertEqual(len(self.policy.renew_calls), 1)
        self.assert_no_side_effect()

    def test_an_unreachable_resolve_holds_the_reservation_and_refuses(self) -> None:
        self.policy.resolve_error = HostUnavailable("fleet policy adapter is missing")

        with self.assertRaises(HostUnavailable):
            self.followup()

        # The route is validated first now, so it has run; what must not have
        # happened is the renewal that would admit the resumed turn.
        self.assertEqual(len(self.policy.route_calls), 1)
        self.assertEqual(self.policy.renew_calls, [])
        self.assert_no_side_effect()

    def test_canonical_policy_drift_blocks_the_resumed_turn(self) -> None:
        self.policy.resolved_version = 8

        with self.assertRaises(PolicyRevisionConflict):
            self.followup()

        self.assertEqual(len(self.policy.route_calls), 1)
        self.assertEqual(self.policy.renew_calls, [])
        self.assert_no_side_effect()

    def test_a_route_that_moved_to_another_decision_blocks_the_turn(self) -> None:
        self.policy.decision_id = "decision-2"

        with self.assertRaisesRegex(PolicyRejected, "another routing decision"):
            self.followup()

        self.assertEqual(self.policy.renew_calls, [])
        self.assert_no_side_effect()

    def test_a_route_that_moved_to_another_reservation_blocks_the_turn(self) -> None:
        self.policy.reservation_id = "reservation-2"

        with self.assertRaisesRegex(
            PolicyRejected, "continuation-admission-required"
        ):
            self.followup()

        self.assertEqual(self.policy.renew_calls, [])
        self.assert_no_side_effect()

    def test_a_route_that_moved_to_another_seat_blocks_the_turn(self) -> None:
        self.policy.seat = "muse13"

        with self.assertRaisesRegex(PolicyRejected, "another seat"):
            self.followup()

        self.assertEqual(self.policy.renew_calls, [])
        self.assert_no_side_effect()

    def test_a_receipt_owned_by_another_target_never_resumes(self) -> None:
        intruder = T3Agent(
            AgentPolicy(
                host_id="second-worker",
                expected_hostname="second-worker",
                platform="linux",
                repositories=self.agent.policy.repositories,
                state_dir=Path(self.directory.name),
                active_limit=2,
                policy_enrollment="required",
            ),
            self.fake_t3,
            hostname=lambda: "second-worker",
            uuid_factory=lambda: "intruder-uuid",
            now=lambda: "2026-07-28T01:00:00Z",
            admission=self.admission,
            fleet_policy=self.policy,
        )

        with self.assertRaisesRegex(PolicyRejected, "another machine"):
            self.followup(intruder)

        self.assertEqual(self.policy.resolve_calls, [])
        self.assert_no_side_effect()

    def test_interrupt_and_reconcile_keep_their_existing_behavior(self) -> None:
        self.store().set_reservation_state(PEER_REQUEST_ID, "released")

        interrupted = self.agent.handle(
            {"action": "interrupt", "thread_id": "thread-1"}
        )
        self.assertIn("state", interrupted)
        reconciled = self.agent.handle(
            {"action": "reconcile", "request_id": PEER_REQUEST_ID}
        )
        self.assertEqual(reconciled["state"], "running")
        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(self.policy.route_calls, [])
        self.assertEqual(self.policy.renew_calls, [])

    def test_an_unenrolled_legacy_request_resumes_unchanged(self) -> None:
        self.store().release(PEER_REQUEST_ID)
        legacy = self.build()
        request = self.unstamped_request(
            action="claim",
            request_id="linux-worker-controller-b-12345678-1234-1234-1234-123456789abc",
            digest="e" * 64,
        )
        legacy.handle(request)
        before = len(self.fake_t3.dispatched)
        self.policy.resolve_calls.clear()
        self.policy.route_calls.clear()

        result = legacy.handle(
            {
                "action": "followup",
                "request_id": request["request_id"],
                "thread_id": "thread-1",
                "prompt": "continue",
                "model_admission": ADMISSION,
            }
        )

        self.assertEqual(result["state"], "running")
        self.assertEqual(len(self.fake_t3.dispatched), before + 1)
        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(self.policy.route_calls, [])
        self.assertEqual(self.policy.renew_calls, [])

    def test_a_resumed_turn_resolves_under_the_recorded_decision(self) -> None:
        """A continuation never re-resolves under an unrestricted origin.

        Resuming asks the same fenced question the first dispatch asked: this
        decision, this repository, this checkout. An origin-only re-resolve
        would let a resumed request drift onto whatever the Hub would choose
        now, which is a new admission wearing the old request's identity.
        """
        self.followup()
        call = self.policy.resolve_calls[0]
        self.assertEqual(call["decision_id"], "decision-1")
        self.assertEqual(call["repository"], CHECKOUT_PATH)
        self.assertEqual(self.policy.route_calls[0]["repository"], CANONICAL_REPO)
        self.assertEqual(self.policy.route_calls[0]["decision_id"], "decision-1")

    def test_a_resumed_turn_validates_its_route_before_resolving(self) -> None:
        order: list[str] = []
        resolve, route = self.policy.resolve, self.policy.route
        self.policy.resolve = lambda **kw: (order.append("resolve"), resolve(**kw))[1]
        self.policy.route = lambda **kw: (order.append("route"), route(**kw))[1]
        self.followup()
        self.assertEqual(order, ["route", "resolve"])

    def rewrite_stored_policy(self, policy: dict[str, Any]) -> None:
        """Rewrite the durable policy receipt the way a stale record would look."""
        connection = self.store()._connect()
        with connection:
            connection.execute(
                "UPDATE target_requests SET policy_json = ? WHERE request_id = ?",
                (json.dumps(policy, sort_keys=True), PEER_REQUEST_ID),
            )
        connection.close()

    def test_a_receipt_for_another_canonical_repository_refuses_to_resume(self) -> None:
        stored = dict(self.receipt().policy)
        stored["repo_identity"] = "github.com/example-org/other-repo"
        self.rewrite_stored_policy(stored)
        with self.assertRaises(PolicyRejected) as caught:
            self.followup()
        self.assertEqual(caught.exception.reason, "policy-repository-mismatch")
        self.assertEqual(len(self.fake_t3.dispatched), self.dispatched_before)

    def test_a_legacy_receipt_without_a_canonical_identity_fails_closed(self) -> None:
        self.rewrite_stored_policy(
            {
                key: value
                for key, value in self.receipt().policy.items()
                if key != "repo_identity"
            }
        )
        with self.assertRaises(PolicyRejected) as caught:
            self.followup()
        self.assertEqual(caught.exception.reason, "continuation-admission-required")

    def test_the_continuation_marker_is_a_typed_reason_not_only_a_message(self) -> None:
        self.policy.renew_error = PolicyRejected("reservation is gone")
        with self.assertRaises(PolicyRejected) as caught:
            self.followup()
        self.assertEqual(caught.exception.reason, "continuation-admission-required")
        self.assertEqual(len(self.fake_t3.dispatched), self.dispatched_before)


class TargetLaunchLifecycleTests(_AgentPolicyFixture, unittest.TestCase):
    """A launch binds one session and one decision to one request id.

    An exact retry of that request is idempotent. A different request id is a
    different request and cannot inherit the proof. An approval or an answer for
    the execution that is already running reuses the same request and the same
    reservation generation; it never claims a second one. Once the reservation
    is terminal, released, or expired, a new follow-up needs a newly negotiated
    reservation, which this client does not invent.
    """

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")
        self.claim_request = dict(
            self.submit_request,
            action="claim",
            request_id=PEER_REQUEST_ID,
            digest="d" * 64,
        )
        self.agent.handle(self.claim_request)
        self.fake_t3.thread["activities"] = [
            {"kind": "approval.requested", "payload": {"requestId": "approval-1"}},
            {"kind": "user-input.requested", "payload": {"requestId": "input-1"}},
        ]

    def store(self) -> Any:
        return self.agent._target_store()

    def launch_calls(self) -> list[dict[str, Any]]:
        return [call for call in self.admission.calls if call["phase"] == "launch"]

    def test_a_successful_launch_binds_the_session_and_decision_to_the_request(
        self,
    ) -> None:
        call = self.launch_calls()[0]
        self.assertEqual(call["request_id"], PEER_REQUEST_ID)
        self.assertEqual(call["policy_session_id"], "t3f-" + "0" * 32)
        self.assertEqual(call["decision_id"], "decision-1")
        receipt = self.store().lookup(PEER_REQUEST_ID)
        # Planned and admitted are both durable, and they are distinguishable.
        self.assertEqual(receipt.plan["state"], "planned")
        self.assertEqual(receipt.admission["model"], CANONICAL_MODEL)

    def test_an_exact_retry_is_idempotent_and_launches_once(self) -> None:
        before = len(self.fake_t3.dispatched)
        replay = self.agent.handle(self.claim_request)
        self.assertEqual(replay["thread_id"], "thread-1")
        self.assertEqual(len(self.fake_t3.dispatched), before)
        # The replay short-circuits on the durable receipt, so no second launch
        # admission is taken and no second turn starts.
        self.assertEqual(len(self.launch_calls()), 1)
        self.assertEqual(len(self.policy.ack_calls), 1)

    def test_a_different_request_id_cannot_reuse_the_proof(self) -> None:
        other = "linux-worker-controller-b-12345678-1234-1234-1234-123456789abc"
        with self.assertRaises(PolicyRejected):
            self.agent.handle(
                {
                    "action": "approve",
                    "request_id": other,
                    "thread_id": "thread-1",
                    "approval_id": "approval-1",
                    "decision": "accept",
                    "model_plan": dict(PLAN),
                }
            )
        # Nothing was admitted for the id that has no receipt of its own.
        self.assertEqual(len(self.launch_calls()), 1)

    def test_the_launch_admission_always_names_this_requests_own_id(self) -> None:
        self.admission.calls.clear()
        self.agent.handle(
            {
                "action": "approve",
                "request_id": PEER_REQUEST_ID,
                "thread_id": "thread-1",
                "approval_id": "approval-1",
                "decision": "accept",
                "model_plan": dict(PLAN),
            }
        )
        self.assertEqual(
            [call["request_id"] for call in self.launch_calls()],
            [PEER_REQUEST_ID],
        )

    def test_an_active_approval_reuses_the_reservation_it_already_holds(self) -> None:
        self.admission.calls.clear()
        self.agent.handle(
            {
                "action": "approve",
                "request_id": PEER_REQUEST_ID,
                "thread_id": "thread-1",
                "approval_id": "approval-1",
                "decision": "accept",
                "model_plan": dict(PLAN),
            }
        )
        # The reservation is renewed, never re-taken: no second route decision
        # is asked for and no second reservation exists.
        self.assertEqual(len(self.policy.renew_calls), 1)
        self.assertEqual(self.policy.renew_calls[0]["reservation_id"], "reservation-1")
        self.assertEqual(self.policy.release_calls, [])
        self.assertEqual(
            {call["decision_id"] for call in self.policy.route_calls}, {"decision-1"}
        )
        # And the launch admission is re-taken under the same request id, which
        # is the idempotent retry rather than a new launch.
        self.assertEqual(len(self.launch_calls()), 1)
        self.assertEqual(self.launch_calls()[0]["request_id"], PEER_REQUEST_ID)

    def test_an_active_answer_reuses_the_reservation_it_already_holds(self) -> None:
        self.agent.handle(
            {
                "action": "answer",
                "request_id": PEER_REQUEST_ID,
                "thread_id": "thread-1",
                "input_id": "input-1",
                "answers": {"question-1": ["Option A"]},
                "model_plan": dict(PLAN),
            }
        )
        self.assertEqual(len(self.policy.renew_calls), 1)
        self.assertEqual(self.policy.release_calls, [])

    def test_a_released_reservation_needs_a_new_one_this_client_never_invents(
        self,
    ) -> None:
        self.store().set_reservation_state(PEER_REQUEST_ID, "released")
        self.admission.calls.clear()
        with self.assertRaisesRegex(
            PolicyRejected, "continuation-admission-required"
        ) as caught:
            self.agent.handle(
                {
                    "action": "followup",
                    "request_id": PEER_REQUEST_ID,
                    "thread_id": "thread-1",
                    "prompt": "continue",
                    "model_plan": dict(PLAN),
                }
            )
        self.assertEqual(
            caught.exception.reason, "continuation-admission-required"
        )
        # No admission, no renewal, no invented reservation.
        self.assertEqual(self.launch_calls(), [])
        self.assertEqual(self.policy.renew_calls, [])

    def test_a_stale_policy_proof_refuses_before_anything_is_admitted(self) -> None:
        self.admission.calls.clear()
        self.policy.version = 8
        with self.assertRaises(PolicyRevisionConflict):
            self.agent.handle(
                {
                    "action": "followup",
                    "request_id": PEER_REQUEST_ID,
                    "thread_id": "thread-1",
                    "prompt": "continue",
                    "model_plan": dict(PLAN),
                }
            )
        self.assertEqual(self.launch_calls(), [])

    def test_a_continuation_plan_that_is_not_the_launched_one_is_refused(self) -> None:
        self.admission.calls.clear()
        with self.assertRaises(PolicyRejected):
            self.agent.handle(
                {
                    "action": "followup",
                    "request_id": PEER_REQUEST_ID,
                    "thread_id": "thread-1",
                    "prompt": "continue",
                    "model_plan": {**PLAN, "launch_model": "gpt-5.6-sol"},
                }
            )
        self.assertEqual(self.launch_calls(), [])

    def test_a_continuation_without_a_plan_is_refused(self) -> None:
        self.admission.calls.clear()
        with self.assertRaises(PolicyRejected):
            self.agent.handle(
                {
                    "action": "followup",
                    "request_id": PEER_REQUEST_ID,
                    "thread_id": "thread-1",
                    "prompt": "continue",
                }
            )
        self.assertEqual(self.launch_calls(), [])

    def test_a_re_admission_for_another_model_stops_the_resumed_turn(self) -> None:
        before = len(self.fake_t3.dispatched)
        self.admission.admitted_model = "gpt-5.6-sol"
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(
                {
                    "action": "followup",
                    "request_id": PEER_REQUEST_ID,
                    "thread_id": "thread-1",
                    "prompt": "continue",
                    "model_plan": dict(PLAN),
                }
            )
        self.assertEqual(caught.exception.reason, "policy-plan-mismatch")
        self.assertEqual(len(self.fake_t3.dispatched), before)

    def test_a_resumed_turn_applies_the_native_launch_model(self) -> None:
        self.agent.handle(
            {
                "action": "followup",
                "request_id": PEER_REQUEST_ID,
                "thread_id": "thread-1",
                "prompt": "continue",
                "model_plan": dict(PLAN),
            }
        )
        turn = self.fake_t3.dispatched[-1]
        self.assertEqual(turn["type"], "thread.turn.start")
        self.assertEqual(turn["modelSelection"]["model"], NATIVE_MODEL)


class TargetLaunchOrderingTests(_AgentPolicyFixture, unittest.TestCase):
    """The launch sequence, in the one order that is not circular.

    Route, then prepare the exact selection, then apply the real model metadata
    and the prepared instructions, then acknowledge the policy, and only then
    take a real launch admission, immediately before the turn starts. The old
    flow admitted a model first and asked about policy afterwards, so the
    admission could never carry proof of the policy it was admitted under.
    """

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")
        self.order = self.record_order()

    def test_the_launch_sequence_runs_route_prepare_apply_ack_admit_turn(self) -> None:
        self.agent.handle(self.submit_request)
        self.assertEqual(
            self.order,
            [
                "route",
                "prepare",
                "thread.create",
                "thread.meta.update",
                "acknowledge",
                "admit:launch",
                "thread.turn.start",
            ],
        )

    def test_prepare_replays_the_stamped_decision_and_origin(self) -> None:
        self.agent.handle(self.submit_request)
        call = self.policy.prepare_calls[0]
        self.assertEqual(call["decision_id"], "decision-1")
        # The original stamp origin, not this target's own host id: the Hub
        # retrieves the overrides and origin it already validated for that
        # decision rather than believing an origin restated now.
        self.assertEqual(call["origin"], "controller-host")
        self.assertEqual(call["session_id"], "t3f-" + "0" * 32)

    def test_prepare_names_the_actual_checkout_path_it_holds(self) -> None:
        # The target proves which checkout it is about to run in. Brigade
        # normalizes the path to the canonical identity and compares it with the
        # repository the decision was stored against.
        self.agent.handle(self.submit_request)
        self.assertEqual(self.policy.prepare_calls[0]["repository"], CHECKOUT_PATH)

    def test_route_is_fenced_to_the_stamped_decision_and_canonical_repo(self) -> None:
        self.agent.handle(self.submit_request)
        call = self.policy.route_calls[0]
        self.assertEqual(call["decision_id"], "decision-1")
        self.assertEqual(call["repository"], CANONICAL_REPO)
        self.assertEqual(call["machine"], "linux-worker")

    def test_a_refused_route_stops_the_turn_before_any_prepare(self) -> None:
        self.policy.route_error = PolicyRejected("decision is not yours")
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.order, ["route"])
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_a_refused_launch_admission_starts_no_turn(self) -> None:
        # Everything up to the turn has already happened, including the
        # acknowledgement, and the turn still does not start.
        self.admission.admit_error = PolicyRejected("Fleet Hub denied the launch")
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.submit_request)
        self.assertEqual(
            self.order,
            [
                "route",
                "prepare",
                "thread.create",
                "thread.meta.update",
                "acknowledge",
                "admit:launch",
                "thread.archive",
            ],
        )
        self.assertNotIn("thread.turn.start", self.order)

    def test_a_refused_launch_admission_writes_a_failed_receipt(self) -> None:
        self.admission.admit_error = PolicyRejected("Fleet Hub denied the launch")
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.submit_request)
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text()
        )
        self.assertEqual(receipt["outcome"], "failed")
        # The durable record says a launch was planned and never granted.
        self.assertEqual(receipt["model_plan"]["state"], "planned")
        self.assertNotIn("model_admission", receipt)

    def test_the_launch_admission_carries_the_acknowledged_policy_proof(self) -> None:
        self.agent.handle(self.submit_request)
        launch = [call for call in self.admission.calls if call["phase"] == "launch"]
        self.assertEqual(len(launch), 1)
        self.assertEqual(
            launch[0],
            {
                "consumer": "t3-fleet",
                "phase": "launch",
                "request_id": "req-12345678",
                "seat": "coder",
                "expect_revision": PLAN["roster_revision"],
                "expect_digest": PLAN["roster_digest"],
                "policy_session_id": "t3f-" + "0" * 32,
                "policy_version": 7,
                "policy_digest": POLICY_DIGEST,
                "decision_id": "decision-1",
                # The frozen final proof: the canonical identity the policy was
                # verified under, and the digest of the exact prepared context
                # this target applied and acknowledged.
                "policy_repo_identity": CANONICAL_REPO,
                "policy_context_hash": CONTEXT_HASH,
            },
        )

    def test_a_launch_without_a_prepared_context_never_reaches_the_hub(self) -> None:
        """An enrolled launch that cannot name its context is not admitted.

        Preparing is where this fails closed, so the launch admission is never
        attempted at all rather than being sent with a partial proof.
        """
        self.policy.context_hash = None
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.submit_request)
        self.assertEqual(
            [call for call in self.admission.calls if call["phase"] == "launch"], []
        )

    def test_the_prefix_states_the_route_selection_as_a_receipt_fact(self) -> None:
        self.agent.handle(self.submit_request)
        text = self.turn_text()
        self.assertIn("linux-worker", text)
        self.assertIn("coder", text)
        # The selection is reported as what the Hub route returned, not as a
        # local default that happened to be picked.
        self.assertIn("route decision-1", text)


class TargetPlanIsNotAuthorizationTests(_AgentPolicyFixture, unittest.TestCase):
    """A plan says what a launch would use. It never permits one."""

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")
        self.order = self.record_order()

    def test_a_plan_alone_never_starts_a_turn(self) -> None:
        # The plan is present and valid; the launch admission is refused. The
        # plan carries no authority of its own, so nothing runs.
        self.admission.admit_error = PolicyRejected("Fleet Hub denied the launch")
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.fake_t3.dispatched[-1]["type"], "thread.archive")
        self.assertNotIn("thread.turn.start", self.dispatched_types())

    def test_a_stamped_request_without_a_plan_is_refused(self) -> None:
        request = dict(self.submit_request)
        request.pop("model_plan")
        with self.assertRaises(PolicyRejected):
            self.agent.handle(request)
        self.assertEqual(self.order, [])

    def test_a_stamped_request_cannot_smuggle_an_admission(self) -> None:
        # Handing the target a ready-made admission alongside the plan is an
        # attempt to skip the launch gate; the target refuses the pair.
        request = dict(self.submit_request, model_admission=ADMISSION)
        with self.assertRaises(ValidationError):
            self.agent.handle(request)
        self.assertEqual(self.order, [])

    def test_a_plan_claiming_launch_authorization_never_parses(self) -> None:
        for mutation in (
            {"state": "admitted"},
            {"source": "lkg"},
            {"launch_model_explicit": False},
        ):
            with self.subTest(mutation=sorted(mutation)):
                request = dict(self.submit_request, model_plan={**PLAN, **mutation})
                with self.assertRaises(ValidationError):
                    self.build(enrollment="required").handle(request)

    def test_a_stale_plan_is_refused_before_anything_runs(self) -> None:
        request = dict(
            self.submit_request,
            model_plan={**PLAN, "expires_at": "2026-07-28T00:59:00Z"},
        )
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(request)
        self.assertEqual(caught.exception.reason, "policy-digest-stale")
        self.assertEqual(self.order, [])

    def test_the_native_binding_is_applied_verbatim(self) -> None:
        self.agent.handle(self.submit_request)
        turn = [
            item
            for item in self.fake_t3.dispatched
            if item["type"] == "thread.turn.start"
        ][0]
        # The explicit native model, never the canonical roster identity and
        # never a guess derived from it.
        self.assertEqual(turn["modelSelection"]["model"], NATIVE_MODEL)
        self.assertNotEqual(NATIVE_MODEL, CANONICAL_MODEL)
        self.assertEqual(turn["modelSelection"]["instanceId"], "codex")

    def test_without_an_explicit_binding_the_canonical_model_is_used(self) -> None:
        request = dict(
            self.submit_request,
            model_plan={
                **PLAN,
                "launch_model": CANONICAL_MODEL,
                "launch_model_explicit": False,
            },
        )
        self.policy.prepared_selection = {
            "seat": "coder",
            "provider": "openai",
            "model": CANONICAL_MODEL,
            "reasoning": "high",
            "instance_id": "codex",
        }
        self.agent.handle(request)
        turn = [
            item
            for item in self.fake_t3.dispatched
            if item["type"] == "thread.turn.start"
        ][0]
        self.assertEqual(turn["modelSelection"]["model"], CANONICAL_MODEL)


class TargetPreparedSelectionTests(_AgentPolicyFixture, unittest.TestCase):
    """The prepared session has to be about this exact run, or nothing runs."""

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")
        self.order = self.record_order()

    def assert_refused(self, reason: str, *, admitted: bool = False) -> None:
        """No turn on any gate refusal, whether the gate is before or at launch."""
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(self.submit_request)
        self.assertEqual(caught.exception.reason, reason)
        self.assertNotIn("thread.turn.start", self.order)
        self.assertEqual("admit:launch" in self.order, admitted)

    def test_a_prepared_repository_that_is_not_ours_is_refused(self) -> None:
        self.policy.prepared_repo_identity = "github.com/example-org/other-repo"
        self.assert_refused("policy-repository-mismatch")

    def test_a_prepared_seat_that_is_not_the_routed_one_is_refused(self) -> None:
        self.policy.prepared_selection = {
            "seat": "muse13",
            "provider": "openai",
            "model": CANONICAL_MODEL,
            "launch_model": NATIVE_MODEL,
            "reasoning": "high",
            "instance_id": "codex",
        }
        self.assert_refused("policy-seat-mismatch")

    def test_a_prepared_native_selection_that_drifted_is_refused(self) -> None:
        for mutation in (
            {"model": "gpt-5.6-sol"},
            {"launch_model": "gpt-5.6-terra-2026-01-01"},
            {"provider": "anthropic"},
            {"instance_id": "claude"},
            {"reasoning": "low"},
        ):
            with self.subTest(mutation=sorted(mutation)):
                agent = self.build(enrollment="required")
                self.policy.prepared_selection = {
                    "seat": "coder",
                    "provider": "openai",
                    "model": CANONICAL_MODEL,
                    "launch_model": NATIVE_MODEL,
                    "reasoning": "high",
                    "instance_id": "codex",
                    **mutation,
                }
                with self.assertRaises(PolicyRejected) as caught:
                    agent.handle(self.submit_request)
                self.assertEqual(caught.exception.reason, "policy-selection-mismatch")

    def test_an_admission_for_another_model_never_starts_a_turn(self) -> None:
        # The launch admission happens; what it granted is not what was planned,
        # so the turn still does not start.
        self.admission.admitted_model = "gpt-5.6-sol"
        self.assert_refused("policy-plan-mismatch", admitted=True)

    def test_an_admission_for_another_launch_model_never_starts_a_turn(self) -> None:
        self.admission.admitted_launch_model = "gpt-5.6-terra-2026-01-01"
        self.assert_refused("policy-plan-mismatch", admitted=True)

    def test_an_admission_for_the_planned_launch_model_starts_the_turn(self) -> None:
        self.admission.admitted_launch_model = NATIVE_MODEL
        self.agent.handle(self.submit_request)
        self.assertIn("thread.turn.start", self.order)
        turn = [
            item
            for item in self.fake_t3.dispatched
            if item["type"] == "thread.turn.start"
        ][0]
        self.assertEqual(turn["modelSelection"]["model"], NATIVE_MODEL)

    def test_an_admission_for_another_seat_never_starts_a_turn(self) -> None:
        self.admission.admitted_seat = "muse13"
        self.assert_refused("policy-plan-mismatch", admitted=True)

    def test_unknown_prepared_fields_are_refused(self) -> None:
        original = self.policy.prepare

        def widened(**kwargs: Any) -> dict[str, Any]:
            return {**original(**kwargs), "launch_authorized": True}

        self.policy.prepare = widened
        with self.assertRaises(ValidationError):
            self.agent.handle(self.submit_request)
        self.assertNotIn("thread.turn.start", self.order)


class TargetCanonicalRepositoryTests(_AgentPolicyFixture, unittest.TestCase):
    """The alias is a local name; the canonical identity is what policy is about."""

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")

    def test_acknowledgement_uses_the_canonical_identity(self) -> None:
        self.agent.handle(self.submit_request)
        self.assertEqual(self.policy.ack_calls[0]["repository"], CANONICAL_REPO)

    def test_a_stamp_for_another_repository_is_a_hard_denial(self) -> None:
        request = dict(
            self.submit_request,
            fleet_policy=policy_stamp_payload(
                repo_identity="github.com/example-org/other-repo"
            ),
        )
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(request)
        self.assertEqual(caught.exception.reason, "policy-repository-mismatch")
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual(self.policy.route_calls, [])

    def test_an_enrolled_target_without_a_canonical_mapping_fails_closed(self) -> None:
        agent = self.build(enrollment="required")
        agent.policy.repositories["example-repo"] = RepositoryPolicy(
            path=CHECKOUT_PATH,
            default_branch="main",
            branch_prefix="codex/",
        )
        with self.assertRaises(PolicyRejected) as caught:
            agent.handle(self.submit_request)
        self.assertEqual(caught.exception.reason, "policy-repository-mismatch")
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_the_alias_stays_the_durable_request_identity(self) -> None:
        self.agent.handle(self.submit_request)
        receipt = json.loads(
            (Path(self.directory.name) / "req-12345678-submit.json").read_text()
        )
        # Two identities, each doing its own job. The alias the caller named is
        # what resolved this machine's checkout; the canonical identity is what
        # the durable policy receipt is keyed on.
        self.assertEqual(self.submit_request["repository"], "example-repo")
        self.assertTrue(
            any(CHECKOUT_PATH in argument for call in self.git_calls for argument in call),
            self.git_calls,
        )
        self.assertEqual(receipt["fleet_policy"]["repo_identity"], CANONICAL_REPO)

    def test_a_legacy_unenrolled_target_is_unchanged(self) -> None:
        agent = self.build()
        agent.policy.repositories["example-repo"] = RepositoryPolicy(
            path=CHECKOUT_PATH,
            default_branch="main",
            branch_prefix="codex/",
        )
        result = agent.handle(self.unstamped_request())
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(self.policy.route_calls, [])


class TargetPolicyReasonTests(_AgentPolicyFixture, unittest.TestCase):
    """A denial has to say which denial it was, without leaking anything."""

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")

    def test_a_seat_mismatch_carries_a_typed_reason(self) -> None:
        request = dict(
            self.submit_request, fleet_policy=policy_stamp_payload(seat="other-seat")
        )
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(request)
        self.assertEqual(caught.exception.reason, "policy-seat-mismatch")

    def test_a_machine_mismatch_carries_a_typed_reason(self) -> None:
        request = dict(
            self.submit_request,
            fleet_policy=policy_stamp_payload(machine="windows-worker"),
        )
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(request)
        self.assertEqual(caught.exception.reason, "policy-machine-mismatch")

    def test_an_unknown_reason_is_dropped_rather_than_passed_through(self) -> None:
        # Reasons are an allowlist so a future typo cannot become a leak
        # channel out of the target.
        self.assertIsNone(PolicyRejected("denied", reason="../etc/passwd").reason)
        self.assertIsNone(PolicyRejected("denied", reason=None).reason)

    def test_the_agent_error_envelope_carries_only_allowlisted_reasons(self) -> None:
        envelope = _agent_error(
            PolicyRejected("denied", reason="continuation-admission-required"), None
        )
        self.assertEqual(envelope["error"]["code"], "policy_rejected")
        self.assertEqual(
            envelope["error"]["reason"], "continuation-admission-required"
        )
        self.assertNotIn("reason", _agent_error(PolicyRejected("denied"), None)["error"])


TARGET_DELEGATION_ID = "del-8f2c1a"
TARGET_PARENT_REQUEST_ID = "run-7a1c9e"


def target_delegation_receipt(**overrides: Any) -> dict[str, Any]:
    receipt = {
        "schema": "brigade.fleet_policy_delegation.v1",
        "delegation_id": TARGET_DELEGATION_ID,
        "launch_authorized": False,
        "consumer": "brigade-run",
        "adapter": "t3-fleet",
        "parent_request_id": TARGET_PARENT_REQUEST_ID,
        "decision_id": "decision-1",
        "reservation_id": "reservation-1",
        "session_id": "t3f-" + "0" * 32,
        "repo_identity": CANONICAL_REPO,
        "seat": "coder",
        "workload": "linux-development",
        "policy_version": 7,
        "policy_digest": POLICY_DIGEST,
        "origin_node": "controller-host",
        "target_machine": "linux-worker",
        "target_node": "linux-worker-node",
        "source_revision": FULL_SHA,
        "created_at": "2026-07-28T00:55:00Z",
        "expires_at": "2026-07-28T01:10:00Z",
    }
    receipt.update(overrides)
    return receipt


class TargetPreparedContextHashTests(_AgentPolicyFixture, unittest.TestCase):
    """The acknowledgement names the exact prepared context that was applied."""

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")
        self.order = self.record_order()

    def test_the_acknowledgement_forwards_the_prepared_digest_verbatim(self) -> None:
        self.agent.handle(self.submit_request)
        self.assertEqual(self.policy.ack_calls[0]["context_hash"], CONTEXT_HASH)

    def test_a_replacement_preparation_acknowledges_its_own_digest(self) -> None:
        """A same-version prepare with a different model has a different hash.

        The old applied context must never acknowledge the replacement, so the
        digest that is forwarded is the one this preparation returned, not the
        one an earlier preparation of the same policy version returned.
        """
        replacement = "sha256:" + "e" * 64
        self.policy.context_hash = replacement
        self.agent.handle(self.submit_request)
        self.assertEqual(self.policy.ack_calls[0]["context_hash"], replacement)
        self.assertNotEqual(replacement, CONTEXT_HASH)

    def test_a_stale_context_refusal_stops_the_turn(self) -> None:
        # The Hub compares the digest under its own transaction; a refusal here
        # is what a replaced context looks like from this side.
        self.policy.ack_error = PolicyRejected(
            "fleet policy acknowledged another prepared context"
        )
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.submit_request)
        self.assertNotIn("thread.turn.start", self.order)
        self.assertNotIn("admit:launch", self.order)

    def test_a_preparation_without_a_context_hash_fails_closed(self) -> None:
        self.policy.context_hash = None
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(self.submit_request)
        self.assertEqual(caught.exception.reason, "policy-selection-mismatch")
        # Nothing was applied, acknowledged, or launched.
        self.assertEqual(self.policy.ack_calls, [])
        self.assertEqual(self.order, ["route", "prepare"])

    def test_a_malformed_context_hash_fails_closed(self) -> None:
        self.policy.context_hash = "not-a-digest"
        with self.assertRaises(ValidationError):
            self.agent.handle(self.submit_request)
        self.assertEqual(self.policy.ack_calls, [])

    def test_the_prepared_digest_is_persisted_with_the_request(self) -> None:
        request_id = PEER_REQUEST_ID
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=request_id,
                digest="d" * 64,
            )
        )
        receipt = self.agent._target_store().lookup(request_id)
        self.assertEqual(receipt.policy["context_hash"], CONTEXT_HASH)


class TargetNativeBindingContractTests(_AgentPolicyFixture, unittest.TestCase):
    """`selected.model` is canonical; `selected.launch_model` is native."""

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")

    def turn_model(self) -> str:
        turn = [
            item
            for item in self.fake_t3.dispatched
            if item["type"] == "thread.turn.start"
        ][0]
        return turn["modelSelection"]["model"]

    def test_a_canonical_and_native_model_that_differ_both_have_to_agree(self) -> None:
        self.agent.handle(self.submit_request)
        prepared = self.policy.prepare_calls[0]
        # The request still names the native model the provider is launched on.
        self.assertEqual(prepared["model"], NATIVE_MODEL)
        self.assertEqual(self.turn_model(), NATIVE_MODEL)
        self.assertNotEqual(NATIVE_MODEL, CANONICAL_MODEL)

    def test_a_prepared_canonical_model_that_drifted_is_refused(self) -> None:
        self.policy.prepared_selection = {
            "seat": "coder",
            "provider": "openai",
            "model": "gpt-5.6-sol",
            "launch_model": NATIVE_MODEL,
            "reasoning": "high",
            "instance_id": "codex",
        }
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(self.submit_request)
        self.assertEqual(caught.exception.reason, "policy-selection-mismatch")

    def test_an_absent_launch_model_means_the_canonical_one_verbatim(self) -> None:
        request = dict(
            self.submit_request,
            model_plan={
                **PLAN,
                "launch_model": CANONICAL_MODEL,
                "launch_model_explicit": False,
            },
        )
        self.policy.prepared_selection = {
            "seat": "coder",
            "provider": "openai",
            "model": CANONICAL_MODEL,
            "reasoning": "high",
            "instance_id": "codex",
        }
        self.agent.handle(request)
        self.assertEqual(self.turn_model(), CANONICAL_MODEL)

    def test_an_absent_launch_model_never_satisfies_an_explicit_binding(self) -> None:
        # The plan named a native model explicitly. A preparation that omits it
        # is claiming the canonical identity is the native one, which is a
        # mapping this client refuses to fabricate.
        self.policy.prepared_selection = {
            "seat": "coder",
            "provider": "openai",
            "model": CANONICAL_MODEL,
            "reasoning": "high",
            "instance_id": "codex",
        }
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(self.submit_request)
        self.assertEqual(caught.exception.reason, "policy-selection-mismatch")


class TargetDelegationAdoptionTests(_AgentPolicyFixture, unittest.TestCase):
    """An adopted request is validated against a receipt this target reads."""

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")
        self.policy.delegation_receipt = target_delegation_receipt()
        self.request = dict(
            self.submit_request,
            fleet_policy=policy_stamp_payload(delegation_id=TARGET_DELEGATION_ID),
        )
        self.order = self.record_order()

    def test_the_target_reads_the_receipt_itself_and_never_trusts_the_stamp(self) -> None:
        self.agent.handle(self.request)
        self.assertEqual(
            self.policy.delegation_calls,
            [{"delegation_id": TARGET_DELEGATION_ID}],
        )

    def test_every_downstream_gate_runs_under_the_parent_consumer(self) -> None:
        self.agent.handle(self.request)
        self.assertEqual(self.policy.route_calls[0]["consumer"], "brigade-run")
        self.assertEqual(self.policy.prepare_calls[0]["consumer"], "brigade-run")
        self.assertEqual(self.policy.ack_calls[0]["consumer"], "brigade-run")
        launch = [call for call in self.admission.calls if call["phase"] == "launch"]
        self.assertEqual(launch[0]["consumer"], "brigade-run")

    def test_the_delegation_scopes_prepare_and_launch_admission(self) -> None:
        self.agent.handle(self.request)
        self.assertEqual(
            self.policy.prepare_calls[0]["delegation_id"], TARGET_DELEGATION_ID
        )
        launch = [call for call in self.admission.calls if call["phase"] == "launch"]
        self.assertEqual(launch[0]["delegation_id"], TARGET_DELEGATION_ID)
        # The decision is still replayed, not re-selected.
        self.assertEqual(self.policy.route_calls[0]["decision_id"], "decision-1")

    def test_a_normal_request_stays_on_the_fixed_adapter_consumer(self) -> None:
        self.agent.handle(self.submit_request)
        self.assertEqual(self.policy.delegation_calls, [])
        self.assertEqual(self.policy.route_calls[0]["consumer"], "t3-fleet")
        self.assertEqual(self.policy.prepare_calls[0]["consumer"], "t3-fleet")
        launch = [call for call in self.admission.calls if call["phase"] == "launch"]
        self.assertEqual(launch[0]["consumer"], "t3-fleet")
        self.assertNotIn("delegation_id", launch[0])

    def test_enrollment_off_refuses_adoption(self) -> None:
        agent = self.build(enrollment="off")
        with self.assertRaises(PolicyRejected) as caught:
            agent.handle(self.request)
        self.assertEqual(caught.exception.reason, "delegation-not-enrolled")
        self.assertEqual(self.policy.delegation_calls, [])

    def test_a_receipt_for_another_machine_never_runs(self) -> None:
        self.policy.delegation_receipt = target_delegation_receipt(
            target_machine="second-worker"
        )
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(self.request)
        self.assertEqual(caught.exception.reason, "policy-machine-mismatch")
        self.assertEqual(self.order, [])

    def test_a_receipt_for_another_repository_never_runs(self) -> None:
        self.policy.delegation_receipt = target_delegation_receipt(
            repo_identity="git.example.invalid/team/repository"
        )
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(self.request)
        self.assertEqual(caught.exception.reason, "policy-repository-mismatch")

    def test_a_receipt_for_another_seat_never_runs(self) -> None:
        self.policy.delegation_receipt = target_delegation_receipt(seat="muse13")
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(self.request)
        self.assertEqual(caught.exception.reason, "policy-seat-mismatch")

    def test_a_stamp_that_does_not_match_the_receipt_never_runs(self) -> None:
        for stamp_override, label in (
            ({"session_id": "t3f-" + "9" * 32}, "session"),
            ({"decision_id": "decision-9"}, "decision"),
            ({"reservation_id": "reservation-9"}, "reservation"),
            ({"workload": "windows-development"}, "workload"),
            ({"origin": "other-origin"}, "origin"),
            ({"version": 8}, "version"),
        ):
            with self.subTest(label=label):
                agent = self.build(enrollment="required")
                request = dict(
                    self.submit_request,
                    fleet_policy=policy_stamp_payload(
                        delegation_id=TARGET_DELEGATION_ID, **stamp_override
                    ),
                )
                with self.assertRaises(PolicyRejected) as caught:
                    agent.handle(request)
                self.assertEqual(caught.exception.reason, "delegation-conflict")

    def test_a_dispatched_source_that_is_not_the_delegated_one_never_runs(self) -> None:
        other = "1" * 40
        request = dict(self.request, source_revision=other)
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(request)
        self.assertEqual(caught.exception.reason, "delegation-conflict")
        # Nothing fell back to a local HEAD and no worktree was created.
        self.assertEqual(self.git_calls, [])
        self.assertEqual(self.order, [])

    def test_an_expired_receipt_never_runs(self) -> None:
        self.policy.delegation_receipt = target_delegation_receipt(
            expires_at="2026-07-28T00:59:00Z"
        )
        with self.assertRaises(PolicyRejected):
            self.agent.handle(self.request)
        self.assertEqual(self.order, [])

    def test_a_receipt_that_claims_launch_authorization_never_parses(self) -> None:
        for mutation in (
            {"launch_authorized": True},
            {"consumer": "t3-fleet"},
            {"adapter": "brigade-run"},
        ):
            with self.subTest(mutation=sorted(mutation)):
                agent = self.build(enrollment="required")
                self.policy.delegation_receipt = target_delegation_receipt(**mutation)
                with self.assertRaises(ValidationError):
                    agent.handle(self.request)

    def test_a_caller_supplied_receipt_is_never_authority(self) -> None:
        for field in ("delegation", "delegation_receipt"):
            with self.subTest(field=field):
                agent = self.build(enrollment="required")
                request = dict(
                    self.request, **{field: target_delegation_receipt()}
                )
                with self.assertRaises(ValidationError):
                    agent.handle(request)
        self.assertEqual(self.order, [])

    def test_the_adopted_request_takes_no_second_route_or_reservation(self) -> None:
        self.agent.handle(self.request)
        # One decision replay, one prepare, one launch admission, one turn.
        self.assertEqual(len(self.policy.route_calls), 1)
        self.assertEqual(len(self.policy.prepare_calls), 1)
        self.assertEqual(
            len([call for call in self.admission.calls if call["phase"] == "launch"]),
            1,
        )
        self.assertEqual(self.policy.renew_calls, [])
        self.assertEqual(self.policy.release_calls, [])


class TargetDelegatedHandoffTests(_AgentPolicyFixture, unittest.TestCase):
    """A finished adoption reports the work this target still holds."""

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")
        self.policy.delegation_receipt = target_delegation_receipt()
        self.request_id = PEER_REQUEST_ID
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=self.request_id,
                digest="d" * 64,
                fleet_policy=policy_stamp_payload(
                    delegation_id=TARGET_DELEGATION_ID
                ),
            )
        )
        self.worktree = Path(self.directory.name) / "worktrees" / self.request_id
        self.worktree.mkdir(parents=True, exist_ok=True)

    def reconcile(self) -> dict[str, Any]:
        return self.agent.handle(
            {"action": "reconcile", "request_id": self.request_id}
        )

    def finish(self) -> None:
        self.fake_t3.thread["latestTurn"] = {"state": "completed", "turnId": "turn-1"}
        self.fake_t3.thread["session"] = {"status": "ready"}

    def test_a_terminal_delegated_result_reports_a_pending_handoff(self) -> None:
        self.finish()
        payload = self.reconcile()
        self.assertEqual(payload["state"], "succeeded")
        self.assertEqual(payload["disposition"], "pending-handoff")
        self.assertEqual(
            payload["handoff"],
            {
                "target_machine": "linux-worker",
                "repository": CANONICAL_REPO,
                "branch": f"codex/{self.request_id}",
                "source_revision": FULL_SHA,
                "request_id": self.request_id,
                "worktree_ref": f"t3-fleet:worktree:{self.request_id}",
            },
        )
        # The reference is opaque and target-owned; it is not the path.
        self.assertNotIn(str(self.worktree), payload["handoff"]["worktree_ref"])

    def test_a_running_delegated_result_claims_no_completion(self) -> None:
        payload = self.reconcile()
        self.assertEqual(payload["state"], "running")
        self.assertNotIn("disposition", payload)
        self.assertNotIn("handoff", payload)

    def test_an_unknown_state_claims_no_completed_handoff(self) -> None:
        self.fake_t3.thread["latestTurn"] = {"state": "running"}
        self.fake_t3.thread["session"] = {"status": "idle"}
        payload = self.reconcile()
        self.assertEqual(payload["state"], "unknown")
        self.assertNotIn("disposition", payload)
        self.assertNotIn("handoff", payload)

    def test_a_retained_worktree_is_what_makes_the_handoff_true(self) -> None:
        self.finish()
        for item in sorted(self.worktree.rglob("*"), reverse=True):
            item.unlink()
        self.worktree.rmdir()
        payload = self.reconcile()
        self.assertEqual(payload["state"], "succeeded")
        self.assertNotIn("disposition", payload)

    def test_a_pre_launch_failure_omits_the_handoff(self) -> None:
        """The frozen contract's own case: a launch that never started.

        A pre-turn failure removes the worktree it had created, so the terminal
        `failed` state that follows has nothing retained to hand off. The
        worktree here is materialised and removed for real by the git runner,
        so what is asserted is the cleanup and the omission wired together,
        rather than a directory that never existed.
        """
        request_id = (
            "linux-worker-controller-c-32345678-1234-1234-1234-123456789abc"
        )
        created: list[Path] = []

        def worktree_git_runner(argv: list[str], timeout: float) -> Any:
            self.git_calls.append(list(argv))
            if "worktree" in argv and "add" in argv:
                path = Path(argv[argv.index("add") + 3])
                path.mkdir(parents=True, exist_ok=True)
                created.append(path)
            elif "worktree" in argv and "remove" in argv:
                path = Path(argv[-1])
                if path.exists():
                    path.rmdir()
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        agent = self.build(enrollment="required")
        agent._git_runner = worktree_git_runner
        # The fixture already holds this repository's lease, and a target runs
        # one claim per repository at a time.
        agent.handle({"action": "release", "request_id": self.request_id})
        # thread.create lands, then thread.meta.update fails: the turn was
        # never dispatched, so this is pre-launch by construction. The bound is
        # cumulative across the fixture's own claim, so it is taken from where
        # the shared FakeT3 already stands.
        self.fake_t3.fail_after = len(self.fake_t3.dispatched) + 1
        with self.assertRaises(Exception):
            agent.handle(
                dict(
                    self.submit_request,
                    action="claim",
                    request_id=request_id,
                    digest="f" * 64,
                    fleet_policy=policy_stamp_payload(
                        delegation_id=TARGET_DELEGATION_ID
                    ),
                )
            )
        self.fake_t3.fail_after = None

        self.assertEqual(len(created), 1)
        self.assertFalse(created[0].exists())
        payload = agent.handle({"action": "lookup", "request_id": request_id})
        self.assertEqual(payload["state"], "failed")
        self.assertNotIn("disposition", payload)
        self.assertNotIn("handoff", payload)

    def test_an_undelegated_request_never_reports_a_handoff(self) -> None:
        other = "linux-worker-controller-b-12345678-1234-1234-1234-123456789abc"
        agent = self.build(enrollment="required")
        agent.handle({"action": "release", "request_id": self.request_id})
        agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=other,
                digest="e" * 64,
            )
        )
        (Path(self.directory.name) / "worktrees" / other).mkdir(
            parents=True, exist_ok=True
        )
        self.finish()
        payload = agent.handle({"action": "reconcile", "request_id": other})
        self.assertEqual(payload["state"], "succeeded")
        self.assertNotIn("disposition", payload)


class TargetDelegatedContinuationTests(_AgentPolicyFixture, unittest.TestCase):
    """An adopted execution continues under the same generation, or not at all."""

    def setUp(self) -> None:
        super().setUp()
        self.agent = self.build(enrollment="required")
        self.policy.delegation_receipt = target_delegation_receipt()
        self.request_id = PEER_REQUEST_ID
        self.agent.handle(
            dict(
                self.submit_request,
                action="claim",
                request_id=self.request_id,
                digest="d" * 64,
                fleet_policy=policy_stamp_payload(
                    delegation_id=TARGET_DELEGATION_ID
                ),
            )
        )
        self.fake_t3.thread["activities"] = [
            {"kind": "approval.requested", "payload": {"requestId": "approval-1"}},
        ]

    def approve(self) -> dict[str, Any]:
        return self.agent.handle(
            {
                "action": "approve",
                "request_id": self.request_id,
                "thread_id": "thread-1",
                "model_plan": dict(PLAN),
                "approval_id": "approval-1",
                "decision": "accept",
            }
        )

    def test_an_active_approval_resumes_under_the_parent_consumer(self) -> None:
        before = len(self.policy.route_calls)
        self.approve()
        launch = [call for call in self.admission.calls if call["phase"] == "launch"]
        self.assertEqual(launch[-1]["consumer"], "brigade-run")
        self.assertEqual(launch[-1]["delegation_id"], TARGET_DELEGATION_ID)
        self.assertEqual(launch[-1]["request_id"], self.request_id)
        self.assertEqual(
            self.policy.route_calls[before]["consumer"], "brigade-run"
        )
        # It reuses the reservation it already holds rather than taking another.
        self.assertEqual(len(self.policy.renew_calls), 1)

    def test_a_terminal_execution_blocks_a_new_delegated_follow_up(self) -> None:
        # The target settles its own reservation on a durable terminal outcome,
        # and a new follow-up after that needs a freshly negotiated one, which
        # this client never invents from an adoption that has already run.
        self.fake_t3.thread["activities"] = []
        self.fake_t3.thread["latestTurn"] = {"state": "completed", "turnId": "turn-1"}
        self.fake_t3.thread["session"] = {"status": "ready"}
        settled = self.agent.handle(
            {"action": "reconcile", "request_id": self.request_id}
        )
        self.assertEqual(settled["state"], "succeeded")
        self.assertEqual(settled["fleet_reservation"]["state"], "released")
        with self.assertRaises(PolicyRejected) as caught:
            self.agent.handle(
                {
                    "action": "reserve_followup",
                    "request_id": self.request_id,
                    "thread_id": "thread-1",
                    "model_plan": dict(PLAN),
                }
            )
        self.assertEqual(
            caught.exception.reason, "continuation-admission-required"
        )
        self.assertEqual(len(self.policy.release_calls), 1)

    def test_a_released_reservation_blocks_the_delegated_follow_up(self) -> None:
        self.agent._target_store().set_reservation_state(
            self.request_id, "released"
        )
        with self.assertRaises(PolicyRejected) as caught:
            self.approve()
        self.assertEqual(
            caught.exception.reason, "continuation-admission-required"
        )


def _claim_request(base: dict[str, Any], index: int) -> dict[str, Any]:
    return dict(
        base,
        action="claim",
        request_id=f"linux-worker-controller-a-{index:08d}-1234-1234-1234-123456789abc",
        digest=f"{index:x}" * 64 if index < 16 else "f" * 64,
    )


class AgentRepositoryConcurrencyTests(unittest.TestCase):
    setUp = AgentTests.setUp

    def _with_policy(self, **overrides: Any) -> None:
        self.agent.policy = AgentPolicy(**{**self.agent.policy.__dict__, **overrides})

    def test_default_policy_lets_a_repository_use_host_capacity(self) -> None:
        first = self.agent.handle(_claim_request(self.submit_request, 1))
        second = self.agent.handle(_claim_request(self.submit_request, 2))
        self.assertEqual(first["state"], "running")
        self.assertEqual(second["state"], "running")
        self.assertNotEqual(first["thread_id"], second["thread_id"])
        worktrees = [call[-2] for call in self.git_calls if call[3:5] == ["worktree", "add"]]
        self.assertEqual(len(set(worktrees)), 2)
        doctor = self.agent.handle({"action": "doctor"})
        self.assertEqual(doctor["repository_limit"], 2)

    def test_explicit_repository_limit_one_rejects_second_claim(self) -> None:
        self._with_policy(max_concurrent_per_repository=1)
        self.agent.handle(_claim_request(self.submit_request, 1))
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            self.agent.handle(_claim_request(self.submit_request, 2))
        self.assertEqual(self.agent.handle({"action": "doctor"})["repository_limit"], 1)

    def test_worktree_git_steps_run_under_the_repository_lock(self) -> None:
        from t3_fleet.agent import _exclusive_file_lock

        lock_path = self.agent._git_lock_path("/srv/repos/example")
        observed: dict[str, bool] = {}
        original = self.agent._git_runner

        def checking_runner(argv: list[str], timeout: float) -> Any:
            try:
                with _exclusive_file_lock(lock_path, timeout=0):
                    observed[argv[3]] = False
            except TargetBusy:
                observed[argv[3]] = True
            return original(argv, timeout)

        self.agent._git_runner = checking_runner
        self.agent.handle(_claim_request(self.submit_request, 1))
        # The read-only revision check needs no lock; the writes do.
        self.assertEqual(
            observed, {"cat-file": False, "worktree": True, "config": True}
        )
        # The lock is released once the worktree exists.
        with _exclusive_file_lock(lock_path, timeout=0):
            pass

    def test_busy_repository_lock_declines_before_any_side_effect(self) -> None:
        from t3_fleet import agent as agent_module

        lock_path = self.agent._git_lock_path("/srv/repos/example")
        request = _claim_request(self.submit_request, 1)
        original_budget = agent_module._GIT_PREPARE_BUDGET
        agent_module._GIT_PREPARE_BUDGET = 0
        self.addCleanup(setattr, agent_module, "_GIT_PREPARE_BUDGET", original_budget)
        with agent_module._exclusive_file_lock(lock_path, timeout=0):
            with self.assertRaisesRegex(TargetBusy, "git operations stayed busy") as caught:
                self.agent.handle(request)
        self.assertEqual(caught.exception.reason, "repository-busy")
        # Only the lock-free revision check ran: no worktree, no branch, and no
        # T3 thread to archive.
        self.assertEqual([call[3] for call in self.git_calls], ["cat-file"])
        self.assertEqual(self.fake_t3.dispatched, [])
        # The declined claim is forgotten, so the target holds no trace of it.
        missing = self.agent.handle({"action": "lookup", "request_id": request["request_id"]})
        self.assertEqual(missing["state"], "not_found")
        # The same request can be claimed again once the repository is free.
        retried = self.agent.handle(request)
        self.assertEqual(retried["state"], "running")
        self._with_policy(max_concurrent_per_repository=1)
        with self.assertRaisesRegex(TargetBusy, "repository is busy"):
            self.agent.handle(_claim_request(self.submit_request, 2))

    def test_followup_refreshes_the_repository_lease(self) -> None:
        AgentTests._seed_admitted_request(self)
        store = self.agent._target_store()
        calls: list[tuple[str, float]] = []
        original = store.refresh_lease

        def tracking(request_id: str, *, lease_ttl: float) -> None:
            calls.append((request_id, lease_ttl))
            original(request_id, lease_ttl=lease_ttl)

        store.refresh_lease = tracking  # type: ignore[method-assign]
        self.agent.handle(
            {
                "action": "followup",
                "request_id": "req-12345678",
                "thread_id": "thread-1",
                "prompt": "next step",
                "model_admission": ADMISSION,
            }
        )
        self.assertEqual(calls, [("req-12345678", 6 * 60 * 60)])



class _SlowFetchGit:
    """A shared fake checkout whose revision appears only after a slow fetch.

    It records how many fetches and how many write steps ever overlapped, so a
    test can prove the per-repository locks serialized them.
    """

    def __init__(self, fetch_seconds: float) -> None:
        self.fetch_seconds = fetch_seconds
        self.guard = threading.Lock()
        self.fetched = False
        self.calls: list[list[str]] = []
        self.active = {"fetch": 0, "write": 0}
        self.peak = {"fetch": 0, "write": 0}

    def _enter(self, kind: str) -> None:
        with self.guard:
            self.active[kind] += 1
            self.peak[kind] = max(self.peak[kind], self.active[kind])

    def _leave(self, kind: str) -> None:
        with self.guard:
            self.active[kind] -= 1

    def __call__(self, argv: list[str], timeout: float) -> Any:
        with self.guard:
            self.calls.append(list(argv))
        verb = argv[3]
        if verb == "cat-file":
            with self.guard:
                present = self.fetched
            return subprocess.CompletedProcess(
                argv, 0 if present else 128, stdout="", stderr="" if present else "missing"
            )
        kind = "fetch" if verb == "fetch" else "write"
        self._enter(kind)
        try:
            time.sleep(self.fetch_seconds if kind == "fetch" else 0.02)
            if kind == "fetch":
                with self.guard:
                    self.fetched = True
        finally:
            self._leave(kind)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def count(self, *verbs: str) -> int:
        return sum(1 for call in self.calls if call[3 : 3 + len(verbs)] == list(verbs))


class AgentParallelSameRepositoryTests(unittest.TestCase):
    """Separate agent processes claiming one repository at the same moment."""

    setUp = AgentTests.setUp

    def _agents(self, count: int, git: _SlowFetchGit) -> list[T3Agent]:
        policy = AgentPolicy(**{**self.agent.policy.__dict__, "active_limit": count + 2})
        return [
            T3Agent(
                policy,
                FakeT3(),
                hostname=lambda: "linux-worker",
                uuid_factory=lambda: str(uuid.uuid4()),
                now=lambda: "2026-07-28T01:00:00Z",
                git_runner=git,
                admission=FakeAdmission(),
            )
            for _ in range(count)
        ]

    def _run_parallel(self, agents: list[T3Agent], offset: int = 0) -> list[Any]:
        barrier = threading.Barrier(len(agents))

        def claim(index: int) -> Any:
            barrier.wait()
            try:
                return agents[index].handle(
                    _claim_request(self.submit_request, offset + index + 1)
                )
            except Exception as error:  # noqa: BLE001 - the test inspects it
                return error

        with ThreadPoolExecutor(max_workers=len(agents)) as pool:
            return list(pool.map(claim, range(len(agents))))

    def test_parallel_claims_with_a_slow_fetch_all_start(self) -> None:
        git = _SlowFetchGit(fetch_seconds=0.6)
        agents = self._agents(6, git)
        results = self._run_parallel(agents)
        self.assertEqual(
            [r["state"] if isinstance(r, dict) else repr(r) for r in results],
            ["running"] * 6,
        )
        # One fetch served everyone, and no git write ever overlapped another.
        self.assertEqual(git.count("fetch"), 1)
        self.assertEqual(git.peak, {"fetch": 1, "write": 1})
        self.assertEqual(git.count("worktree", "add"), 6)
        self.assertEqual(len({r["thread_id"] for r in results}), 6)
        for agent in agents:
            self.assertEqual(
                [c["type"] for c in agent.t3.dispatched],
                ["thread.create", "thread.meta.update", "thread.turn.start"],
            )

    def test_waiters_past_the_budget_decline_without_side_effects(self) -> None:
        from t3_fleet import agent as agent_module

        for name, value in (("_GIT_PREPARE_BUDGET", 1.0), ("_GIT_MIN_FETCH_WINDOW", 0.2)):
            self.addCleanup(setattr, agent_module, name, getattr(agent_module, name))
            setattr(agent_module, name, value)
        git = _SlowFetchGit(fetch_seconds=1.5)
        agents = self._agents(4, git)
        results = self._run_parallel(agents)
        started = [r for r in results if isinstance(r, dict)]
        declined = [r for r in results if isinstance(r, TargetBusy)]
        self.assertEqual((len(started), len(declined)), (1, 3), results)
        self.assertTrue(all(error.reason == "repository-busy" for error in declined))
        # A declined claim created no worktree, no branch, and no thread, and
        # left nothing behind on the target.
        self.assertEqual(git.count("worktree", "add"), 1)
        self.assertEqual(sum(len(agent.t3.dispatched) > 0 for agent in agents), 1)
        store = agents[0]._target_store()
        busy = [i for i, r in enumerate(results) if isinstance(r, TargetBusy)]
        for index in busy:
            request_id = _claim_request(self.submit_request, index + 1)["request_id"]
            self.assertIsNone(store.lookup(request_id))
        # Retried once the fetch is done, every declined request starts.
        for index in busy:
            retried = agents[index].handle(_claim_request(self.submit_request, index + 1))
            self.assertEqual(retried["state"], "running")
        self.assertEqual(git.count("fetch"), 1)


class AgentGitLockErrorTests(unittest.TestCase):
    setUp = AgentTests.setUp

    def test_contention_errnos_wait_then_decline_as_busy(self) -> None:
        from t3_fleet.agent import _exclusive_file_lock

        for code in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EACCES):
            with self.subTest(errno=errno.errorcode[code]):
                clock = [0.0]
                sleeps: list[float] = []

                def contended(handle: Any) -> None:
                    raise OSError(code, "locked")

                def sleep(seconds: float) -> None:
                    sleeps.append(seconds)
                    clock[0] += seconds

                with self.assertRaises(TargetBusy) as raised:
                    with _exclusive_file_lock(
                        Path(self.directory.name) / "locks" / "x.lock",
                        timeout=1.0,
                        clock=lambda: clock[0],
                        sleep=sleep,
                        try_lock=contended,
                    ):
                        self.fail("lock must not be acquired")
                self.assertEqual(raised.exception.reason, "repository-busy")
                self.assertGreater(len(sleeps), 1)

    def test_other_lock_errors_surface_immediately(self) -> None:
        from t3_fleet.agent import _exclusive_file_lock

        for code in (errno.ENOLCK, errno.EBADF, errno.EIO):
            with self.subTest(errno=errno.errorcode[code]):
                sleeps: list[float] = []

                def broken(handle: Any) -> None:
                    raise OSError(code, os.strerror(code))

                with self.assertRaises(UnknownState) as raised:
                    with _exclusive_file_lock(
                        Path(self.directory.name) / "locks" / "x.lock",
                        timeout=30.0,
                        sleep=sleeps.append,
                        try_lock=broken,
                    ):
                        self.fail("lock must not be acquired")
                self.assertNotIsInstance(raised.exception, TargetBusy)
                self.assertIn(errno.errorcode[code], raised.exception.message)
                self.assertEqual(sleeps, [])

    def test_a_broken_lock_fails_the_claim_before_any_thread_exists(self) -> None:
        from t3_fleet.agent import _exclusive_file_lock

        def broken(handle: Any) -> None:
            raise OSError(errno.ENOLCK, "No locks available")

        self.agent._repository_git_lock = lambda path, *, timeout, kind="worktree": (  # type: ignore[method-assign]
            _exclusive_file_lock(
                self.agent._git_lock_path(path, kind), timeout=timeout, try_lock=broken
            )
        )
        request = _claim_request(self.submit_request, 1)
        with self.assertRaisesRegex(UnknownState, "ENOLCK") as raised:
            self.agent.handle(request)
        self.assertNotIsInstance(raised.exception, TargetBusy)
        self.assertEqual(self.fake_t3.dispatched, [])
        self.assertEqual([call[3] for call in self.git_calls], ["cat-file"])
        # A real failure keeps the claim's record but hands its slot back.
        receipt = self.agent.handle({"action": "lookup", "request_id": request["request_id"]})
        self.assertIsNone(receipt["thread_id"])

    def test_cleanup_on_a_busy_lock_is_deferred_not_leaked(self) -> None:
        from t3_fleet import agent as agent_module

        self.addCleanup(
            setattr, agent_module, "_GIT_CLEANUP_LOCK_TIMEOUT", agent_module._GIT_CLEANUP_LOCK_TIMEOUT
        )
        agent_module._GIT_CLEANUP_LOCK_TIMEOUT = 0
        lock_path = self.agent._git_lock_path(CHECKOUT_PATH)
        holder: list[Any] = []

        def fail_update_while_locked(command: dict) -> dict:
            if command["type"] == "thread.meta.update":
                # Another request takes the repository lock just as this one
                # needs it for cleanup.
                lock = agent_module._exclusive_file_lock(lock_path, timeout=0)
                lock.__enter__()
                holder.append(lock)
                raise RuntimeError("update failed")
            return FakeT3.dispatch(self.fake_t3, command)

        self.fake_t3.dispatch = fail_update_while_locked
        with self.assertRaisesRegex(RuntimeError, "update failed"):
            self.agent.handle(_claim_request(self.submit_request, 1))
        holder[0].__exit__(None, None, None)
        first_worktree = self.agent._worktree_path(_claim_request(self.submit_request, 1)["request_id"])
        self.assertFalse(any(call[3:5] == ["worktree", "remove"] for call in self.git_calls))
        pending = list((Path(self.directory.name) / "locks" / "pending-cleanup").glob("*.json"))
        self.assertEqual(len(pending), 1)

        # The next request for the repository finishes the removal under the lock.
        self.fake_t3.dispatch = lambda command: FakeT3.dispatch(self.fake_t3, command)
        self.agent.handle(_claim_request(self.submit_request, 2))
        removals = [call for call in self.git_calls if call[3:5] == ["worktree", "remove"]]
        self.assertEqual([call[6] for call in removals], [first_worktree])
        branch = _claim_request(self.submit_request, 1)["request_id"]
        self.assertIn(
            ["git", "-C", CHECKOUT_PATH, "branch", "-D", f"codex/{branch}"], self.git_calls
        )
        self.assertEqual(
            list((Path(self.directory.name) / "locks" / "pending-cleanup").glob("*.json")), []
        )

    def test_reconcile_renews_the_lease_of_a_running_request(self) -> None:
        request = _claim_request(self.submit_request, 1)
        self.agent.handle(request)
        store = self.agent._target_store()
        with sqlite3.connect(store.path) as connection:
            connection.execute("DELETE FROM repository_leases")
        self.agent.handle({"action": "reconcile", "request_id": request["request_id"]})
        with sqlite3.connect(store.path) as connection:
            rows = connection.execute(
                "SELECT request_id, expires_at FROM repository_leases"
            ).fetchall()
        self.assertEqual([row[0] for row in rows], [request["request_id"]])
        self.assertGreater(rows[0][1], time.time() + 6 * 60 * 60 - 60)

    def test_a_busy_error_after_thread_creation_is_not_reported_as_unstarted(self) -> None:
        def busy_update(command: dict) -> dict:
            if command["type"] == "thread.meta.update":
                raise TargetBusy("late busy", reason="repository-busy")
            return FakeT3.dispatch(self.fake_t3, command)

        self.fake_t3.dispatch = busy_update
        request = _claim_request(self.submit_request, 1)
        with self.assertRaises(UnknownState) as raised:
            self.agent.handle(request)
        self.assertNotIsInstance(raised.exception, TargetBusy)
        # The thread existed, so the claim is kept and settled, not forgotten.
        self.assertIsNotNone(self.agent._target_store().lookup(request["request_id"]))

    def test_agent_error_names_the_busy_reason(self) -> None:
        payload = _agent_error(TargetBusy("repository git operations stayed busy on this target", reason="repository-busy"), {})
        self.assertEqual(payload["error"]["code"], "target_busy")
        self.assertEqual(payload["error"]["reason"], "repository-busy")
        # A plain HostUnavailable never claims to be a clean decline.
        plain = _agent_error(HostUnavailable("x", reason="repository-busy"), {})
        self.assertNotIn("reason", plain["error"])


class ContinuationAdmissionWindowTests(unittest.TestCase):
    """answer/approve on a request whose turn outlived its admission window.

    Observed live on 2026-10-10: the Hub replays a target-phase admission by its
    idempotency key, so re-admitting under the original request id returns the
    original fifteen-minute admission. Once that has passed every continuation
    failed with `admission-expired`, which surfaced as a lost mutation.
    """

    EXPIRED = {**ADMISSION, "expires_at": "2026-07-28T00:15:00Z"}

    def setUp(self) -> None:
        self.fixture = AgentTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.agent = self.fixture.agent
        self.admission = self.fixture.admission
        self.fake_t3 = self.fixture.fake_t3
        self.fake_t3.thread["activities"] = [
            {"kind": "user-input.requested", "payload": {"requestId": "input-current"}}
        ]

    def _seed(self, admission: dict[str, object]) -> None:
        store = self.agent._target_store()
        store.claim(
            TargetClaim(
                request_id="req-12345678",
                digest="a" * 64,
                target_id="linux-worker",
                repository="example-repo",
                source_revision=FULL_SHA,
                prompt_sha256="b" * 64,
                prompt_bytes=1,
                admission=admission,
            ),
            active_limit=2,
            reservation_ttl=60,
        )
        store.attach_thread("req-12345678", "thread-1")

    def _answer(self, admission: dict[str, object]) -> dict[str, object]:
        return self.agent.handle(
            {
                "action": "answer",
                "request_id": "req-12345678",
                "thread_id": "thread-1",
                "input_id": "input-current",
                "answers": {"0": "use bubblewrap"},
                "model_admission": admission,
            }
        )

    def test_expired_admission_is_reconfirmed_under_a_continuation_key(self) -> None:
        self._seed(self.EXPIRED)
        result = self._answer(self.EXPIRED)
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.fake_t3.dispatched[-1]["type"], "thread.user-input.respond")
        (call,) = self.admission.calls
        self.assertEqual(call["phase"], "target")
        self.assertEqual(call["expect_revision"], ADMISSION["roster_revision"])
        self.assertEqual(call["expect_digest"], ADMISSION["roster_digest"])
        # Deterministic per ten-minute window, so a retry replays the same
        # fresh admission instead of minting another one.
        bucket = int(datetime(2026, 7, 28, 1, 0, tzinfo=UTC).timestamp()) // 600
        self.assertEqual(call["request_id"], f"req-12345678.cont-{bucket}")

    def test_a_replayed_expired_admission_is_retried_once_under_the_continuation_key(self) -> None:
        self._seed(ADMISSION)
        original = FakeAdmission.admit
        replies: list[Exception | None] = [
            PolicyRejected("Fleet Hub model admission has expired", reason="model-admission-expired"),
            None,
        ]

        def admit(request: dict[str, object]) -> dict[str, object]:
            error = replies.pop(0)
            if error is not None:
                self.admission.calls.append(dict(request))
                raise error
            return original(self.admission, request)

        self.admission.admit = admit  # type: ignore[method-assign]
        result = self._answer(ADMISSION)
        self.assertEqual(result["state"], "running")
        keys = [call["request_id"] for call in self.admission.calls]
        self.assertEqual(keys[0], "req-12345678")
        self.assertTrue(str(keys[1]).startswith("req-12345678.cont-"))

    def test_a_changed_identity_under_the_continuation_key_still_fails_closed(self) -> None:
        self._seed(self.EXPIRED)
        self.admission.admitted_model = "other-model"
        with self.assertRaises(ValidationError):
            self._answer(self.EXPIRED)
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_submit_admission_keeps_the_request_id_key(self) -> None:
        self.agent.handle(self.fixture.submit_request)
        self.assertEqual(self.admission.calls[0]["request_id"], "req-12345678")


class AgentConstructionTests(unittest.TestCase):
    def _config(self, target_origin: str, foreign_origin: str) -> str:
        return f'''\
schema = "t3-fleet.config.v1"
controller_host = "target-a"
global_active_limit = 2

[hosts.target-a]
enabled = true
platform = "linux"
transport = "local"
agent_command = ["/opt/t3-fleet/bin/t3-fleet-agent"]
active_limit = 1
expected_hostname = "target-a"
t3_url = {json.dumps(target_origin)}

[hosts.target-b]
enabled = true
platform = "linux"
transport = "ssh"
ssh_alias = "target-b"
agent_command = ["/opt/t3-fleet/bin/t3-fleet-agent"]
active_limit = 1
expected_hostname = "target-b"
t3_url = {json.dumps(foreign_origin)}

[repositories.example]
default_branch = "main"
branch_prefix = "codex/"

[repositories.example.paths]
target-a = "/srv/repos/example"
target-b = "/srv/repos/example"
'''

    def _write_config(self, directory: str, text: str) -> Path:
        path = Path(directory) / "fleet.toml"
        path.write_text(text, encoding="utf-8")
        path.chmod(0o600)
        return path

    def test_build_uses_valid_selected_origin_and_retains_foreign_origin(self) -> None:
        from t3_fleet import agent as agent_module

        with tempfile.TemporaryDirectory() as directory:
            path = self._write_config(
                directory,
                self._config(
                    "http://127.0.0.1:3773",  # content-guard: allow loopback-ipv4
                    "https://unused.example.invalid",
                ),
            )
            built = agent_module._build_agent(
                {"T3_FLEET_CONFIG": str(path), "T3_FLEET_HOST": "target-a"}
            )
        self.assertEqual(built.policy.host_id, "target-a")
        self.assertEqual(built.t3._base_url, "http://127.0.0.1:3773/")  # content-guard: allow loopback-ipv4

    def test_build_rejects_each_invalid_selected_origin_before_client_construction(self) -> None:
        from unittest.mock import patch

        from t3_fleet import agent as agent_module

        cases = (
            ("target-a", "https://selected.example.invalid", "http://127.0.0.1:3774"),  # content-guard: allow loopback-ipv4
            ("target-b", "http://127.0.0.1:3773", "https://foreign.example.invalid"),  # content-guard: allow loopback-ipv4
        )
        for selected, target_origin, foreign_origin in cases:
            with self.subTest(selected=selected), tempfile.TemporaryDirectory() as directory:
                path = self._write_config(
                    directory,
                    self._config(target_origin, foreign_origin),
                )
                original_select = agent_module._select_local_host
                with (
                    patch.object(
                        agent_module,
                        "_select_local_host",
                        wraps=original_select,
                    ) as select,
                    patch.object(
                        agent_module,
                        "T3SessionIssuer",
                        side_effect=AssertionError("issuer constructed before origin validation"),
                    ),
                    patch.object(
                        agent_module,
                        "T3Client",
                        side_effect=AssertionError("client constructed before origin validation"),
                    ),
                ):
                    with self.assertRaisesRegex(ConfigError, "invalid T3 origin"):
                        agent_module._build_agent(
                            {"T3_FLEET_CONFIG": str(path), "T3_FLEET_HOST": selected}
                        )
                select.assert_called_once()


class AgentNotStartedMarkerTests(unittest.TestCase):
    """The agent says when an answer or approval failed before T3 saw it."""

    def setUp(self) -> None:
        self.fixture = AgentTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture._seed_admitted_request()
        self.fake_t3 = self.fixture.fake_t3

    def _run(self, request: dict[str, object]) -> dict[str, object]:
        import io
        from unittest.mock import patch

        from t3_fleet import agent as agent_module

        stdout = io.StringIO()
        with patch.object(agent_module, "_build_agent", lambda environ: self.fixture.agent):
            agent_module.main(
                [], stdin=io.StringIO(json.dumps(request) + "\n"), stdout=stdout,
                stderr=io.StringIO(), environ={},
            )
        return json.loads(stdout.getvalue())

    def _answer(self) -> dict[str, object]:
        return {
            "action": "answer", "request_id": "req-12345678", "thread_id": "thread-1",
            "input_id": "input-current", "answers": {"0": "yes"}, "model_admission": ADMISSION,
        }

    def test_refusal_before_dispatch_is_marked_not_started_with_its_reason(self) -> None:
        self.fake_t3.thread["activities"] = []
        error = self._run(self._answer())["error"]
        self.assertEqual(error["code"], "invalid_request")
        self.assertIs(error["not_started"], True)
        self.assertEqual(error["detail"], "user-input request is not currently pending")
        self.assertEqual(self.fake_t3.dispatched, [])

    def test_admission_failure_before_dispatch_is_marked_not_started(self) -> None:
        self.fake_t3.thread["activities"] = [
            {"kind": "user-input.requested", "payload": {"requestId": "input-current"}}
        ]
        self.fixture.admission.admit_error = HostUnavailable("model admission adapter failed: hub-unavailable")
        error = self._run(self._answer())["error"]
        self.assertEqual(error["code"], "host_unavailable")
        self.assertIs(error["not_started"], True)

    def test_failure_at_or_after_dispatch_is_not_marked(self) -> None:
        self.fake_t3.thread["activities"] = [
            {"kind": "user-input.requested", "payload": {"requestId": "input-current"}}
        ]
        self.fake_t3.fail_after = 0
        error = self._run(self._answer())["error"]
        self.assertNotIn("not_started", error)

    def test_other_actions_are_never_marked(self) -> None:
        error = self._run({"action": "status", "thread_id": "missing-thread"})["error"]
        self.assertNotIn("not_started", error)

if __name__ == "__main__":
    unittest.main()
