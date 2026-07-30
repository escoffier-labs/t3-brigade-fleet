from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from t3_fleet.config import FleetConfig, HostConfig, RepositoryConfig
from t3_fleet.controller import FleetController
from t3_fleet.errors import HostUnavailable, IdempotencyConflict, UnknownState, ValidationError
from t3_fleet.store import FleetStore

FULL_SHA = "0123456789abcdef0123456789abcdef01234567"


def config() -> FleetConfig:
    host = HostConfig(
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
    return FleetConfig(
        controller_host="linux-worker",
        global_active_limit=3,
        state_dir=None,
        hosts={"linux-worker": host},
        repositories={
            "example-repo": RepositoryConfig(
                repository_id="example-repo",
                default_branch="main",
                branch_prefix="codex/",
                paths={"linux-worker": "/srv/repos/example"},
            )
        },
    )


def peer_config(controller_id: str = "controller-a") -> FleetConfig:
    base = config()
    return FleetConfig(
        controller_host=base.controller_host,
        global_active_limit=sum(
            host.active_limit for host in base.hosts.values() if host.enabled
        ),
        state_dir=base.state_dir,
        hosts=base.hosts,
        repositories=base.repositories,
        controller_id=controller_id,
        coordination_mode="target-authoritative",
    )


def two_host_peer_config(controller_id: str = "controller-a") -> FleetConfig:
    base = peer_config(controller_id)
    first = base.hosts["linux-worker"]
    second = HostConfig(
        **{
            **first.__dict__,
            "host_id": "second-worker",
            "ssh_alias": "second-worker",
            "expected_hostname": "second-worker",
        }
    )
    repository = base.repositories["example-repo"]
    return FleetConfig(
        controller_host=base.controller_host,
        global_active_limit=first.active_limit + second.active_limit,
        state_dir=base.state_dir,
        hosts={first.host_id: first, second.host_id: second},
        repositories={
            "example-repo": RepositoryConfig(
                repository_id=repository.repository_id,
                default_branch=repository.default_branch,
                branch_prefix=repository.branch_prefix,
                paths={
                    first.host_id: repository.paths[first.host_id],
                    second.host_id: "/srv/repos/example",
                },
            )
        },
        controller_id=controller_id,
        coordination_mode="target-authoritative",
    )


class FakeTransport:
    def __init__(self) -> None:
        self.calls = []
        self.fail_on = None
        self.status_state = "running"
        self.status_states = []
        self.submit_response = {
            "state": "running",
            "thread_id": "thread-1",
            "sequence": 8,
        }
        self.receipts: dict[str, dict[str, object]] = {}

    def call(self, host, request):
        self.calls.append(request)
        if request["action"] == self.fail_on:
            raise HostUnavailable("simulated transport failure")
        if request["action"] == "claim":
            request_id = request["request_id"]
            receipt = self.receipts.setdefault(
                request_id,
                {
                    "schema": "t3-fleet.agent-result.v1",
                    "request_id": request_id,
                    "request_digest": request["digest"],
                    "target_id": host.host_id,
                    "repository": request["repository"],
                    "source_revision": request["source_revision"],
                    "state": "running",
                    "thread_id": "thread-1",
                    "prompt_sha256": hashlib.sha256(
                        request["prompt"].encode("utf-8")
                    ).hexdigest(),
                    "prompt_bytes": len(request["prompt"].encode("utf-8")),
                    "created_at": "2026-07-28T01:00:00Z",
                    "updated_at": "2026-07-28T01:00:00Z",
                    "legacy": False,
                },
            )
            return dict(receipt)
        if request["action"] == "lookup":
            receipt = self.receipts.get(request["request_id"])
            if receipt is None:
                return {"schema": "t3-fleet.agent-result.v1", "state": "not_found"}
            return dict(receipt)
        if request["action"] == "reconcile":
            receipt = self.receipts.get(request["request_id"])
            if receipt is None:
                return {"schema": "t3-fleet.agent-result.v1", "state": "not_found"}
            return dict(receipt)
        if request["action"] == "reserve_followup":
            receipt = self.receipts[request["request_id"]]
            updated = dict(receipt)
            updated["state"] = "dispatching"
            updated["updated_at"] = "2026-07-28T02:00:00Z"
            self.receipts[request["request_id"]] = updated
            return dict(updated)
        if request["action"] == "doctor":
            return {"state": "eligible"}
        if request["action"] == "submit":
            return dict(self.submit_response)
        if request["action"] in {"status", "recover"}:
            state = (
                self.status_states.pop(0)
                if self.status_states
                else self.status_state
            )
            return {
                "state": state,
                "thread_id": "thread-1",
                "needs_attention": state == "needs_attention",
            }
        if request["action"] == "interrupt":
            return {"state": "running", "thread_id": "thread-1"}
        return {"state": "running", "thread_id": "thread-1"}


class DuplicateReceiptTransport(FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.mutating_actions: list[str] = []

    def call(self, host, request):
        if request["action"] == "lookup":
            self.calls.append(request)
            return {
                "schema": "t3-fleet.agent-result.v1",
                "request_id": request["request_id"],
                "request_digest": "legacy:" + "a" * 64,
                "target_id": host.host_id,
                "repository": "example-repo",
                "source_revision": None,
                "state": "running",
                "thread_id": "thread-1",
                "prompt_sha256": "a" * 64,
                "prompt_bytes": 0,
                "created_at": "2026-07-28T01:00:00Z",
                "updated_at": "2026-07-28T01:00:00Z",
                "legacy": True,
            }
        if request["action"] in {
            "claim", "reconcile", "reserve_followup", "followup",
            "interrupt", "approve", "answer", "recover", "release",
        }:
            self.mutating_actions.append(request["action"])
        return super().call(host, request)


class SelectiveLookupTransport(DuplicateReceiptTransport):
    def __init__(self, *, fail_host: str) -> None:
        super().__init__()
        self.fail_host = fail_host

    def call(self, host, request):
        if request["action"] == "lookup" and host.host_id == self.fail_host:
            self.calls.append(request)
            raise HostUnavailable(f"simulated transport failure on {host.host_id}")
        return super().call(host, request)


_MISSING_PROMPT_BYTES = object()


class PartialReceiptTransport(FakeTransport):
    def __init__(self, *, prompt_bytes: object = _MISSING_PROMPT_BYTES) -> None:
        super().__init__()
        self.prompt_bytes = prompt_bytes

    def call(self, host, request):
        if request["action"] == "reconcile":
            receipt = self.receipts[request["request_id"]]
            partial = {
                key: receipt[key]
                for key in (
                    "request_id",
                    "request_digest",
                    "target_id",
                    "repository",
                    "state",
                    "created_at",
                    "updated_at",
                    "thread_id",
                )
            }
            if self.prompt_bytes is not _MISSING_PROMPT_BYTES:
                partial["prompt_bytes"] = self.prompt_bytes
            return partial
        return super().call(host, request)


class TrackingFleetStore(FleetStore):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.import_receipt_calls = 0

    def import_receipt(self, receipt):
        self.import_receipt_calls += 1
        return super().import_receipt(receipt)


class ControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = FleetStore(Path(self.directory.name) / "fleet.db")
        self.transport = FakeTransport()
        self.controller = FleetController(config(), self.store, self.transport)
        self.request = {
            "request_id": "req-12345678",
            "host": "linux-worker",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
        }

    def test_submit_journals_before_transport_and_returns_thread(self) -> None:
        result = self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(result["thread_id"], "thread-1")
        self.assertEqual(self.store.get("req-12345678").state, "running")
        self.assertEqual(self.transport.calls[0]["action"], "doctor")
        self.assertEqual(self.transport.calls[1]["action"], "submit")

    def test_submit_rejects_setup_script_before_journal_or_transport(self) -> None:
        request = dict(self.request, run_setup_script=True)

        with self.assertRaisesRegex(
            ValidationError,
            "run_setup_script is unavailable",
        ):
            self.controller.submit(request, prompt="private prompt")

        self.assertEqual(self.transport.calls, [])
        with self.assertRaisesRegex(ValidationError, "unknown request id"):
            self.store.get("req-12345678")

    def test_submit_disconnect_after_agent_start_is_unknown(self) -> None:
        self.transport.fail_on = "submit"
        with self.assertRaises(UnknownState):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.store.get("req-12345678").state, "unknown")

    def test_malformed_post_dispatch_response_is_unknown(self) -> None:
        self.transport.submit_response = {"state": "failed"}
        with self.assertRaises(UnknownState):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.store.get("req-12345678").state, "unknown")
        self.assertFalse(
            self.store.acquire_lease(
                "linux-worker",
                "example-repo",
                "another-request",
                ttl=60,
            )
        )

    def test_running_submit_requires_nonempty_thread_id(self) -> None:
        self.transport.submit_response = {"state": "running", "thread_id": ""}
        with self.assertRaises(UnknownState):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.store.get("req-12345678").state, "unknown")

    def test_preflight_failure_is_blocked_offline(self) -> None:
        self.transport.fail_on = "doctor"
        with self.assertRaises(HostUnavailable):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.store.get("req-12345678").state, "blocked_offline")

    def test_blocked_preflight_can_retry_same_request(self) -> None:
        self.transport.fail_on = "doctor"
        with self.assertRaises(HostUnavailable):
            self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = None
        result = self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.store.get("req-12345678").thread_id, "thread-1")

    def test_terminal_status_updates_store(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.transport.status_state = "succeeded"
        result = self.controller.status("req-12345678")
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(self.store.get("req-12345678").state, "succeeded")

    def test_host_capacity_blocks_second_active_request(self) -> None:
        limited = config()
        host = limited.hosts["linux-worker"]
        limited = FleetConfig(
            controller_host=limited.controller_host,
            global_active_limit=limited.global_active_limit,
            state_dir=limited.state_dir,
            hosts={
                "linux-worker": HostConfig(
                    **{**host.__dict__, "active_limit": 1}
                )
            },
            repositories=limited.repositories,
        )
        controller = FleetController(limited, self.store, self.transport)
        controller.submit(self.request, prompt="first prompt")
        second = dict(self.request, request_id="req-87654321")
        with self.assertRaisesRegex(HostUnavailable, "capacity"):
            controller.submit(second, prompt="second prompt")
        self.assertEqual(self.store.get("req-87654321").state, "queued")

    def test_wait_uses_bounded_backoff_until_attention(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.transport.status_states = ["running", "running", "needs_attention"]
        sleeps = []
        moments = iter([0.0, 0.0, 1.0, 1.0, 3.0])
        controller = FleetController(
            config(),
            self.store,
            self.transport,
            sleep=sleeps.append,
            monotonic=lambda: next(moments),
        )
        result = controller.wait("req-12345678", timeout=20)
        self.assertEqual(result["state"], "needs_attention")
        self.assertEqual(sleeps, [1, 2])

    def test_recover_reconciles_unknown_without_resubmit(self) -> None:
        self.transport.fail_on = "submit"
        with self.assertRaises(UnknownState):
            self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = None
        self.transport.status_state = "running"
        result = self.controller.recover("req-12345678")
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.transport.calls[-1]["action"], "recover")
        self.assertEqual(
            sum(call["action"] == "submit" for call in self.transport.calls),
            1,
        )

    def test_followup_approve_and_interrupt_use_recorded_thread(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        followed = self.controller.followup(
            "req-12345678",
            prompt="next private prompt",
        )
        self.assertEqual(followed["state"], "running")
        approved = self.controller.approve(
            "req-12345678",
            "approval-1",
            "accept",
        )
        self.assertEqual(approved["state"], "running")
        interrupted = self.controller.interrupt("req-12345678")
        self.assertEqual(interrupted["state"], "running")
        self.assertEqual(
            [call["action"] for call in self.transport.calls[-3:]],
            ["followup", "approve", "interrupt"],
        )

    def test_followup_reopens_a_completed_thread(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.transport.status_state = "succeeded"
        self.controller.status("req-12345678")
        result = self.controller.followup(
            "req-12345678",
            prompt="continue from the completed turn",
        )
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.store.get("req-12345678").state, "running")

    def test_followup_rechecks_capacity_before_reopening(self) -> None:
        base = config()
        repositories = dict(base.repositories)
        repositories["second-repo"] = RepositoryConfig(
            repository_id="second-repo",
            default_branch="main",
            branch_prefix="codex/",
            paths={"linux-worker": "/srv/repos/second"},
        )
        limited = FleetConfig(
            controller_host=base.controller_host,
            global_active_limit=1,
            state_dir=base.state_dir,
            hosts=base.hosts,
            repositories=repositories,
        )
        controller = FleetController(limited, self.store, self.transport)
        controller.submit(self.request, prompt="private prompt")
        self.transport.status_state = "succeeded"
        controller.status("req-12345678")
        self.store.create_request(
            "req-22222222",
            "second-digest",
            "linux-worker",
            "second-repo",
        )
        self.store.transition("req-22222222", "preflight")
        self.store.transition("req-22222222", "dispatching")
        self.store.transition(
            "req-22222222",
            "running",
            thread_id="thread-2",
        )
        with self.assertRaisesRegex(HostUnavailable, "capacity"):
            controller.followup(
                "req-12345678",
                prompt="continue from the completed turn",
            )
        self.assertEqual(self.store.get("req-12345678").state, "succeeded")

    def test_interrupt_acceptance_stays_running_until_status_confirms(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        result = self.controller.interrupt("req-12345678")
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.store.get("req-12345678").state, "running")
        self.transport.status_state = "interrupted"
        self.assertEqual(self.controller.status("req-12345678")["state"], "interrupted")

    def test_interrupt_then_terminal_status_releases_active_request_lease(self) -> None:
        # Live canary defect: a canary submitted with approval-required reached
        # needs_attention with a pending approval. t3-fleet interrupt returned
        # accepted/running, but subsequent status stayed needs_attention because
        # T3 leaves a stale pending marker after thread.turn.interrupt +
        # thread.session.stop. The terminal interrupted status must release the
        # stored active request so host capacity drops to 0.
        self.controller.submit(self.request, prompt="private prompt")
        interrupted = self.controller.interrupt("req-12345678")
        self.assertEqual(interrupted["state"], "running")
        self.assertEqual(self.store.get("req-12345678").state, "running")
        # The target now reports the terminal interrupted/stopped session state
        # (with a stale pending approval marker that the agent fix ignores).
        self.transport.status_state = "interrupted"
        result = self.controller.status("req-12345678")
        self.assertEqual(result["state"], "interrupted")
        self.assertEqual(self.store.get("req-12345678").state, "interrupted")
        self.assertFalse(result["needs_attention"])
        self.assertEqual(
            self.store.active_counts(
                host="linux-worker", exclude_request_id="req-12345678"
            )[1],
            0,
        )
        # A second submit against the same host/repo acquires a fresh lease.
        second = dict(self.request, request_id="req-87654321")
        second_result = self.controller.submit(second, prompt="second prompt")
        self.assertEqual(second_result["state"], "running")

    def test_mutating_disconnect_becomes_unknown(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = "approve"
        with self.assertRaises(UnknownState):
            self.controller.approve("req-12345678", "approval-1", "accept")
        self.assertEqual(self.store.get("req-12345678").state, "unknown")

    def test_recover_disconnect_counts_toward_host_circuit(self) -> None:
        self.transport.fail_on = "submit"
        with self.assertRaises(UnknownState):
            self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = "recover"
        for _ in range(2):
            with self.assertRaises(HostUnavailable):
                self.controller.recover("req-12345678")
        self.assertTrue(self.store.circuit_is_open("linux-worker"))

    def test_status_renews_active_repository_lease(self) -> None:
        now = [0.0]
        store = FleetStore(
            Path(self.directory.name) / "renewal.db",
            clock=lambda: now[0],
        )
        controller = FleetController(config(), store, self.transport)
        controller.submit(self.request, prompt="private prompt")
        now[0] = 86_399.0
        controller.status("req-12345678")
        now[0] = 86_401.0
        self.assertFalse(
            store.acquire_lease(
                "linux-worker",
                "example-repo",
                "another-request",
                ttl=60,
            )
        )

    def test_needs_attention_can_reconcile_directly_to_succeeded(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.transport.status_state = "needs_attention"
        self.controller.status("req-12345678")
        self.transport.status_state = "succeeded"
        result = self.controller.status("req-12345678")
        self.assertEqual(result["state"], "succeeded")

    def test_answer_resumes_current_user_input_request(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.transport.status_state = "needs_attention"
        self.controller.status("req-12345678")
        result = self.controller.answer(
            "req-12345678",
            "input-1",
            {"question-1": ["Option A"]},
        )
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.store.get("req-12345678").state, "running")
        self.assertEqual(self.transport.calls[-1]["action"], "answer")

    def test_answer_disconnect_becomes_unknown(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = "answer"
        with self.assertRaises(UnknownState):
            self.controller.answer(
                "req-12345678",
                "input-1",
                {"question-1": ["Option A"]},
            )
        self.assertEqual(self.store.get("req-12345678").state, "unknown")

    def test_peer_submit_generates_target_and_controller_qualified_id(self) -> None:
        controller = FleetController(peer_config(), self.store, self.transport, uuid_factory=lambda: "12345678-1234-1234-1234-123456789abc")
        request = dict(self.request)
        request.pop("request_id")
        result = controller.submit(request, prompt="private prompt")
        self.assertEqual(
            result["request_id"],
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc",
        )

    def test_peer_submit_rejects_explicit_request_for_other_target(self) -> None:
        request = dict(
            self.request,
            request_id="other-worker-controller-a-12345678-1234-1234-1234-123456789abc",
        )
        with self.assertRaisesRegex(ValidationError, "target prefix"):
            FleetController(peer_config(), self.store, self.transport).submit(request, prompt="private prompt")

    def test_second_controller_discovers_imports_and_controls_foreign_request(self) -> None:
        first = FleetController(peer_config("controller-a"), self.store, self.transport)
        second_store = FleetStore(Path(self.directory.name) / "controller-b.db")
        second = FleetController(peer_config("controller-b"), second_store, self.transport)

        request = dict(self.request)
        request["request_id"] = "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        first.submit(request, prompt="private prompt")

        self.assertEqual(second.status(request["request_id"])["thread_id"], "thread-1")
        self.assertEqual(second.followup(request["request_id"], prompt="continue")["state"], "running")
        self.assertEqual(second.interrupt(request["request_id"])["state"], "running")
        self.assertEqual(second.recover(request["request_id"])["state"], "running")
        self.assertEqual(second_store.get(request["request_id"]).host, "linux-worker")

    def test_recover_by_target_resolves_single_local_unknown(self) -> None:
        self.transport.fail_on = "submit"
        with self.assertRaises(UnknownState):
            self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = None
        self.transport.status_state = "running"
        result = self.controller.recover(None, target_hint="linux-worker")
        self.assertEqual(result["state"], "running")
        self.assertEqual(result["request_id"], "req-12345678")
        self.assertEqual(self.transport.calls[-1]["action"], "recover")
        self.assertEqual(self.transport.calls[-1]["request_id"], "req-12345678")

    def test_recover_by_target_fails_closed_when_no_unknown_matches(self) -> None:
        self.transport.fail_on = "submit"
        with self.assertRaises(UnknownState):
            self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = None
        # The locally journaled unknown is recovered, leaving zero unknowns.
        self.transport.status_state = "running"
        self.controller.recover(None, target_hint="linux-worker")
        with self.assertRaisesRegex(ValidationError, "unknown"):
            self.controller.recover(None, target_hint="linux-worker")

    def test_recover_by_target_fails_closed_when_multiple_unknown_matches(self) -> None:
        for request_id in ("req-11111111", "req-22222222"):
            self.store.create_request(
                request_id,
                f"digest-{request_id}",
                "linux-worker",
                "example-repo",
            )
            self.store.transition(request_id, "preflight")
            self.store.transition(request_id, "dispatching")
            self.store.mark_transport_failure(request_id)
        self.transport.fail_on = None
        with self.assertRaisesRegex(ValidationError, "unknown"):
            self.controller.recover(None, target_hint="linux-worker")

    def test_recover_target_hint_does_not_override_explicit_request_id(self) -> None:
        self.transport.fail_on = "submit"
        with self.assertRaises(UnknownState):
            self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = None
        self.store.create_request(
            "req-22222222",
            "digest-b",
            "linux-worker",
            "example-repo",
        )
        self.store.transition("req-22222222", "preflight")
        self.store.transition("req-22222222", "dispatching")
        self.store.mark_transport_failure("req-22222222")
        self.transport.status_state = "running"
        result = self.controller.recover(
            "req-12345678",
            target_hint="linux-worker",
        )
        self.assertEqual(result["request_id"], "req-12345678")
        self.assertEqual(self.transport.calls[-1]["request_id"], "req-12345678")

    def test_peer_recover_not_found_fresh_grace_window_keeps_unknown(self) -> None:
        now = [1_000.0]
        store = FleetStore(
            Path(self.directory.name) / "fresh_grace.db",
            clock=lambda: now[0],
        )
        controller = FleetController(
            peer_config(),
            store,
            self.transport,
            wall_clock=lambda: now[0],
        )
        self.transport.fail_on = "claim"
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        request = dict(self.request, request_id=request_id)
        with self.assertRaises(UnknownState):
            controller.submit(request, prompt="private prompt")
        self.transport.fail_on = None
        self.transport.calls.clear()
        now[0] += 29.999
        result = controller.recover(request_id)
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(store.get(request_id).state, "unknown")
        self.assertEqual(
            [call["action"] for call in self.transport.calls],
            ["reconcile"],
        )

    def test_peer_recover_not_found_expired_grace_window_transitions_to_failed(self) -> None:
        now = [1_000.0]
        store = FleetStore(
            Path(self.directory.name) / "expired_grace.db",
            clock=lambda: now[0],
        )
        controller = FleetController(
            peer_config(),
            store,
            self.transport,
            wall_clock=lambda: now[0],
        )
        self.transport.fail_on = "claim"
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        request = dict(self.request, request_id=request_id)
        with self.assertRaises(UnknownState):
            controller.submit(request, prompt="private prompt")
        self.transport.fail_on = None
        self.transport.calls.clear()
        now[0] += 30.0
        result = controller.recover(request_id)
        self.assertEqual(result["state"], "failed")
        self.assertEqual(store.get(request_id).state, "failed")
        self.assertEqual(
            [call["action"] for call in self.transport.calls],
            ["reconcile"],
        )

    def test_peer_recover_not_found_failed_retry_is_idempotent(self) -> None:
        now = [1_000.0]
        store = FleetStore(
            Path(self.directory.name) / "failed_retry.db",
            clock=lambda: now[0],
        )
        controller = FleetController(
            peer_config(),
            store,
            self.transport,
            wall_clock=lambda: now[0],
        )
        self.transport.fail_on = "claim"
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        request = dict(self.request, request_id=request_id)
        with self.assertRaises(UnknownState):
            controller.submit(request, prompt="private prompt")
        self.transport.fail_on = None
        now[0] += 30.0
        self.assertEqual(controller.recover(request_id)["state"], "failed")
        self.transport.calls.clear()
        self.assertEqual(controller.recover(request_id)["state"], "failed")
        self.assertEqual(
            [call["action"] for call in self.transport.calls],
            ["reconcile"],
        )

    def test_peer_recover_not_found_uses_when_request_became_unknown(self) -> None:
        now = [0.0]
        store = FleetStore(
            Path(self.directory.name) / "unknown_since.db",
            clock=lambda: now[0],
        )
        controller = FleetController(
            peer_config(),
            store,
            self.transport,
            wall_clock=lambda: now[0],
        )
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        store.create_request(
            request_id,
            "digest",
            "linux-worker",
            "example-repo",
        )
        now[0] = 1_000.0
        store.transition(request_id, "preflight")
        store.transition(request_id, "dispatching")
        store.mark_transport_failure(request_id)
        now[0] += 29.999

        result = controller.recover(request_id)

        self.assertEqual(result["state"], "unknown")
        self.assertEqual(store.get(request_id).created_at, 0.0)
        self.assertEqual(store.get(request_id).updated_at, 1_000.0)
        self.assertEqual(
            [call["action"] for call in self.transport.calls],
            ["reconcile"],
        )

    def test_peer_recover_not_found_target_hint_recovery(self) -> None:
        now = [1_000.0]
        store = FleetStore(
            Path(self.directory.name) / "target_hint_grace.db",
            clock=lambda: now[0],
        )
        controller = FleetController(
            peer_config(),
            store,
            self.transport,
            wall_clock=lambda: now[0],
        )
        self.transport.fail_on = "claim"
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        request = dict(self.request, request_id=request_id)
        with self.assertRaises(UnknownState):
            controller.submit(request, prompt="private prompt")
        self.transport.fail_on = None
        self.transport.calls.clear()
        now[0] += 30.0
        result = controller.recover(None, target_hint="linux-worker")
        self.assertEqual(result["request_id"], request_id)
        self.assertEqual(result["state"], "failed")
        self.assertEqual(
            [call["action"] for call in self.transport.calls],
            ["reconcile"],
        )

    def test_recover_without_id_or_target_is_invalid_request(self) -> None:
        with self.assertRaisesRegex(ValidationError, "request_id"):
            self.controller.recover(None)

    def test_single_controller_unknown_request_does_not_lookup_targets(self) -> None:
        with self.assertRaisesRegex(ValidationError, "unknown request id"):
            self.controller.status("req-87654321")
        lookup_calls = [
            call for call in self.transport.calls if call.get("action") == "lookup"
        ]
        self.assertEqual(lookup_calls, [])

    def test_multiple_target_matches_stop_before_control_mutation(self) -> None:
        controller = FleetController(
            two_host_peer_config(),
            self.store,
            DuplicateReceiptTransport(),
        )
        with self.assertRaises(IdempotencyConflict):
            controller.interrupt("req-12345678")
        self.assertEqual(controller.transport.mutating_actions, [])

    def test_peer_qualified_id_shortcuts_to_matching_target(self) -> None:
        transport = FakeTransport()
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        transport.receipts[request_id] = {
            "schema": "t3-fleet.agent-result.v1",
            "request_id": request_id,
            "request_digest": "a" * 64,
            "target_id": "linux-worker",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "state": "running",
            "thread_id": "thread-1",
            "prompt_sha256": "b" * 64,
            "prompt_bytes": 12,
            "created_at": "2026-07-28T01:00:00Z",
            "updated_at": "2026-07-28T01:00:00Z",
            "legacy": False,
        }
        controller = FleetController(two_host_peer_config(), self.store, transport)
        controller.status(request_id)
        lookup_calls = [
            call for call in transport.calls if call.get("action") == "lookup"
        ]
        self.assertEqual(len(lookup_calls), 1)

    def test_overlapping_legacy_id_scans_all_enabled_targets(self) -> None:
        transport = DuplicateReceiptTransport()
        controller = FleetController(two_host_peer_config(), self.store, transport)
        legacy_id = "linux-worker-overlapping-legacy-id"
        with self.assertRaises(IdempotencyConflict):
            controller.status(legacy_id)
        lookup_calls = [
            call for call in transport.calls if call.get("action") == "lookup"
        ]
        self.assertEqual(len(lookup_calls), 2)
        self.assertEqual(
            {call["request_id"] for call in lookup_calls},
            {legacy_id},
        )

    def test_legacy_lookup_fails_closed_on_transport_loss(self) -> None:
        transport = SelectiveLookupTransport(fail_host="second-worker")
        controller = FleetController(two_host_peer_config(), self.store, transport)
        with self.assertRaises(HostUnavailable):
            controller.status("req-12345678")
        lookup_calls = [
            call for call in transport.calls if call.get("action") == "lookup"
        ]
        self.assertEqual(len(lookup_calls), 2)

    def test_peer_terminal_followup_reserve_transport_loss_is_unknown_recoverable(
        self,
    ) -> None:
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        request = dict(self.request, request_id=request_id)
        first = FleetController(peer_config("controller-a"), self.store, self.transport)
        first.submit(request, prompt="private prompt")
        self.transport.receipts[request_id]["state"] = "succeeded"
        first.status(request_id)

        second_store = FleetStore(Path(self.directory.name) / "controller-b.db")
        second = FleetController(peer_config("controller-b"), second_store, self.transport)
        second.status(request_id)
        self.transport.fail_on = "reserve_followup"
        with self.assertRaises(UnknownState):
            second.followup(request_id, prompt="continue from completed turn")
        self.assertEqual(second_store.get(request_id).state, "unknown")
        reserve_calls = [
            call
            for call in self.transport.calls
            if call.get("action") == "reserve_followup"
        ]
        self.assertEqual(len(reserve_calls), 1)
        followup_calls = [
            call for call in self.transport.calls if call.get("action") == "followup"
        ]
        self.assertEqual(len(followup_calls), 0)

        self.transport.fail_on = None
        self.transport.receipts[request_id]["state"] = "running"
        recovered = second.recover(request_id)
        self.assertEqual(recovered["state"], "running")
        self.assertEqual(
            sum(
                1
                for call in self.transport.calls
                if call.get("action") == "reserve_followup"
            ),
            1,
        )

    def test_peer_terminal_followup_failure_after_reserve_is_unknown_recoverable(
        self,
    ) -> None:
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        request = dict(self.request, request_id=request_id)
        first = FleetController(peer_config("controller-a"), self.store, self.transport)
        first.submit(request, prompt="private prompt")
        self.transport.receipts[request_id]["state"] = "succeeded"
        first.status(request_id)

        second_store = FleetStore(Path(self.directory.name) / "controller-b.db")
        second = FleetController(peer_config("controller-b"), second_store, self.transport)
        second.status(request_id)
        self.transport.fail_on = "followup"
        with self.assertRaises(UnknownState):
            second.followup(request_id, prompt="continue from completed turn")
        self.assertEqual(second_store.get(request_id).state, "unknown")
        reserve_calls = [
            call
            for call in self.transport.calls
            if call.get("action") == "reserve_followup"
        ]
        self.assertEqual(len(reserve_calls), 1)

        self.transport.fail_on = None
        self.transport.receipts[request_id]["state"] = "running"
        recovered = second.recover(request_id)
        self.assertEqual(recovered["state"], "running")
        self.assertEqual(
            sum(
                1
                for call in self.transport.calls
                if call.get("action") == "reserve_followup"
            ),
            1,
        )

    def test_peer_status_imports_complete_receipt_via_reconcile(self) -> None:
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        request = dict(self.request, request_id=request_id)
        store = TrackingFleetStore(Path(self.directory.name) / "reconcile-import.db")
        transport = FakeTransport()
        controller = FleetController(peer_config(), store, transport)
        controller.submit(request, prompt="private prompt")
        import_calls_after_submit = store.import_receipt_calls
        transport.calls.clear()
        transport.receipts[request_id]["state"] = "succeeded"
        result = controller.status(request_id)
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(store.get(request_id).state, "succeeded")
        self.assertEqual(store.import_receipt_calls, import_calls_after_submit + 1)
        self.assertEqual(
            [call["action"] for call in transport.calls],
            ["reconcile"],
        )

    def test_refresh_from_target_rejects_partial_receipt_without_erasing_thread_id(
        self,
    ) -> None:
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        request = dict(self.request, request_id=request_id)
        for case, prompt_bytes in enumerate(
            (_MISSING_PROMPT_BYTES, True, -1),
            start=1,
        ):
            with self.subTest(case=case, prompt_bytes=prompt_bytes):
                store = TrackingFleetStore(
                    Path(self.directory.name) / f"partial-{case}.db"
                )
                transport = PartialReceiptTransport(prompt_bytes=prompt_bytes)
                controller = FleetController(peer_config(), store, transport)
                controller.submit(request, prompt="private prompt")
                import_calls_after_submit = store.import_receipt_calls
                result = controller.status(request_id)
                self.assertEqual(result["thread_id"], "thread-1")
                self.assertEqual(result["state"], "running")
                record = store.get(request_id)
                self.assertEqual(record.thread_id, "thread-1")
                self.assertEqual(store.import_receipt_calls, import_calls_after_submit)


if __name__ == "__main__":
    unittest.main()
