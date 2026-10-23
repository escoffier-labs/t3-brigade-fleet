from __future__ import annotations

import hashlib
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from t3_fleet.config import FleetConfig, HostConfig, PolicyConfig, RepositoryConfig
from t3_fleet.controller import FleetController
from t3_fleet.errors import (
    HostUnavailable,
    IdempotencyConflict,
    PolicyRejected,
    PolicyRevisionConflict,
    TargetBusy,
    UnknownState,
    ValidationError,
)
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
                policy_identity=CANONICAL_REPO,
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
        self.fail_exc = None
        self.fail_detail = None
        self.status_state = "running"
        self.status_states = []
        self.reservation: dict[str, object] | None = None
        self.submit_response = {
            "state": "running",
            "thread_id": "thread-1",
            "sequence": 8,
        }
        self.receipts: dict[str, dict[str, object]] = {}

    def call(self, host, request):
        self.calls.append(request)
        if request["action"] == self.fail_on:
            if self.fail_exc is not None:
                raise self.fail_exc
            raise HostUnavailable(
                "simulated transport failure",
                detail=self.fail_detail,
            )
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
                    # A target that was handed a plan reports back the admission
                    # it actually took at launch, never the plan.
                    **(
                        {
                            "model_plan": request["model_plan"],
                            "model_admission": admission_for_plan(
                                request["model_plan"]
                            ),
                        }
                        if "model_plan" in request
                        else {"model_admission": request["model_admission"]}
                    ),
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
            response = {
                "state": state,
                "thread_id": "thread-1",
                "needs_attention": state == "needs_attention",
            }
            if self.reservation is not None:
                response["fleet_reservation"] = dict(self.reservation)
            response.update(getattr(self, "status_extra", {}))
            return response
        if request["action"] == "interrupt":
            return {"state": "running", "thread_id": "thread-1"}
        return {"state": "running", "thread_id": "thread-1"}


# Canonical model identity versus the native model the provider is launched on.
# The roster's explicit binding is the only thing that makes them differ.
CANONICAL_MODEL = "gpt-5.6-terra"
NATIVE_MODEL = "gpt-5.6-terra-2026-08-30"

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


def admission_for_plan(plan: dict[str, object]) -> dict[str, object]:
    """The admission a target reports after launching the seat it was planned.

    The canonical identity is the plan's; the native launch model is not part of
    the admission envelope at all, which is why the controller can only ever
    compare canonical identity here.
    """
    admission = {
        key: plan[key]
        for key in (
            "roster_revision",
            "roster_digest",
            "seat",
            "provider",
            "model",
            "reasoning",
            "provider_instance_id",
        )
    }
    if "service_tier" in plan:
        admission["service_tier"] = plan["service_tier"]
    admission["source"] = "hub"
    admission["expires_at"] = plan["expires_at"]
    return admission


class FakeAdmission:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.plan_calls: list[dict[str, object]] = []
        self.plan_error: Exception | None = None

    def admit(self, request: dict[str, object]) -> dict[str, object]:
        self.calls.append(dict(request))
        return {
            "roster_revision": 42,
            "roster_digest": "sha256:" + "a" * 64,
            "seat": "coder",
            "provider": "openai",
            "model": CANONICAL_MODEL,
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
        # The Hub plans for the seat it was asked about, which under enrollment
        # is the seat the route decision selected.
        return {**PLAN, "seat": request["seat"]}


class ChangingExpiryAdmission(FakeAdmission):
    def admit(self, request: dict[str, object]) -> dict[str, object]:
        result = super().admit(request)
        result["expires_at"] = (
            "2026-08-30T14:00:00Z"
            if len(self.calls) == 1
            else "2026-08-30T14:01:00Z"
        )
        return result


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
        self.admission = FakeAdmission()
        self.controller = FleetController(
            config(), self.store, self.transport, admission=self.admission
        )
        self.request = {
            "request_id": "req-12345678",
            "host": "linux-worker",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "seat": "coder",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
        }

    def test_submit_journals_before_transport_and_returns_thread(self) -> None:
        result = self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(result["thread_id"], "thread-1")
        self.assertEqual(self.store.get("req-12345678").state, "running")
        self.assertEqual(self.transport.calls[0]["action"], "doctor")
        self.assertEqual(self.transport.calls[1]["action"], "submit")

    def test_submit_admits_logical_seat_before_journal_and_forwards_exact_selection(self) -> None:
        admission = FakeAdmission()
        controller = FleetController(
            config(), self.store, self.transport, admission=admission
        )
        request = dict(self.request)
        request["seat"] = "coder"

        result = controller.submit(request, prompt="private prompt")

        self.assertEqual(admission.calls, [{
            "consumer": "t3-fleet",
            "phase": "controller",
            "request_id": "req-12345678",
            "seat": "coder",
        }])
        record = self.store.get("req-12345678")
        self.assertEqual(record.admission["roster_revision"], 42)
        self.assertEqual(result["admission"]["model"], "gpt-5.6-terra")
        submitted = self.transport.calls[-1]
        self.assertEqual(submitted["model_admission"], result["admission"])
        self.assertNotIn("model_selection", submitted)

    def test_submit_idempotency_digest_excludes_admission_expiry(self) -> None:
        admission = ChangingExpiryAdmission()
        controller = FleetController(
            config(), self.store, self.transport, admission=admission
        )

        first = controller.submit(self.request, prompt="private prompt")
        second = controller.submit(self.request, prompt="private prompt")

        self.assertEqual(first["request_id"], second["request_id"])
        self.assertEqual(first["state"], second["state"])
        # A moving admission expiry is not a payload change, so the replay
        # returns the durable receipt instead of re-admitting the seat.
        self.assertEqual(len(admission.calls), 1)
        self.assertEqual(
            len([call for call in self.transport.calls if call["action"] == "submit"]),
            1,
        )

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

    def test_submit_unknown_preserves_safe_agent_error_detail(self) -> None:
        self.transport.fail_on = "submit"
        self.transport.fail_detail = "target capacity is exhausted at 1 active requests"

        with self.assertRaises(UnknownState) as raised:
            self.controller.submit(self.request, prompt="private prompt")

        self.assertEqual(
            raised.exception.detail,
            "target capacity is exhausted at 1 active requests",
        )

    def test_submit_target_admission_policy_error_terminalizes_and_releases_lease(self) -> None:
        for error in (
            PolicyRejected("Fleet Hub model admission was denied"),
            PolicyRevisionConflict("Fleet Hub roster revision changed"),
        ):
            with self.subTest(error=type(error).__name__):
                store = FleetStore(
                    Path(self.directory.name) / f"ctrl-{type(error).__name__}.db"
                )
                transport = FakeTransport()
                transport.fail_on = "submit"
                transport.fail_exc = error
                controller = FleetController(
                    config(), store, transport, admission=self.admission
                )
                with self.assertRaises(type(error)) as raised:
                    controller.submit(self.request, prompt="private prompt")
                self.assertIs(raised.exception, error)
                record = store.get("req-12345678")
                self.assertEqual(record.state, "failed")
                self.assertIsNone(record.thread_id)
                self.assertTrue(
                    store.acquire_lease(
                        "linux-worker",
                        "example-repo",
                        "another-request",
                        ttl=60,
                    )
                )

    def test_peer_submit_target_admission_policy_error_terminalizes_and_releases_lease(
        self,
    ) -> None:
        request_id = (
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        request = dict(self.request, request_id=request_id)
        for error in (
            PolicyRejected("Fleet Hub model admission was denied"),
            PolicyRevisionConflict("Fleet Hub roster revision changed"),
        ):
            with self.subTest(error=type(error).__name__):
                store = FleetStore(
                    Path(self.directory.name) / f"peer-{type(error).__name__}.db"
                )
                transport = FakeTransport()
                transport.fail_on = "claim"
                transport.fail_exc = error
                controller = FleetController(
                    peer_config(), store, transport, admission=self.admission
                )
                with self.assertRaises(type(error)) as raised:
                    controller.submit(request, prompt="private prompt")
                self.assertIs(raised.exception, error)
                record = store.get(request_id)
                self.assertEqual(record.state, "failed")
                self.assertIsNone(record.thread_id)
                self.assertTrue(
                    store.acquire_lease(
                        "linux-worker",
                        "example-repo",
                        "another-request",
                        ttl=60,
                    )
                )

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
        controller = FleetController(
            limited, self.store, self.transport, admission=self.admission
        )
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
            admission=self.admission,
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
        controller = FleetController(
            limited, self.store, self.transport, admission=self.admission
        )
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
        controller = FleetController(
            config(), store, self.transport, admission=self.admission
        )
        controller.submit(self.request, prompt="private prompt")
        ttl = config().hosts["linux-worker"].repository_lease_ttl_seconds
        now[0] = ttl - 1.0
        controller.status("req-12345678")
        now[0] = ttl + 1.0
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
        controller = FleetController(peer_config(), self.store, self.transport, uuid_factory=lambda: "12345678-1234-1234-1234-123456789abc", admission=self.admission)
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
            FleetController(
                peer_config(), self.store, self.transport, admission=self.admission
            ).submit(request, prompt="private prompt")

    def test_second_controller_discovers_imports_and_controls_foreign_request(self) -> None:
        first = FleetController(
            peer_config("controller-a"), self.store, self.transport,
            admission=self.admission,
        )
        second_store = FleetStore(Path(self.directory.name) / "controller-b.db")
        second = FleetController(
            peer_config("controller-b"), second_store, self.transport,
            admission=self.admission,
        )

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
            admission=self.admission,
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
            admission=self.admission,
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
            admission=self.admission,
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
            admission=self.admission,
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
            admission=self.admission,
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
            admission=self.admission,
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
        controller = FleetController(
            two_host_peer_config(), self.store, transport, admission=self.admission
        )
        controller.status(request_id)
        lookup_calls = [
            call for call in transport.calls if call.get("action") == "lookup"
        ]
        self.assertEqual(len(lookup_calls), 1)

    def test_overlapping_legacy_id_scans_all_enabled_targets(self) -> None:
        transport = DuplicateReceiptTransport()
        controller = FleetController(
            two_host_peer_config(), self.store, transport, admission=self.admission
        )
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
        controller = FleetController(
            two_host_peer_config(), self.store, transport, admission=self.admission
        )
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
        first = FleetController(
            peer_config("controller-a"), self.store, self.transport,
            admission=self.admission,
        )
        first.submit(request, prompt="private prompt")
        self.transport.receipts[request_id]["state"] = "succeeded"
        first.status(request_id)

        second_store = FleetStore(Path(self.directory.name) / "controller-b.db")
        second = FleetController(
            peer_config("controller-b"), second_store, self.transport,
            admission=self.admission,
        )
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
        first = FleetController(
            peer_config("controller-a"), self.store, self.transport,
            admission=self.admission,
        )
        first.submit(request, prompt="private prompt")
        self.transport.receipts[request_id]["state"] = "succeeded"
        first.status(request_id)

        second_store = FleetStore(Path(self.directory.name) / "controller-b.db")
        second = FleetController(
            peer_config("controller-b"), second_store, self.transport,
            admission=self.admission,
        )
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
        controller = FleetController(
            peer_config(), store, transport, admission=self.admission
        )
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
                controller = FleetController(
                    peer_config(), store, transport, admission=self.admission
                )
                controller.submit(request, prompt="private prompt")
                import_calls_after_submit = store.import_receipt_calls
                result = controller.status(request_id)
                self.assertEqual(result["thread_id"], "thread-1")
                self.assertEqual(result["state"], "running")
                record = store.get(request_id)
                self.assertEqual(record.thread_id, "thread-1")
                self.assertEqual(store.import_receipt_calls, import_calls_after_submit)



POLICY_DIGEST = "sha256:" + "b" * 64
CANONICAL_REPO = "github.com/example-org/example-repo"
POLICY_WALL_CLOCK = datetime(2026, 9, 5, 12, 0, tzinfo=UTC).timestamp()


def two_host_config() -> FleetConfig:
    base = config()
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
        controller_host="linux-worker",
        global_active_limit=base.global_active_limit,
        state_dir=None,
        hosts={first.host_id: first, second.host_id: second},
        repositories={
            "example-repo": RepositoryConfig(
                repository_id=repository.repository_id,
                default_branch=repository.default_branch,
                branch_prefix=repository.branch_prefix,
                policy_identity=CANONICAL_REPO,
                paths={
                    first.host_id: repository.paths[first.host_id],
                    second.host_id: "/srv/repos/example",
                },
            )
        },
    )


def enrolled(base: FleetConfig | None = None) -> FleetConfig:
    return replace(
        base if base is not None else two_host_config(),
        policy=PolicyConfig(enrollment="required", workload="linux-development"),
    )


class HostRecordingTransport(FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.hosts: list[str] = []

    def call(self, host, request):
        self.hosts.append(host.host_id)
        return super().call(host, request)


class FakePolicy:
    def __init__(self, *, machine: str = "second-worker", seat: str = "muse13") -> None:
        self.machine = machine
        self.seat = seat
        self.version = 7
        self.digest = POLICY_DIGEST
        self.decision_policy_version: int | None = None
        self.decision_policy_digest: str | None = None
        self.expires_at = "2026-09-05T12:10:00Z"
        self.resolve_error: Exception | None = None
        self.route_error: Exception | None = None
        self.serial = 0
        self.renewed_expires_at = "2026-09-05T12:20:00Z"
        self.renew_error: Exception | None = None
        self.release_error: Exception | None = None
        self.resolve_calls: list[dict[str, object]] = []
        self.route_calls: list[dict[str, object]] = []
        self.ack_calls: list[dict[str, object]] = []
        self.renew_calls: list[dict[str, object]] = []
        self.release_calls: list[dict[str, object]] = []
        self.delegation_calls: list[dict[str, object]] = []
        self.delegation_receipt: dict[str, object] | None = None
        self.delegation_error: Exception | None = None

    def resolve(self, **kwargs: object) -> dict[str, object]:
        self.resolve_calls.append(dict(kwargs))
        if self.resolve_error is not None:
            raise self.resolve_error
        return {
            "version": self.version,
            "digest": self.digest,
            "roles": {"impl": self.seat},
            "execution": {"machine": self.machine},
            "data": {"allow_training": False},
            "sources": {},
            "ack_required": True,
        }

    def route(self, **kwargs: object) -> dict[str, object]:
        self.route_calls.append(dict(kwargs))
        if self.route_error is not None:
            raise self.route_error
        self.serial += 1
        return {
            "decision_id": f"decision-{self.serial}",
            "policy_version": (
                self.version
                if self.decision_policy_version is None
                else self.decision_policy_version
            ),
            "policy_digest": self.decision_policy_digest or self.digest,
            "machine": self.machine,
            "seat": self.seat,
            "reason": "workload match",
            "candidates": [],
            "reservation_id": f"reservation-{self.serial}",
            "expires_at": self.expires_at,
        }

    def acknowledge(self, **kwargs: object) -> dict[str, object]:
        self.ack_calls.append(dict(kwargs))
        return {"acknowledged": True}

    def delegation(self, **kwargs: object) -> dict[str, object]:
        self.delegation_calls.append(dict(kwargs))
        if self.delegation_error is not None:
            raise self.delegation_error
        if self.delegation_receipt is None:
            raise AssertionError("this controller holds no delegation receipt")
        return dict(self.delegation_receipt)

    def renew_reservation(self, **kwargs: object) -> dict[str, object]:
        raise AssertionError(
            "only the authenticated target may renew a fleet reservation"
        )

    def release_reservation(self, **kwargs: object) -> dict[str, object]:
        raise AssertionError(
            "only the authenticated target may release a fleet reservation"
        )


class _FleetPolicyFixture:
    """Shared enrolled-controller fixture for the policy and reservation suites."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = FleetStore(Path(self.directory.name) / "fleet.db")
        self.transport = HostRecordingTransport()
        self.admission = FakeAdmission()
        self.policy = FakePolicy()
        self.clock = [POLICY_WALL_CLOCK]
        self.controller = self.build()
        self.request = {
            "request_id": "req-12345678",
            "host": "auto",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
        }

    def build(
        self,
        *,
        fleet_config: FleetConfig | None = None,
        store: FleetStore | None = None,
    ) -> FleetController:
        return FleetController(
            fleet_config if fleet_config is not None else enrolled(),
            store if store is not None else self.store,
            self.transport,
            admission=self.admission,
            policy=self.policy,
            wall_clock=lambda: self.clock[0],
        )

    def stamp(self) -> dict[str, object]:
        submits = [
            call
            for call in self.transport.calls
            if call["action"] in {"submit", "claim"}
        ]
        self.assertEqual(len(submits), 1)
        return submits[0]["fleet_policy"]


class FleetPolicyControllerTests(_FleetPolicyFixture, unittest.TestCase):
    def test_host_auto_routes_through_the_hub_to_a_worker_elsewhere(self) -> None:
        result = self.controller.submit(self.request, prompt="private prompt")

        self.assertEqual(result["host"], "second-worker")
        self.assertEqual(self.transport.hosts, ["second-worker", "second-worker"])
        self.assertEqual(len(self.policy.resolve_calls), 1)
        self.assertEqual(
            self.policy.resolve_calls[0]["consumer"],
            "t3-fleet",
        )
        self.assertEqual(self.policy.resolve_calls[0]["origin"], "linux-worker")
        self.assertEqual(self.policy.resolve_calls[0]["repository"], CANONICAL_REPO)
        self.assertEqual(len(self.policy.route_calls), 1)
        self.assertEqual(self.policy.route_calls[0]["workload"], "linux-development")
        self.assertIsNone(self.policy.route_calls[0]["machine"])
        self.assertIsNone(self.policy.route_calls[0]["seat"])
        self.assertIsNone(self.policy.route_calls[0]["override_reason"])
        self.assertEqual(self.policy.ack_calls, [])
        stamp = self.stamp()
        self.assertEqual(stamp["decision_id"], "decision-1")
        self.assertEqual(stamp["version"], 7)
        self.assertEqual(stamp["digest"], POLICY_DIGEST)
        self.assertEqual(stamp["machine"], "second-worker")
        self.assertEqual(stamp["seat"], "muse13")
        self.assertEqual(stamp["origin"], "linux-worker")
        self.assertEqual(stamp["reservation_id"], "reservation-1")
        self.assertEqual(self.store.get("req-12345678").host, "second-worker")

    def test_policy_session_is_durable_and_linked_to_the_local_request(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        session = self.store.policy_session_for_request("req-12345678")
        self.assertIsNotNone(session)
        self.assertEqual(session.request_id, "req-12345678")
        self.assertEqual(session.policy_version, 7)
        self.assertEqual(session.policy_digest, POLICY_DIGEST)
        self.assertEqual(session.decision["decision_id"], "decision-1")
        self.assertEqual(session.envelope["data"], {"allow_training": False})
        self.assertEqual(session.session_id, self.stamp()["session_id"])
        self.assertTrue(session.session_id.startswith("t3f-"))
        self.assertNotIn("req-12345678", session.session_id)

    def test_policy_selected_machine_must_pass_the_local_allowlist(self) -> None:
        self.policy.machine = "ghost-worker"
        with self.assertRaises(PolicyRejected):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.transport.calls, [])
        with self.assertRaises(ValidationError):
            self.store.get("req-12345678")

    def test_policy_selected_machine_must_be_enabled_locally(self) -> None:
        base = two_host_config()
        disabled = HostConfig(**{**base.hosts["second-worker"].__dict__, "enabled": False})
        controller = self.build(
            fleet_config=enrolled(
                replace(
                    base,
                    hosts={"linux-worker": base.hosts["linux-worker"], "second-worker": disabled},
                )
            )
        )
        with self.assertRaises(PolicyRejected):
            controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.transport.calls, [])

    def test_route_denial_fails_closed_before_journal_and_dispatch(self) -> None:
        self.policy.route_error = PolicyRejected("Fleet Hub denied the route")
        with self.assertRaises(PolicyRejected):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.admission.calls, [])
        with self.assertRaises(ValidationError):
            self.store.get("req-12345678")

    def test_missing_policy_helper_fails_closed_on_the_enrolled_path(self) -> None:
        self.policy.resolve_error = HostUnavailable("fleet policy adapter is missing")
        with self.assertRaises(HostUnavailable):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.policy.route_calls, [])
        with self.assertRaises(ValidationError):
            self.store.get("req-12345678")

    def test_explicit_host_requires_a_reason_while_enrolled(self) -> None:
        request = dict(self.request, host="linux-worker")
        with self.assertRaises(ValidationError):
            self.controller.submit(request, prompt="private prompt")
        self.assertEqual(self.policy.route_calls, [])
        self.assertEqual(self.transport.calls, [])

    def test_explicit_host_override_reason_reaches_the_same_central_route(self) -> None:
        self.policy.machine = "linux-worker"
        request = dict(
            self.request,
            host="linux-worker",
            override_reason="operator pinned the controller host",
        )
        self.controller.submit(request, prompt="private prompt")
        self.assertEqual(len(self.policy.route_calls), 1)
        self.assertEqual(self.policy.route_calls[0]["machine"], "linux-worker")
        self.assertEqual(
            self.policy.route_calls[0]["override_reason"],
            "operator pinned the controller host",
        )
        self.assertEqual(self.stamp()["machine"], "linux-worker")
        self.assertEqual(self.transport.hosts, ["linux-worker", "linux-worker"])

    def test_explicit_host_rerouted_by_policy_is_rejected(self) -> None:
        request = dict(
            self.request,
            host="linux-worker",
            override_reason="operator pinned the controller host",
        )
        with self.assertRaises(PolicyRejected):
            self.controller.submit(request, prompt="private prompt")
        self.assertEqual(self.transport.calls, [])

    def test_auto_selection_rejects_an_override_reason(self) -> None:
        request = dict(self.request, override_reason="no host to override")
        with self.assertRaises(ValidationError):
            self.controller.submit(request, prompt="private prompt")
        self.assertEqual(self.policy.route_calls, [])

    def test_effective_seat_is_applied_when_the_caller_omits_one(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.admission.plan_calls[0]["seat"], "muse13")
        submit = [
            call for call in self.transport.calls if call["action"] == "submit"
        ][0]
        self.assertEqual(submit["seat"], "muse13")
        # The controller hands the target a plan, never an admission.
        self.assertEqual(submit["model_plan"]["state"], "planned")
        self.assertNotIn("model_admission", submit)

    def test_a_caller_seat_without_a_reason_is_refused_before_routing(self) -> None:
        request = dict(self.request, seat="coder")
        with self.assertRaises(ValidationError):
            self.controller.submit(request, prompt="private prompt")
        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(self.policy.route_calls, [])
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.admission.calls, [])

    def test_a_caller_seat_is_a_negotiated_override_on_the_central_route(self) -> None:
        request = dict(
            self.request,
            seat="muse13",
            override_reason="operator pinned the review seat",
        )
        result = self.controller.submit(request, prompt="private prompt")
        self.assertEqual(result["host"], "second-worker")
        self.assertEqual(len(self.policy.route_calls), 1)
        call = self.policy.route_calls[0]
        self.assertEqual(call["seat"], "muse13")
        self.assertIsNone(call["machine"])
        self.assertEqual(call["override_reason"], "operator pinned the review seat")
        self.assertEqual(self.stamp()["seat"], "muse13")
        # An enrolled controller plans; it never admits.
        self.assertEqual(self.admission.calls, [])
        self.assertEqual(self.admission.plan_calls[0]["seat"], "muse13")

    def test_an_explicit_machine_and_seat_are_negotiated_together(self) -> None:
        self.policy.machine = "linux-worker"
        request = dict(
            self.request,
            host="linux-worker",
            seat="muse13",
            override_reason="operator pinned the host and seat",
        )
        self.controller.submit(request, prompt="private prompt")
        call = self.policy.route_calls[0]
        self.assertEqual(call["machine"], "linux-worker")
        self.assertEqual(call["seat"], "muse13")

    def test_caller_seat_that_contradicts_the_policy_seat_is_rejected(self) -> None:
        request = dict(
            self.request,
            seat="coder",
            override_reason="operator asked for the coder seat",
        )
        with self.assertRaises(PolicyRejected):
            self.controller.submit(request, prompt="private prompt")
        self.assertEqual(len(self.policy.route_calls), 1)
        self.assertEqual(self.policy.route_calls[0]["seat"], "coder")
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.admission.calls, [])

    def test_a_reusable_reservation_must_match_the_explicit_seat(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        session = self.store.policy_session_for_request("req-12345678")
        self.assertEqual(session.decision["seat"], "muse13")
        self.assertIsNotNone(
            self.controller._reusable_decision(session, "second-worker", "muse13")
        )
        self.assertIsNone(
            self.controller._reusable_decision(session, "second-worker", "coder")
        )
        self.assertIsNone(
            self.controller._reusable_decision(session, "linux-worker", "muse13")
        )

    def test_a_dispatched_request_replays_its_receipt_without_resolving(self) -> None:
        first = self.controller.submit(self.request, prompt="private prompt")
        self.policy.resolve_error = AssertionError(
            "a durable replay must not resolve fleet policy again"
        )
        self.policy.route_error = AssertionError(
            "a durable replay must not reserve a second machine"
        )
        second = self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(first["request_id"], second["request_id"])
        self.assertEqual(first["state"], second["state"])
        self.assertEqual(len(self.policy.resolve_calls), 1)
        self.assertEqual(len(self.policy.route_calls), 1)
        session = self.store.policy_session_for_request("req-12345678")
        self.assertEqual(session.decision["reservation_id"], "reservation-1")
        self.assertEqual(
            len([call for call in self.transport.calls if call["action"] == "submit"]),
            1,
        )

    def test_policy_drift_after_dispatch_still_replays_the_same_receipt(self) -> None:
        first = self.controller.submit(self.request, prompt="private prompt")
        self.clock[0] = datetime(2026, 9, 5, 12, 30, tzinfo=UTC).timestamp()
        self.policy.version = 8
        self.policy.digest = "sha256:" + "c" * 64
        self.policy.resolve_error = AssertionError(
            "a durable replay must not resolve fleet policy again"
        )
        second = self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(first["request_id"], second["request_id"])
        self.assertEqual(len(self.policy.route_calls), 1)
        session = self.store.policy_session_for_request("req-12345678")
        self.assertEqual(session.policy_version, 7)
        self.assertEqual(session.decision["reservation_id"], "reservation-1")

    def test_a_conflicting_payload_is_denied_without_reserving(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        with self.assertRaises(IdempotencyConflict):
            self.controller.submit(self.request, prompt="a different prompt")
        self.assertEqual(len(self.policy.route_calls), 1)
        self.assertEqual(len(self.policy.resolve_calls), 1)
        self.assertEqual(
            len([call for call in self.transport.calls if call["action"] == "submit"]),
            1,
        )

    def test_the_request_digest_ignores_the_hub_chosen_host_and_seat(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        first = self.store.get("req-12345678").digest
        other = FleetStore(Path(self.directory.name) / "second.db")
        rerouted = FakePolicy(machine="linux-worker", seat="coder")
        FleetController(
            enrolled(),
            other,
            self.transport,
            admission=self.admission,
            policy=rerouted,
            wall_clock=lambda: self.clock[0],
        ).submit(self.request, prompt="private prompt")
        self.assertEqual(other.get("req-12345678").digest, first)

    def test_the_request_digest_keeps_the_caller_override_intent(self) -> None:
        self.policy.machine = "linux-worker"
        pinned = dict(
            self.request,
            host="linux-worker",
            override_reason="operator pinned the controller host",
        )
        self.controller.submit(pinned, prompt="private prompt")
        with self.assertRaises(IdempotencyConflict):
            self.controller.submit(
                dict(pinned, override_reason="a different operator reason"),
                prompt="private prompt",
            )

    def test_the_request_digest_keeps_the_caller_seat_override(self) -> None:
        pinned = dict(
            self.request,
            seat="muse13",
            override_reason="operator pinned the review seat",
        )
        self.controller.submit(pinned, prompt="private prompt")
        with self.assertRaises(IdempotencyConflict):
            self.controller.submit(
                dict(pinned, seat="coder"), prompt="private prompt"
            )

    def test_an_unstarted_retry_reuses_the_decision_without_reserving_again(self) -> None:
        self.transport.fail_on = "doctor"
        with self.assertRaises(HostUnavailable):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.store.get("req-12345678").state, "blocked_offline")
        self.transport.fail_on = None
        self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(len(self.policy.route_calls), 1)
        self.assertEqual(len(self.policy.resolve_calls), 2)
        session = self.store.policy_session_for_request("req-12345678")
        self.assertEqual(session.decision["reservation_id"], "reservation-1")

    def test_expired_decision_is_re_routed_instead_of_reused(self) -> None:
        self.transport.fail_on = "doctor"
        with self.assertRaises(HostUnavailable):
            self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = None
        self.clock[0] = datetime(2026, 9, 5, 12, 30, tzinfo=UTC).timestamp()
        self.policy.expires_at = "2026-09-05T12:40:00Z"
        self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(len(self.policy.route_calls), 2)
        session = self.store.policy_session_for_request("req-12345678")
        self.assertEqual(session.decision["reservation_id"], "reservation-2")

    def test_already_expired_route_result_is_rejected(self) -> None:
        self.policy.expires_at = "2026-09-05T11:59:00Z"
        with self.assertRaises(PolicyRejected):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.transport.calls, [])

    def test_route_that_names_another_policy_version_conflicts(self) -> None:
        self.policy.decision_policy_version = 8
        with self.assertRaises(PolicyRevisionConflict):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.transport.calls, [])

    def test_a_later_policy_version_produces_a_new_decision_and_stamp(self) -> None:
        self.transport.fail_on = "doctor"
        with self.assertRaises(HostUnavailable):
            self.controller.submit(self.request, prompt="private prompt")
        self.transport.fail_on = None
        self.policy.version = 8
        self.policy.digest = "sha256:" + "c" * 64
        self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.stamp()["version"], 8)
        self.assertEqual(len(self.policy.route_calls), 2)
        session = self.store.policy_session_for_request("req-12345678")
        self.assertEqual(session.policy_version, 8)
        self.assertEqual(session.policy_digest, "sha256:" + "c" * 64)
        self.assertEqual(session.decision["decision_id"], "decision-2")

    def test_peer_mode_session_identity_precedes_the_routed_request_id(self) -> None:
        controller = self.build(fleet_config=enrolled(two_host_peer_config()))
        request = dict(self.request)
        request.pop("request_id")
        result = controller.submit(request, prompt="private prompt")
        request_id = result["request_id"]
        self.assertTrue(request_id.startswith("second-worker-controller-a-"))
        session_id = self.policy.resolve_calls[0]["session_id"]
        self.assertEqual(session_id, self.policy.route_calls[0]["session_id"])
        self.assertNotEqual(session_id, request_id)
        self.assertNotIn("second-worker", session_id)
        self.assertEqual(
            self.store.policy_session_for_request(request_id).session_id, session_id
        )

    def test_enrollment_off_keeps_the_legacy_contract(self) -> None:
        controller = FleetController(
            two_host_config(),
            self.store,
            self.transport,
            admission=self.admission,
            policy=self.policy,
        )
        with self.assertRaises(ValidationError):
            controller.submit(self.request, prompt="private prompt")
        with self.assertRaises(ValidationError):
            controller.submit(
                dict(self.request, host="linux-worker", seat="coder", override_reason="why"),
                prompt="private prompt",
            )
        controller.submit(
            dict(self.request, host="linux-worker", seat="coder"),
            prompt="private prompt",
        )
        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(self.policy.route_calls, [])
        submit = [
            call for call in self.transport.calls if call["action"] == "submit"
        ][0]
        self.assertNotIn("fleet_policy", submit)

class ControllerModelPlanTests(_FleetPolicyFixture, unittest.TestCase):
    """An enrolled controller plans a seat. It never admits one.

    Admission is the act that spends fleet capacity, and it belongs to the
    target that runs the turn, taken under proof of an acknowledged policy. The
    controller's job is to route and to say what a launch would use.
    """

    def order(self) -> list[str]:
        """The central calls this controller made, in the order it made them."""
        ledger: list[str] = []

        def wrap(target: object, name: str, label: str) -> None:
            original = getattr(target, name)

            def recorded(*args: object, **kwargs: object) -> object:
                ledger.append(label)
                return original(*args, **kwargs)

            setattr(target, name, recorded)

        wrap(self.policy, "resolve", "resolve")
        wrap(self.policy, "route", "route")
        wrap(self.admission, "plan", "plan")
        wrap(self.admission, "admit", "admit")
        return ledger

    def test_the_controller_resolves_routes_then_plans(self) -> None:
        ledger = self.order()
        self.controller.submit(self.request, prompt="private prompt")
        # The plan comes last: there is no seat to plan for until the Hub has
        # chosen one, and nothing is admitted here at all.
        self.assertEqual(ledger, ["resolve", "route", "plan"])
        self.assertEqual(self.admission.calls, [])

    def test_the_plan_names_only_the_consumer_and_the_routed_seat(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(
            self.admission.plan_calls,
            [{"consumer": "t3-fleet", "seat": "muse13"}],
        )

    def test_the_target_receives_a_plan_and_never_an_admission(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        submit = [
            call for call in self.transport.calls if call["action"] == "submit"
        ][0]
        self.assertNotIn("model_admission", submit)
        self.assertEqual(submit["model_plan"]["state"], "planned")
        self.assertEqual(submit["model_plan"]["launch_model"], NATIVE_MODEL)
        self.assertEqual(submit["model_plan"]["model"], CANONICAL_MODEL)

    def test_the_result_reports_a_plan_as_a_plan(self) -> None:
        result = self.controller.submit(self.request, prompt="private prompt")
        # A reader must never be able to mistake a plan for launch authority.
        self.assertNotIn("admission", result)
        self.assertEqual(result["model_plan"]["state"], "planned")

    def test_the_durable_record_keeps_the_plan_apart_from_an_admission(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        record = self.store.get("req-12345678")
        self.assertIsNone(record.admission)
        self.assertEqual(record.plan["state"], "planned")

    def test_a_caller_cannot_inject_a_plan_or_an_admission(self) -> None:
        for field, payload in (
            ("model_plan", dict(PLAN)),
            ("model_admission", self.admission.admit({
                "consumer": "t3-fleet",
                "phase": "controller",
                "request_id": "req-12345678",
                "seat": "muse13",
            })),
        ):
            with self.subTest(field=field):
                request = dict(self.request, **{field: payload})
                with self.assertRaisesRegex(ValidationError, field):
                    self.controller.submit(request, prompt="private prompt")
                self.assertEqual(self.transport.calls, [])

    def test_a_denied_plan_dispatches_nothing(self) -> None:
        self.admission.plan_error = PolicyRejected("Fleet Hub denied the plan")
        with self.assertRaises(PolicyRejected):
            self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.transport.calls, [])
        with self.assertRaises(ValidationError):
            self.store.get("req-12345678")

    def test_a_receipt_admitted_against_another_plan_is_refused(self) -> None:
        peer = self.build(fleet_config=replace(enrolled(), coordination_mode="target-authoritative"))
        original = self.transport.call

        def drifted(host: object, request: dict[str, object]) -> dict[str, object]:
            response = original(host, request)
            if request["action"] == "claim":
                response["model_admission"] = {
                    **response["model_admission"],
                    "model": "gpt-5.6-sol",
                }
            return response

        self.transport.call = drifted
        request = dict(self.request)
        request.pop("request_id")
        with self.assertRaisesRegex(PolicyRejected, "model plan"):
            peer.submit(request, prompt="private prompt")


class FleetReservationRelayTests(_FleetPolicyFixture, unittest.TestCase):
    """The controller relays reservation state; it never speaks for the target.

    A reservation is fenced by the target's node identity, so this controller
    only records what the target reported. Silence, a transport failure, or a
    reply about another decision all leave the capacity counted.
    """

    def running(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")

    def session(self):
        return self.store.policy_session_for_request("req-12345678")

    def reservation(self, state: str, **overrides: object) -> None:
        reply = {
            "schema": "brigade.fleet_reservation.v1",
            "reservation_id": "reservation-1",
            "decision_id": "decision-1",
            "state": state,
            "expires_at": "2026-09-05T12:20:00Z",
        }
        reply.update(overrides)
        self.transport.reservation = reply

    def test_status_never_runs_the_lifecycle_from_the_controller(self) -> None:
        self.running()
        self.transport.status_state = "running"
        self.reservation("active")
        result = self.controller.status("req-12345678")
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.session().reservation_state, "active")

    def test_a_release_reported_by_the_target_is_recorded(self) -> None:
        self.running()
        self.transport.status_state = "succeeded"
        self.reservation("released")
        self.controller.status("req-12345678")
        self.assertEqual(self.session().reservation_state, "released")

    def test_a_target_that_reports_nothing_keeps_the_reservation_counted(self) -> None:
        self.running()
        self.transport.status_state = "succeeded"
        self.controller.status("req-12345678")
        self.assertEqual(self.session().reservation_state, "active")

    def test_a_reply_about_another_decision_is_ignored(self) -> None:
        self.running()
        self.transport.status_state = "succeeded"
        self.reservation("released", decision_id="decision-9")
        self.controller.status("req-12345678")
        self.assertEqual(self.session().reservation_state, "active")
        self.reservation("released", reservation_id="reservation-9")
        self.controller.status("req-12345678")
        self.assertEqual(self.session().reservation_state, "active")

    def test_a_released_reservation_is_never_resurrected(self) -> None:
        self.running()
        self.transport.status_state = "succeeded"
        self.reservation("released")
        self.controller.status("req-12345678")
        self.assertEqual(self.session().reservation_state, "released")
        self.reservation("active")
        self.controller.status("req-12345678")
        self.assertEqual(self.session().reservation_state, "released")

    def test_a_transport_failure_never_reads_as_a_finished_request(self) -> None:
        self.running()
        self.reservation("released")
        self.transport.fail_on = "status"
        self.controller.status("req-12345678")
        self.assertEqual(self.session().reservation_state, "active")

    def test_recovery_records_the_state_the_target_reported(self) -> None:
        self.running()
        self.transport.fail_on = "status"
        self.controller.status("req-12345678")
        self.assertEqual(self.store.get("req-12345678").state, "unknown")
        self.assertEqual(self.session().reservation_state, "active")
        self.transport.fail_on = None
        self.transport.status_state = "succeeded"
        self.reservation("released")
        self.controller.recover("req-12345678")
        self.assertEqual(self.session().reservation_state, "released")

    def test_an_unenrolled_controller_records_no_reservation_state(self) -> None:
        controller = FleetController(
            two_host_config(),
            self.store,
            self.transport,
            admission=self.admission,
            policy=self.policy,
        )
        controller.submit(
            dict(self.request, host="linux-worker", seat="coder"),
            prompt="private prompt",
        )
        self.reservation("released")
        controller.status("req-12345678")
        self.assertIsNone(self.session())


class ControllerCanonicalIdentityTests(_FleetPolicyFixture, unittest.TestCase):
    """Requests are named by the local alias; policy is asked about the canonical id.

    The two identities do different jobs. The alias is what a caller types, what
    the work id and the checkout lookup use, and what the UI shows. The canonical
    remote identity is the only thing the Fleet Hub is asked about, so a repo
    keeps one policy across every machine that has a differently named checkout
    of it.
    """

    def test_every_hub_call_uses_the_canonical_identity(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.policy.resolve_calls[0]["repository"], CANONICAL_REPO)
        self.assertEqual(self.policy.route_calls[0]["repository"], CANONICAL_REPO)

    def test_the_stamp_carries_the_canonical_identity_to_the_target(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.stamp()["repo_identity"], CANONICAL_REPO)

    def test_the_alias_remains_the_request_and_lease_identity(self) -> None:
        result = self.controller.submit(self.request, prompt="private prompt")
        record = self.store.get(result["request_id"])
        self.assertEqual(record.repository, "example-repo")
        # The target is addressed with the alias too, because that is what its
        # own checkout table is keyed on.
        submits = [
            call for call in self.transport.calls if call["action"] in {"submit", "claim"}
        ]
        self.assertEqual(submits[0]["repository"], "example-repo")

    def test_a_caller_cannot_supply_its_own_canonical_identity(self) -> None:
        # An alias is the only repository a caller names. A caller-supplied
        # canonical id would be an untrusted replacement for the configured one.
        request = dict(self.request, repository=CANONICAL_REPO)
        with self.assertRaises(ValidationError):
            self.controller.submit(request, prompt="private prompt")
        self.assertEqual(self.policy.resolve_calls, [])

    def test_enrollment_without_a_canonical_mapping_fails_closed(self) -> None:
        config = enrolled()
        repository = config.repositories["example-repo"]
        controller = self.build(
            fleet_config=replace(
                config,
                repositories={
                    "example-repo": replace(repository, policy_identity=None)
                },
            )
        )
        with self.assertRaises(PolicyRejected):
            controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(self.transport.calls, [])

    def test_an_unenrolled_controller_never_needs_a_canonical_identity(self) -> None:
        config = two_host_config()
        repository = config.repositories["example-repo"]
        controller = self.build(
            fleet_config=replace(
                config,
                repositories={
                    "example-repo": replace(repository, policy_identity=None)
                },
            )
        )
        controller.submit(
            dict(self.request, host="linux-worker", seat="muse13"),
            prompt="private prompt",
        )
        submits = [
            call for call in self.transport.calls if call["action"] in {"submit", "claim"}
        ]
        self.assertEqual(submits[0]["repository"], "example-repo")
        self.assertNotIn("fleet_policy", submits[0])
        self.assertEqual(self.policy.resolve_calls, [])


DELEGATION_ID = "del-8f2c1a"
PARENT_REQUEST_ID = "run-7a1c9e"
DELEGATED_SESSION = "t3f-" + "d" * 32
DELEGATED_SOURCE = "abcdef0123456789abcdef0123456789abcdef01"


def delegation_receipt(**overrides: object) -> dict[str, object]:
    receipt = {
        "schema": "brigade.fleet_policy_delegation.v1",
        "delegation_id": DELEGATION_ID,
        "launch_authorized": False,
        "consumer": "brigade-run",
        "adapter": "t3-fleet",
        "parent_request_id": PARENT_REQUEST_ID,
        "decision_id": "decision-parent",
        "reservation_id": "reservation-parent",
        "session_id": DELEGATED_SESSION,
        "repo_identity": CANONICAL_REPO,
        "seat": "muse13",
        "workload": "windows-development",
        "policy_version": 7,
        "policy_digest": POLICY_DIGEST,
        "origin_node": "origin-node",
        "target_machine": "second-worker",
        "target_node": "second-worker-node",
        "source_revision": DELEGATED_SOURCE,
        "created_at": "2026-09-05T11:55:00Z",
        "expires_at": "2026-09-05T12:10:00Z",
    }
    receipt.update(overrides)
    return receipt


class ControllerWorkloadTests(_FleetPolicyFixture, unittest.TestCase):
    """An explicit workload is route input, never a local ranking override."""

    def test_a_windows_workload_with_host_auto_reaches_the_central_route(self) -> None:
        result = self.controller.submit(
            dict(self.request, workload="windows-development"),
            prompt="private prompt",
        )

        self.assertEqual(result["host"], "second-worker")
        self.assertEqual(len(self.policy.route_calls), 1)
        call = self.policy.route_calls[0]
        self.assertEqual(call["workload"], "windows-development")
        # It is not an override: central selection is still asked for, and no
        # operator justification is required to state what kind of work this is.
        self.assertIsNone(call["machine"])
        self.assertIsNone(call["seat"])
        self.assertIsNone(call["override_reason"])
        self.assertEqual(self.stamp()["workload"], "windows-development")

    def test_an_omitted_workload_inherits_the_configured_default(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(
            self.policy.route_calls[0]["workload"], "linux-development"
        )
        self.assertEqual(self.stamp()["workload"], "linux-development")

    def test_an_invalid_workload_is_refused_before_anything_is_journalled(self) -> None:
        for workload in ("Windows Development", "", "x" * 65, 7, "-leading"):
            with self.subTest(workload=workload):
                with self.assertRaises(ValidationError):
                    self.controller.submit(
                        dict(self.request, workload=workload),
                        prompt="private prompt",
                    )
        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(self.transport.calls, [])

    def test_workload_is_part_of_the_request_identity(self) -> None:
        self.controller.submit(
            dict(self.request, workload="windows-development"),
            prompt="private prompt",
        )
        with self.assertRaises(IdempotencyConflict):
            self.controller.submit(
                dict(self.request, workload="linux-development"),
                prompt="private prompt",
            )
        # And the same explicit intent replays rather than routing again.
        self.controller.submit(
            dict(self.request, workload="windows-development"),
            prompt="private prompt",
        )
        self.assertEqual(len(self.policy.route_calls), 1)

    def test_an_unenrolled_controller_refuses_an_explicit_workload(self) -> None:
        controller = self.build(fleet_config=two_host_config())
        with self.assertRaises(ValidationError):
            controller.submit(
                dict(
                    self.request,
                    host="linux-worker",
                    seat="muse13",
                    workload="windows-development",
                ),
                prompt="private prompt",
            )
        self.assertEqual(self.transport.calls, [])

    def test_an_unenrolled_controller_without_a_workload_is_unchanged(self) -> None:
        controller = self.build(fleet_config=two_host_config())
        controller.submit(
            dict(self.request, host="linux-worker", seat="muse13"),
            prompt="private prompt",
        )
        submits = [
            call for call in self.transport.calls if call["action"] in {"submit", "claim"}
        ]
        self.assertEqual(len(submits), 1)
        self.assertNotIn("fleet_policy", submits[0])


class ControllerDelegationAdoptionTests(_FleetPolicyFixture, unittest.TestCase):
    """`--delegation-id` adopts an existing parent decision. It never routes."""

    def setUp(self) -> None:
        super().setUp()
        self.policy.delegation_receipt = delegation_receipt()
        self.adopted = {
            "delegation_id": DELEGATION_ID,
            "title": "Fix issue 123",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
        }

    def submit(self, **overrides: object) -> dict[str, object]:
        return self.controller.submit(
            dict(self.adopted, **overrides), prompt="private prompt"
        )

    def stamp(self) -> dict[str, object]:
        submits = [
            call
            for call in self.transport.calls
            if call["action"] in {"submit", "claim"}
        ]
        self.assertEqual(len(submits), 1)
        return submits[0]["fleet_policy"]

    def test_adoption_takes_the_existing_decision_without_routing_again(self) -> None:
        result = self.submit()

        self.assertEqual(self.policy.route_calls, [])
        self.assertEqual(len(self.policy.delegation_calls), 1)
        self.assertEqual(
            self.policy.delegation_calls[0], {"delegation_id": DELEGATION_ID}
        )
        self.assertEqual(result["host"], "second-worker")
        stamp = self.stamp()
        self.assertEqual(stamp["delegation_id"], DELEGATION_ID)
        self.assertEqual(stamp["decision_id"], "decision-parent")
        self.assertEqual(stamp["reservation_id"], "reservation-parent")
        self.assertEqual(stamp["session_id"], DELEGATED_SESSION)
        self.assertEqual(stamp["seat"], "muse13")
        self.assertEqual(stamp["workload"], "windows-development")
        # The canonical origin identity from the receipt, never this
        # controller's own local alias.
        self.assertEqual(stamp["origin"], "origin-node")

    def test_the_parent_consumer_is_used_only_after_the_receipt_is_verified(self) -> None:
        self.submit()
        self.assertEqual(
            self.policy.resolve_calls[0]["consumer"], "brigade-run"
        )
        self.assertEqual(
            self.policy.resolve_calls[0]["decision_id"], "decision-parent"
        )
        self.assertEqual(self.admission.plan_calls[0]["consumer"], "brigade-run")
        self.assertEqual(
            self.admission.plan_calls[0]["delegation_id"], DELEGATION_ID
        )

    def test_a_normal_request_keeps_the_fixed_t3_fleet_consumer(self) -> None:
        self.controller.submit(self.request, prompt="private prompt")
        self.assertEqual(self.policy.resolve_calls[0]["consumer"], "t3-fleet")
        self.assertEqual(self.policy.route_calls[0]["consumer"], "t3-fleet")
        self.assertEqual(self.admission.plan_calls[0]["consumer"], "t3-fleet")
        self.assertNotIn("delegation_id", self.admission.plan_calls[0])

    def test_the_immutable_source_revision_comes_from_the_receipt(self) -> None:
        self.submit()
        submits = [
            call
            for call in self.transport.calls
            if call["action"] in {"submit", "claim"}
        ]
        self.assertEqual(submits[0]["source_revision"], DELEGATED_SOURCE)

    def test_a_conflicting_supplied_context_refuses(self) -> None:
        for key, value in (
            ("host", "linux-worker"),
            ("host", "auto"),
            ("seat", "claude_standby"),
            ("workload", "linux-development"),
            ("source_revision", FULL_SHA),
        ):
            with self.subTest(key=key, value=value):
                controller = self.build(store=FleetStore(
                    Path(self.directory.name) / f"conflict-{key}-{value}.db"
                ))
                with self.assertRaises(PolicyRejected) as caught:
                    controller.submit(
                        dict(self.adopted, **{key: value}),
                        prompt="private prompt",
                    )
                self.assertEqual(caught.exception.reason, "delegation-conflict")
        self.assertEqual(self.transport.calls, [])

    def test_a_supplied_option_that_confirms_the_receipt_is_accepted(self) -> None:
        self.submit(
            host="second-worker",
            seat="muse13",
            workload="windows-development",
            source_revision=DELEGATED_SOURCE,
            repository="example-repo",
        )
        self.assertEqual(self.stamp()["delegation_id"], DELEGATION_ID)

    def test_enrollment_off_refuses_adoption_instead_of_falling_back(self) -> None:
        controller = self.build(fleet_config=two_host_config())
        with self.assertRaises(PolicyRejected) as caught:
            controller.submit(dict(self.adopted), prompt="private prompt")
        self.assertEqual(caught.exception.reason, "delegation-not-enrolled")
        self.assertEqual(self.policy.delegation_calls, [])
        self.assertEqual(self.transport.calls, [])

    def test_an_override_reason_does_not_apply_to_an_adopted_decision(self) -> None:
        with self.assertRaises(ValidationError):
            self.submit(override_reason="operator pinned the seat")
        self.assertEqual(self.transport.calls, [])

    def test_a_caller_supplied_request_id_is_refused(self) -> None:
        with self.assertRaises(ValidationError):
            self.submit(request_id="req-12345678")
        self.assertEqual(self.transport.calls, [])

    def test_a_receipt_that_is_not_this_contract_never_adopts(self) -> None:
        for mutation in (
            {"launch_authorized": True},
            {"consumer": "t3-fleet"},
            {"adapter": "brigade-run"},
            {"schema": "brigade.fleet_policy_delegation.v2"},
            {"source_revision": "ABCDEF0123456789abcdef0123456789abcdef01"},
            {"source_revision": "abcdef01"},
        ):
            with self.subTest(mutation=sorted(mutation)):
                self.policy.delegation_receipt = delegation_receipt(**mutation)
                controller = self.build(store=FleetStore(
                    Path(self.directory.name) / f"receipt-{sorted(mutation)}.db"
                ))
                with self.assertRaises(ValidationError):
                    controller.submit(dict(self.adopted), prompt="private prompt")
        # The parent consumer is only ever used after the receipt verified, so
        # a refused receipt leaves no call under it at all.
        self.assertEqual(self.policy.resolve_calls, [])
        self.assertEqual(self.admission.plan_calls, [])
        self.assertEqual(self.transport.calls, [])

    def test_an_extra_identity_field_is_refused_rather_than_ignored(self) -> None:
        self.policy.delegation_receipt = delegation_receipt(worktree="/srv/secret")
        with self.assertRaises(ValidationError):
            self.submit()

    def test_a_receipt_missing_a_mandatory_field_is_refused(self) -> None:
        for missing in ("seat", "workload", "source_revision", "origin_node", "target_node"):
            with self.subTest(missing=missing):
                receipt = delegation_receipt()
                receipt.pop(missing)
                self.policy.delegation_receipt = receipt
                controller = self.build(store=FleetStore(
                    Path(self.directory.name) / f"missing-{missing}.db"
                ))
                with self.assertRaises(ValidationError):
                    controller.submit(dict(self.adopted), prompt="private prompt")

    def test_an_expired_delegation_is_refused(self) -> None:
        self.policy.delegation_receipt = delegation_receipt(
            expires_at="2026-09-05T11:59:00Z"
        )
        with self.assertRaises(PolicyRejected):
            self.submit()
        self.assertEqual(self.transport.calls, [])

    def test_a_delegation_window_beyond_the_bound_is_refused(self) -> None:
        self.policy.delegation_receipt = delegation_receipt(
            expires_at="2026-09-08T12:00:00Z"
        )
        with self.assertRaises(PolicyRejected):
            self.submit()

    def test_a_policy_that_moved_after_the_delegation_is_a_conflict(self) -> None:
        self.policy.version = 8
        with self.assertRaises(PolicyRevisionConflict):
            self.submit()
        self.assertEqual(self.transport.calls, [])

    def test_no_configured_repository_matches_the_canonical_identity(self) -> None:
        self.policy.delegation_receipt = delegation_receipt(
            repo_identity="git.example.invalid/team/repository"
        )
        with self.assertRaises(PolicyRejected) as caught:
            self.submit()
        self.assertEqual(
            caught.exception.reason, "delegation-repository-unmatched"
        )

    def test_an_ambiguous_canonical_identity_refuses(self) -> None:
        base = enrolled()
        repository = base.repositories["example-repo"]
        controller = self.build(
            fleet_config=replace(
                base,
                repositories={
                    "example-repo": repository,
                    "example-mirror": replace(
                        repository,
                        repository_id="example-mirror",
                        policy_identity=CANONICAL_REPO,
                    ),
                },
            )
        )
        with self.assertRaises(PolicyRejected) as caught:
            controller.submit(dict(self.adopted), prompt="private prompt")
        self.assertEqual(
            caught.exception.reason, "delegation-repository-unmatched"
        )

    def test_an_explicit_repository_may_narrow_an_ambiguous_identity(self) -> None:
        base = enrolled()
        repository = base.repositories["example-repo"]
        controller = self.build(
            fleet_config=replace(
                base,
                repositories={
                    "example-repo": repository,
                    "example-mirror": replace(
                        repository,
                        repository_id="example-mirror",
                        policy_identity=CANONICAL_REPO,
                    ),
                },
            )
        )
        controller.submit(
            dict(self.adopted, repository="example-mirror"),
            prompt="private prompt",
        )
        submits = [
            call
            for call in self.transport.calls
            if call["action"] in {"submit", "claim"}
        ]
        self.assertEqual(submits[0]["repository"], "example-mirror")

    def test_an_explicit_repository_that_maps_elsewhere_refuses(self) -> None:
        base = enrolled()
        repository = base.repositories["example-repo"]
        controller = self.build(
            fleet_config=replace(
                base,
                repositories={
                    "example-repo": repository,
                    "other-repo": replace(
                        repository,
                        repository_id="other-repo",
                        policy_identity="git.example.invalid/team/repository",
                    ),
                },
            )
        )
        with self.assertRaises(PolicyRejected) as caught:
            controller.submit(
                dict(self.adopted, repository="other-repo"),
                prompt="private prompt",
            )
        self.assertEqual(
            caught.exception.reason, "delegation-repository-unmatched"
        )

    def test_one_adoption_is_one_execution(self) -> None:
        first = self.submit()
        second = self.submit()

        self.assertEqual(first["request_id"], second["request_id"])
        # No second delegation read, no route, no second plan and no second
        # dispatch: the replay is answered from the durable receipt.
        self.assertEqual(len(self.policy.delegation_calls), 1)
        self.assertEqual(self.policy.route_calls, [])
        self.assertEqual(len(self.admission.plan_calls), 1)
        self.assertEqual(
            len(
                [
                    call
                    for call in self.transport.calls
                    if call["action"] in {"submit", "claim"}
                ]
            ),
            1,
        )

    def test_different_context_under_one_adoption_conflicts(self) -> None:
        self.submit()
        with self.assertRaises(IdempotencyConflict):
            self.controller.submit(
                dict(self.adopted), prompt="a different private prompt"
            )
        with self.assertRaises(IdempotencyConflict):
            self.submit(title="Another title")
        self.assertEqual(len(self.policy.delegation_calls), 1)

    def test_the_parent_request_id_is_kept_beside_the_execution_identity(self) -> None:
        result = self.submit()
        binding = self.store.delegation_binding(DELEGATION_ID)
        self.assertEqual(binding, (result["request_id"], PARENT_REQUEST_ID))
        self.assertNotEqual(result["request_id"], PARENT_REQUEST_ID)

    def test_the_execution_identity_is_derived_from_the_delegation(self) -> None:
        first = self.submit()
        rebuilt = self.build(
            store=FleetStore(Path(self.directory.name) / "rebuilt.db")
        )
        second = rebuilt.submit(dict(self.adopted), prompt="private prompt")
        self.assertEqual(first["request_id"], second["request_id"])

    def test_the_hub_reservation_becomes_the_execution_claim(self) -> None:
        """Adoption takes the parent's reservation over, it never takes a second.

        `FakePolicy` has no reserve verb at all - a reservation only ever comes
        back from `route` - so an empty `route_calls` is the proof that none was
        taken here. What this adds is the other half: the reservation the
        session now runs under is the parent's own, live, and the only one.
        """
        result = self.submit()
        session = self.store.policy_session_for_request(result["request_id"])

        self.assertEqual(self.policy.route_calls, [])
        self.assertEqual(
            session.decision["reservation_id"], "reservation-parent"
        )
        self.assertEqual(session.reservation_state, "active")
        # The claim the target is handed names that same reservation, so no
        # second claim exists anywhere for this execution.
        self.assertEqual(self.stamp()["reservation_id"], "reservation-parent")
        self.assertEqual(self.stamp()["decision_id"], "decision-parent")

    def test_a_delegated_request_records_the_adopted_session_and_decision(self) -> None:
        result = self.submit()
        session = self.store.policy_session_for_request(result["request_id"])
        self.assertEqual(session.session_id, DELEGATED_SESSION)
        self.assertEqual(session.consumer, "brigade-run")
        self.assertEqual(session.workload, "windows-development")
        self.assertEqual(session.decision["decision_id"], "decision-parent")
        self.assertEqual(session.decision["reservation_id"], "reservation-parent")


class ControllerDelegatedHandoffTests(_FleetPolicyFixture, unittest.TestCase):
    """A delegated result may carry the target's own pending-handoff statement."""

    def setUp(self) -> None:
        super().setUp()
        self.controller = self.build(
            fleet_config=enrolled(two_host_peer_config()),
            store=self.store,
        )
        self.policy.delegation_receipt = delegation_receipt()
        self.adopted = {
            "delegation_id": DELEGATION_ID,
            "title": "Fix issue 123",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
        }

    def handoff(self, request_id: str, overrides: dict[str, object] | None = None) -> dict[str, object]:
        payload = {
            "target_machine": "second-worker",
            "repository": CANONICAL_REPO,
            "branch": f"codex/{request_id}",
            "source_revision": DELEGATED_SOURCE,
            "request_id": request_id,
            "worktree_ref": f"t3-fleet:worktree:{request_id}",
        }
        payload.update(overrides or {})
        return payload

    def finish(self, request_id: str, state: str, **extra: object) -> None:
        receipt = dict(self.transport.receipts[request_id])
        receipt["state"] = state
        receipt.update(extra)
        self.transport.receipts[request_id] = receipt

    def test_a_terminal_delegated_result_carries_the_frozen_handoff(self) -> None:
        result = self.controller.submit(dict(self.adopted), prompt="private prompt")
        request_id = result["request_id"]
        self.finish(
            request_id,
            "succeeded",
            disposition="pending-handoff",
            handoff=self.handoff(request_id),
        )
        status = self.controller.status(request_id)

        # The real execution state is retained; the disposition is additional.
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(status["disposition"], "pending-handoff")
        self.assertEqual(
            set(status["handoff"]),
            {
                "target_machine",
                "repository",
                "branch",
                "source_revision",
                "request_id",
                "worktree_ref",
            },
        )
        self.assertEqual(
            status["handoff"]["worktree_ref"], f"t3-fleet:worktree:{request_id}"
        )

    def test_a_running_delegated_result_carries_no_disposition(self) -> None:
        result = self.controller.submit(dict(self.adopted), prompt="private prompt")
        status = self.controller.status(result["request_id"])
        self.assertEqual(status["state"], "running")
        self.assertNotIn("disposition", status)
        self.assertNotIn("handoff", status)

    def test_a_pre_launch_failure_carries_no_disposition(self) -> None:
        """A terminal failure the target omitted a handoff for stays a failure.

        The target removes the worktree of a launch that never started, so it
        reports no handoff. Nothing here may invent one, and the failure must
        not be quietly upgraded into retained work awaiting review.
        """
        result = self.controller.submit(dict(self.adopted), prompt="private prompt")
        request_id = result["request_id"]
        self.finish(request_id, "failed")
        status = self.controller.status(request_id)

        self.assertEqual(status["state"], "failed")
        self.assertNotIn("disposition", status)
        self.assertNotIn("handoff", status)

    def test_an_unknown_state_may_not_claim_a_completed_handoff(self) -> None:
        """`unknown` is not terminal, so it can never carry a completion."""
        result = self.controller.submit(dict(self.adopted), prompt="private prompt")
        request_id = result["request_id"]
        self.finish(
            request_id,
            "unknown",
            disposition="pending-handoff",
            handoff=self.handoff(request_id),
        )
        with self.assertRaises(ValidationError):
            self.controller.status(request_id)

    def test_a_disposition_on_a_non_terminal_state_is_refused(self) -> None:
        result = self.controller.submit(dict(self.adopted), prompt="private prompt")
        request_id = result["request_id"]
        self.finish(
            request_id,
            "running",
            disposition="pending-handoff",
            handoff=self.handoff(request_id),
        )
        with self.assertRaises(ValidationError):
            self.controller.status(request_id)

    def test_a_handoff_that_is_not_the_frozen_shape_is_refused(self) -> None:
        result = self.controller.submit(dict(self.adopted), prompt="private prompt")
        request_id = result["request_id"]
        for mutation in (
            {"worktree_ref": "/srv/state/worktrees/" + request_id, "extra": "x"},
            {"source_revision": "not-a-commit"},
            {"request_id": "second-worker-controller-a-11111111-1111-1111-1111-111111111111"},
            {"target_machine": "linux-worker"},
        ):
            with self.subTest(mutation=sorted(mutation)):
                self.finish(
                    request_id,
                    "succeeded",
                    disposition="pending-handoff",
                    handoff=self.handoff(request_id, mutation),
                )
                with self.assertRaises(ValidationError):
                    self.controller.status(request_id)

    def test_an_unsupported_disposition_is_refused(self) -> None:
        result = self.controller.submit(dict(self.adopted), prompt="private prompt")
        request_id = result["request_id"]
        self.finish(request_id, "succeeded", disposition="implemented")
        with self.assertRaises(ValidationError):
            self.controller.status(request_id)


def _with_host(base: FleetConfig, **overrides) -> FleetConfig:
    host = base.hosts["linux-worker"]
    return FleetConfig(
        controller_host=base.controller_host,
        global_active_limit=base.global_active_limit,
        state_dir=base.state_dir,
        hosts={"linux-worker": HostConfig(**{**host.__dict__, **overrides})},
        repositories=base.repositories,
    )


class RepositoryConcurrencyControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.now = [1000.0]
        self.store = FleetStore(
            Path(self.directory.name) / "fleet.db", clock=lambda: self.now[0]
        )
        self.transport = FakeTransport()
        self.admission = FakeAdmission()
        self.request = {
            "request_id": "req-12345678",
            "host": "linux-worker",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "seat": "coder",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
        }

    def leases(self) -> list[str]:
        import sqlite3

        with sqlite3.connect(self.store.path) as connection:
            return [
                row[0]
                for row in connection.execute(
                    "SELECT request_id FROM leases ORDER BY request_id"
                )
            ]

    def test_default_lets_one_repository_use_full_host_capacity(self) -> None:
        cfg = _with_host(config(), active_limit=3)
        controller = FleetController(cfg, self.store, self.transport, admission=self.admission)
        for suffix in ("11111111", "22222222", "33333333"):
            result = controller.submit(
                dict(self.request, request_id=f"req-{suffix}"), prompt="prompt"
            )
            self.assertEqual(result["state"], "running")
        self.assertEqual(len(self.leases()), 3)
        # The host limit, not the repository limit, rejects the fourth.
        with self.assertRaisesRegex(HostUnavailable, "capacity"):
            controller.submit(dict(self.request, request_id="req-44444444"), prompt="prompt")

    def test_explicit_limit_one_blocks_second_request_for_the_repository(self) -> None:
        cfg = _with_host(config(), active_limit=3, max_concurrent_per_repository=1)
        controller = FleetController(cfg, self.store, self.transport, admission=self.admission)
        controller.submit(self.request, prompt="prompt")
        second = dict(self.request, request_id="req-87654321")
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            controller.submit(second, prompt="prompt")
        self.assertEqual(self.store.get("req-87654321").state, "queued")

    def test_explicit_limit_n_allows_n_and_rejects_n_plus_one(self) -> None:
        cfg = _with_host(config(), active_limit=5, max_concurrent_per_repository=2)
        controller = FleetController(cfg, self.store, self.transport, admission=self.admission)
        controller.submit(dict(self.request, request_id="req-11111111"), prompt="p")
        controller.submit(dict(self.request, request_id="req-22222222"), prompt="p")
        with self.assertRaisesRegex(HostUnavailable, r"repository is busy.*\(2 concurrent"):
            controller.submit(dict(self.request, request_id="req-33333333"), prompt="p")

    def test_lease_uses_configured_ttl(self) -> None:
        cfg = _with_host(
            config(), max_concurrent_per_repository=1, repository_lease_ttl_seconds=600
        )
        controller = FleetController(cfg, self.store, self.transport, admission=self.admission)
        controller.submit(self.request, prompt="prompt")
        self.now[0] += 599
        self.assertFalse(
            self.store.acquire_lease("linux-worker", "example-repo", "other", ttl=60, limit=1)
        )
        self.now[0] += 1
        self.assertTrue(
            self.store.acquire_lease("linux-worker", "example-repo", "other", ttl=60, limit=1)
        )

    def test_terminal_status_and_recover_release_leases(self) -> None:
        cfg = _with_host(config(), active_limit=3)
        controller = FleetController(cfg, self.store, self.transport, admission=self.admission)
        for suffix in ("11111111", "22222222", "33333333"):
            controller.submit(dict(self.request, request_id=f"req-{suffix}"), prompt="p")
        self.transport.status_state = "succeeded"
        controller.status("req-11111111")
        self.transport.status_state = "failed"
        controller.status("req-22222222")
        self.assertEqual(self.leases(), ["req-33333333"])
        self.transport.status_state = "interrupted"
        controller.recover("req-33333333")
        self.assertEqual(self.store.get("req-33333333").state, "interrupted")
        self.assertEqual(self.leases(), [])


class TargetBusyControllerTests(unittest.TestCase):
    """A target that declines unstarted work is a pre-launch block, not a loss."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = FleetStore(Path(self.directory.name) / "fleet.db")
        self.transport = FakeTransport()
        self.admission = FakeAdmission()
        self.request = {
            "request_id": "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc",
            "host": "linux-worker",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "seat": "coder",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
        }

    def test_busy_claim_requeues_instead_of_going_unknown(self) -> None:
        controller = FleetController(
            peer_config(), self.store, self.transport, admission=self.admission
        )
        self.transport.fail_on = "claim"
        self.transport.fail_exc = TargetBusy(
            "target declined the request before starting it",
            reason="repository-busy",
        )
        for _ in range(3):
            with self.assertRaises(HostUnavailable) as raised:
                controller.submit(self.request, prompt="private prompt")
            self.assertNotIsInstance(raised.exception, UnknownState)
            self.assertEqual(raised.exception.reason, "repository-busy")
            self.assertEqual(self.store.get(self.request["request_id"]).state, "queued")
        # A busy answer is a healthy target: no circuit opens.
        self.assertFalse(self.store.circuit_is_open("linux-worker"))
        # Once the repository frees up, the same request goes through.
        self.transport.fail_on = None
        result = controller.submit(self.request, prompt="private prompt")
        self.assertEqual(result["state"], "running")

    def test_busy_single_controller_dispatch_releases_its_lease(self) -> None:
        controller = FleetController(
            _with_host(config(), max_concurrent_per_repository=1),
            self.store,
            self.transport,
            admission=self.admission,
        )
        request = dict(self.request, request_id="req-12345678")
        self.transport.fail_on = "submit"
        self.transport.fail_exc = TargetBusy("busy", reason="repository-busy")
        with self.assertRaises(HostUnavailable) as raised:
            controller.submit(request, prompt="private prompt")
        self.assertNotIsInstance(raised.exception, UnknownState)
        self.assertEqual(self.store.get("req-12345678").state, "queued")
        # The slot was handed back, so another request can take it.
        self.assertTrue(
            self.store.acquire_lease("linux-worker", "example-repo", "other", ttl=60)
        )

    def test_status_keeps_a_running_request_whose_lease_expired(self) -> None:
        now = [1000.0]
        store = FleetStore(Path(self.directory.name) / "lease.db", clock=lambda: now[0])
        cfg = _with_host(
            config(), max_concurrent_per_repository=1, repository_lease_ttl_seconds=600
        )
        controller = FleetController(cfg, store, self.transport, admission=self.admission)
        request = dict(self.request, request_id="req-12345678")
        controller.submit(request, prompt="private prompt")
        # Nobody polled for longer than the lease lifetime, and another
        # request took the slot in the meantime.
        now[0] += 601
        self.assertTrue(
            store.acquire_lease("linux-worker", "example-repo", "other", ttl=600, limit=1)
        )
        result = controller.status("req-12345678")
        self.assertEqual(result["state"], "running")
        import sqlite3

        with sqlite3.connect(store.path) as connection:
            rows = dict(
                connection.execute(
                    "SELECT request_id, expires_at FROM leases ORDER BY request_id"
                ).fetchall()
            )
        # The running request holds its repository again, renewed for a full
        # window, and the count includes it.
        self.assertEqual(rows["req-12345678"], now[0] + 600)
        self.assertIn("other", rows)


LONG_INPUT_ID = "runtime-request:provider:codex:provider-turn:" + "x" * 190


class PendingRequestControllerTests(unittest.TestCase):
    """`t3-fleet status` names the pending request so answer/approve can use it."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = FleetStore(Path(self.directory.name) / "fleet.db")
        self.transport = FakeTransport()
        self.controller = FleetController(
            config(), self.store, self.transport, admission=FakeAdmission()
        )
        self.controller.submit(
            {
                "request_id": "req-12345678", "host": "linux-worker", "repository": "example-repo",
                "source_revision": FULL_SHA, "title": "t", "seat": "coder",
                "runtime_mode": "full-access", "interaction_mode": "default",
            },
            prompt="private prompt",
        )
        self.transport.status_state = "needs_attention"
        self.question = {"id": "0", "header": "Question", "question": "Which route?", "options": [{"label": "A"}]}

    def test_status_passes_the_validated_pending_request_through(self) -> None:
        self.transport.status_extra = {
            "pending_runtime_request": {"id": LONG_INPUT_ID, "kind": "user_input", "createdAt": "2026-10-10T00:45:38Z"},
            "pending_request": {
                "id": LONG_INPUT_ID, "kind": "user_input", "created_at": "2026-10-10T00:45:38Z",
                "respond_with": "answer", "questions": [self.question], "injected": "drop",
            },
        }
        result = self.controller.status("req-12345678")
        self.assertEqual(result["pending_request"], {
            "id": LONG_INPUT_ID, "kind": "user_input", "created_at": "2026-10-10T00:45:38Z",
            "respond_with": "answer", "questions": [self.question],
        })
        self.assertNotIn("pending_runtime_request", result)

    def test_controller_bounds_what_a_target_sends(self) -> None:
        self.transport.status_extra = {"pending_request": {
            "id": LONG_INPUT_ID, "kind": "user_input",
            "questions": [{"id": str(n), "question": "q" * 9000, "header": "h" * 900,
                           "options": [{"label": "l" * 900, "evil": 1}] * 40} for n in range(30)],
        }}
        payload = self.controller.status("req-12345678")["pending_request"]
        self.assertEqual(len(payload["questions"]), 5)
        self.assertTrue(payload["questions_truncated"])
        self.assertEqual(len(payload["questions"][0]["question"]), 2000)
        self.assertEqual(len(payload["questions"][0]["header"]), 200)
        self.assertEqual(len(payload["questions"][0]["options"]), 10)
        self.assertEqual(payload["questions"][0]["options"][0], {"label": "l" * 200})

    def test_an_older_target_still_yields_the_answerable_id(self) -> None:
        self.transport.status_extra = {
            "pending_runtime_request": {"id": "approval-7", "kind": "command", "createdAt": "2026-10-10T00:00:00Z"},
        }
        result = self.controller.status("req-12345678")
        self.assertEqual(result["pending_request"], {
            "id": "approval-7", "kind": "command", "created_at": "2026-10-10T00:00:00Z", "respond_with": "approve",
        })

    def test_malformed_or_oversized_ids_are_withheld(self) -> None:
        for pending in ({"id": "", "kind": "user_input"}, {"id": "i" * 2000, "kind": "user_input"},
                        {"id": 7, "kind": "user_input"}, {"id": "ok", "kind": ""}, "nope"):
            with self.subTest(pending=pending):
                self.transport.status_extra = {"pending_request": pending}
                self.assertNotIn("pending_request", self.controller.status("req-12345678"))

    def test_only_needs_attention_carries_a_pending_request(self) -> None:
        self.transport.status_state = "running"
        self.transport.status_extra = {"pending_request": {"id": "x", "kind": "user_input"}}
        self.assertNotIn("pending_request", self.controller.status("req-12345678"))

    def test_answer_refused_before_delivery_keeps_the_request_answerable(self) -> None:
        self.controller.status("req-12345678")
        self.transport.fail_on = "answer"
        self.transport.fail_exc = HostUnavailable(
            "target adapter failed", detail="model admission adapter failed: hub-unavailable", not_started=True
        )
        with self.assertRaises(HostUnavailable) as raised:
            self.controller.answer("req-12345678", "input-1", {"0": "yes"})
        self.assertIn("before delivering", raised.exception.message)
        self.assertIn("hub-unavailable", raised.exception.message)
        self.assertEqual(self.store.get("req-12345678").state, "needs_attention")

    def test_approve_refused_before_delivery_keeps_the_request_approvable(self) -> None:
        self.controller.status("req-12345678")
        self.transport.fail_on = "approve"
        self.transport.fail_exc = HostUnavailable("target adapter failed", not_started=True)
        with self.assertRaises(HostUnavailable):
            self.controller.approve("req-12345678", "approval-1", "accept")
        self.assertEqual(self.store.get("req-12345678").state, "needs_attention")

    def test_ambiguous_answer_failure_still_becomes_unknown(self) -> None:
        self.controller.status("req-12345678")
        self.transport.fail_on = "answer"
        with self.assertRaises(UnknownState):
            self.controller.answer("req-12345678", "input-1", {"0": "yes"})
        self.assertEqual(self.store.get("req-12345678").state, "unknown")

    def test_followup_on_needs_attention_points_at_answer_and_approve(self) -> None:
        self.controller.status("req-12345678")
        with self.assertRaises(ValidationError) as raised:
            self.controller.followup("req-12345678", prompt="continue")
        self.assertIn("t3-fleet answer", raised.exception.message)
        self.assertIn("t3-fleet approve", raised.exception.message)
        self.assertIn("pending_request", raised.exception.message)

if __name__ == "__main__":
    unittest.main()
