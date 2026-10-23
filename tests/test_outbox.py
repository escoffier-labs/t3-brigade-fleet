"""The local ordinary-work outbox and the `tick` that drains it.

The rules these tests hold to, in one place, because most of them are about
what must *not* happen:

* Only an explicit submission enters the queue, and an explicit refusal is
  honoured as a refusal.
* A retry replays the caller's own intent, never the machine a failed route
  picked.
* Unknown acceptance is held, never retried and never freed.
* Nothing that leaves this process carries the prompt.
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from t3_fleet.config import FleetConfig, OutboxConfig, PolicyConfig
from t3_fleet.controller import FleetController
from t3_fleet.errors import (
    HostUnavailable,
    IdempotencyConflict,
    PolicyRejected,
    UnknownState,
    ValidationError,
)
from t3_fleet.outbox import OutboxLimits, OutboxStore, StaleLease
from t3_fleet.store import FleetStore

from test_controller import (
    FULL_SHA,
    FakeAdmission,
    FakePolicy,
    HostRecordingTransport,
    POLICY_WALL_CLOCK,
    enrolled,
    two_host_config,
)

PROMPT = "private prompt about an unreleased internal design"


def outbox_config(base: FleetConfig, **overrides: object) -> FleetConfig:
    settings = {
        "enabled": True,
        "max_records": 4,
        "max_attempts": 3,
        "backoff_seconds": 60,
        "max_jobs_per_tick": 2,
        "max_tick_seconds": 60,
        "retention_seconds": 3600,
        "lease_seconds": 120,
    }
    settings.update(overrides)
    return replace(base, outbox=OutboxConfig(**settings))


class OutboxStoreTests(unittest.TestCase):
    """The durable store on its own: permissions, bounds, fencing, retention."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.now = [1_000.0]
        self.serial = [0]
        self.store = self.build()

    def build(self, **limits: object) -> OutboxStore:
        def mint() -> str:
            self.serial[0] += 1
            return f"{self.serial[0]:032x}"

        return OutboxStore(
            Path(self.directory.name) / "outbox",
            limits=OutboxLimits(
                max_records=3,
                max_prompt_bytes=1024,
                max_intent_bytes=1024,
                retention_seconds=3600.0,
                max_attempts=3,
                backoff_seconds=60.0,
                max_backoff_seconds=600.0,
                lease_seconds=120.0,
                **limits,
            ),
            clock=lambda: self.now[0],
            uuid_factory=mint,
        )

    def enqueue(self, **overrides: object):
        payload = {
            "digest": "d" * 64,
            "intent": {"repository": "example-repo", "host": "auto"},
            "prompt": PROMPT,
            "host": "auto",
            "repository": "example-repo",
        }
        payload.update(overrides)
        return self.store.enqueue(**payload)

    def test_each_connection_enables_secure_delete_even_when_sqlite_default_is_off(self) -> None:
        import sqlite3
        from unittest.mock import patch
        connect = sqlite3.connect
        connections = []
        def initially_insecure(*args, **kwargs):
            connection = connect(*args, **kwargs)
            connection.execute("PRAGMA secure_delete = OFF")
            self.assertEqual(connection.execute("PRAGMA secure_delete").fetchone()[0], 0)
            connections.append(connection)
            return connection
        try:
            with patch("t3_fleet.outbox.sqlite3.connect", side_effect=initially_insecure):
                for _ in range(2):
                    connection = self.store._connect()
                    self.assertEqual(connection.execute("PRAGMA secure_delete").fetchone()[0], 1)
        finally:
            for connection in connections:
                connection.close()

    def test_the_store_is_owner_only_on_disk(self) -> None:
        if os.name == "nt":
            self.skipTest("POSIX mode bits do not apply on Windows")
        directory = Path(self.directory.name) / "outbox"
        self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        self.enqueue()
        self.assertEqual(stat.S_IMODE(self.store.path.stat().st_mode) & 0o077, 0)

    def test_a_symlinked_outbox_path_is_refused_rather_than_followed(self) -> None:
        if os.name == "nt":
            self.skipTest("symlink creation is not the Windows failure mode here")
        elsewhere = Path(self.directory.name) / "elsewhere"
        elsewhere.mkdir()
        link = Path(self.directory.name) / "linked"
        link.symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaisesRegex(ValidationError, "symlink"):
            OutboxStore(link)

    def test_the_three_identities_stay_separate(self) -> None:
        job = self.enqueue()
        self.assertTrue(job.job_id.startswith("job-"))
        # A queue identity exists before any machine is chosen, so it cannot be
        # the target-qualified execution id, and it is not a parent request.
        self.assertIsNone(job.request_id)
        self.assertIsNone(job.parent_request_id)

    def test_the_same_work_submitted_twice_is_one_job(self) -> None:
        first = self.enqueue()
        second = self.enqueue()
        self.assertEqual(first.job_id, second.job_id)
        self.assertEqual(len(self.store.jobs()), 1)

    def test_an_adopted_delegation_is_never_queued(self) -> None:
        with self.assertRaisesRegex(ValidationError, "delegation"):
            self.enqueue(intent={"repository": "example-repo", "delegation_id": "del-1"})

    def test_the_record_count_is_bounded(self) -> None:
        for index in range(3):
            self.enqueue(digest=f"{index}" * 64)
        with self.assertRaisesRegex(ValidationError, "full"):
            self.enqueue(digest="f" * 64)

    def test_an_oversized_prompt_or_intent_is_refused(self) -> None:
        with self.assertRaisesRegex(ValidationError, "prompt"):
            self.enqueue(prompt="x" * 2048)
        with self.assertRaisesRegex(ValidationError, "request"):
            self.enqueue(intent={"title": "y" * 2048})

    def test_a_claim_fences_every_later_write(self) -> None:
        job = self.enqueue()
        claimed = self.store.claim(job.job_id, "tick-a")
        assert claimed is not None
        _, generation = claimed
        # A second tick cannot take the same job while the lease is live.
        self.assertIsNone(self.store.claim(job.job_id, "tick-b"))
        with self.assertRaises(StaleLease):
            self.store.mark_dispatched(
                job.job_id, generation, "tick-b", request_id="r", host="h"
            )
        with self.assertRaises(StaleLease):
            self.store.mark_dispatched(
                job.job_id, generation - 1, "tick-a", request_id="r", host="h"
            )
        updated = self.store.mark_dispatched(
            job.job_id, generation, "tick-a", request_id="r", host="h"
        )
        self.assertEqual(updated.state, "dispatched")

    def test_a_stale_generation_refuses_after_the_lease_expires(self) -> None:
        job = self.enqueue()
        claimed = self.store.claim(job.job_id, "tick-a")
        assert claimed is not None
        _, first = claimed
        self.now[0] += 300
        taken = self.store.claim(job.job_id, "tick-b")
        assert taken is not None
        _, second = taken
        self.assertGreater(second, first)
        with self.assertRaises(StaleLease):
            self.store.mark_failed(
                job.job_id, first, "tick-a", disposition="failed-policy"
            )
        self.assertEqual(self.store.get(job.job_id).state, "queued")

    def test_a_claimed_job_stays_queued_so_a_crash_is_replayable(self) -> None:
        job = self.enqueue()
        claimed = self.store.claim(job.job_id, "tick-a")
        assert claimed is not None
        self.assertEqual(self.store.get(job.job_id).state, "queued")
        self.assertIsNotNone(self.store.payload(job.job_id))

    def test_dispatch_purges_the_payload_and_nothing_else(self) -> None:
        job = self.enqueue()
        claimed = self.store.claim(job.job_id, "tick-a")
        assert claimed is not None
        _, generation = claimed
        updated = self.store.mark_dispatched(
            job.job_id, generation, "tick-a", request_id="req-1", host="linux-worker"
        )
        self.assertIsNone(self.store.payload(job.job_id))
        self.assertTrue(updated.payload_purged)
        # The record survives the purge: identity and disposition are what an
        # operator needs afterwards, and neither is the prompt.
        self.assertEqual(updated.request_id, "req-1")
        self.assertEqual(updated.execution_state, "dispatched")

    def test_a_held_job_keeps_its_payload(self) -> None:
        job = self.enqueue()
        claimed = self.store.claim(job.job_id, "tick-a")
        assert claimed is not None
        _, generation = claimed
        self.store.mark_held(
            job.job_id, generation, "tick-a", request_id="req-1", host="linux-worker"
        )
        self.assertIsNotNone(self.store.payload(job.job_id))

    def test_retention_expires_a_queued_job_without_claiming_it_stopped(self) -> None:
        job = self.enqueue()
        self.now[0] += 7200
        self.store.purge_expired()
        updated = self.store.get(job.job_id)
        self.assertEqual(updated.state, "expired")
        self.assertEqual(updated.disposition, "expired-retention")
        self.assertTrue(updated.payload_purged)
        # The one thing an expiry may never be read as. This job never left the
        # queue, so that is what it says, rather than anything about a run.
        self.assertEqual(updated.execution_state, "never-dispatched")

    def test_retention_never_rewrites_a_held_job_as_stopped(self) -> None:
        job = self.enqueue()
        claimed = self.store.claim(job.job_id, "tick-a")
        assert claimed is not None
        _, generation = claimed
        self.store.mark_held(
            job.job_id, generation, "tick-a", request_id="req-1", host="linux-worker"
        )
        self.now[0] += 7200
        self.store.purge_expired()
        updated = self.store.get(job.job_id)
        # Only the payload goes. The record stays held and stays honest about
        # not knowing whether the execution is still running.
        self.assertEqual(updated.state, "held")
        self.assertEqual(updated.execution_state, "unknown")
        self.assertTrue(updated.payload_purged)
        self.assertIsNone(self.store.payload(job.job_id))

    def test_the_attempt_budget_is_bounded_and_backs_off(self) -> None:
        job = self.enqueue()
        delays = []
        for _ in range(3):
            claimed = self.store.claim(job.job_id, "tick-a")
            if claimed is None:
                break
            _, generation = claimed
            updated = self.store.reschedule(job.job_id, generation, "tick-a")
            delays.append(updated.next_attempt_at - self.now[0])
            self.now[0] = updated.next_attempt_at
        self.assertEqual(delays[:2], [60.0, 120.0])
        self.assertEqual(self.store.get(job.job_id).state, "failed")
        self.assertEqual(self.store.get(job.job_id).disposition, "failed-attempts")
        self.assertIsNone(self.store.payload(job.job_id))

    def test_only_one_tick_owns_the_drain_at_a_time(self) -> None:
        self.assertTrue(self.store.acquire_tick("tick-a"))
        self.assertFalse(self.store.acquire_tick("tick-b"))
        self.store.finish_tick("tick-a")
        self.assertTrue(self.store.acquire_tick("tick-b"))

    def test_the_tick_status_separates_a_pending_queue_from_a_drain(self) -> None:
        status = self.store.tick_status()
        self.assertIsNone(status["last_tick_at"])
        self.store.acquire_tick("tick-a")
        self.store.finish_tick("tick-a", failure_reason="host_unavailable")
        status = self.store.tick_status()
        self.assertEqual(status["last_tick_at"], self.now[0])
        self.assertEqual(status["last_failure_reason"], "host_unavailable")

    def test_a_redacted_job_carries_no_prompt_and_no_intent(self) -> None:
        job = self.enqueue(intent={"title": "an internal codename", "host": "auto"})
        rendered = json.dumps(job.redacted())
        self.assertNotIn(PROMPT, rendered)
        self.assertNotIn("internal codename", rendered)


class _OutboxControllerFixture:
    """An enrolled controller with the queue on, and a Hub that can be blocked."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = FleetStore(Path(self.directory.name) / "fleet.db")
        self.now = [1_000.0]
        self.serial = [0]
        self.outbox = OutboxStore(
            Path(self.directory.name) / "outbox",
            limits=OutboxLimits(
                max_records=4,
                max_attempts=3,
                backoff_seconds=60.0,
                max_backoff_seconds=600.0,
                max_jobs_per_tick=2,
                max_tick_seconds=60.0,
                retention_seconds=3600.0,
                lease_seconds=120.0,
            ),
            clock=lambda: self.now[0],
            uuid_factory=self._mint,
        )
        self.transport = HostRecordingTransport()
        self.admission = FakeAdmission()
        self.policy = FakePolicy()
        self.config = outbox_config(enrolled())
        self.controller = self.build()
        self.request = {
            "request_id": "req-12345678",
            "host": "auto",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "runtime_mode": "full-access",
            "interaction_mode": "default",
            "allow_queue": True,
        }

    def _mint(self) -> str:
        self.serial[0] += 1
        return f"{self.serial[0]:032x}"

    def build(self, *, config: FleetConfig | None = None) -> FleetController:
        return FleetController(
            config if config is not None else self.config,
            self.store,
            self.transport,
            admission=self.admission,
            policy=self.policy,
            outbox=self.outbox,
            wall_clock=lambda: POLICY_WALL_CLOCK,
            monotonic=lambda: self.now[0],
        )

    def block(self, error: Exception | None = None) -> None:
        self.policy.route_error = error or HostUnavailable(
            "no eligible machine", reason="capacity-exhausted"
        )

    def unblock(self) -> None:
        self.policy.route_error = None


class OutboxSubmitTests(_OutboxControllerFixture, unittest.TestCase):
    def test_a_blocked_submit_is_queued_when_the_caller_allowed_it(self) -> None:
        self.block()
        result = self.controller.submit(self.request, prompt=PROMPT)
        self.assertEqual(result["state"], "pending_autotick")
        self.assertEqual(result["queue_disposition"], "queued-blocked")
        self.assertEqual(result["queue_reason"], "capacity-exhausted")
        self.assertEqual(result["execution_state"], "never-dispatched")
        self.assertEqual(len(self.outbox.jobs(state="queued")), 1)

    def test_a_blocked_submit_without_the_flag_still_fails(self) -> None:
        self.block()
        request = {key: value for key, value in self.request.items() if key != "allow_queue"}
        with self.assertRaises(HostUnavailable):
            self.controller.submit(request, prompt=PROMPT)
        self.assertEqual(self.outbox.jobs(), [])

    def test_an_explicit_refusal_to_queue_is_honoured(self) -> None:
        self.block()
        with self.assertRaises(HostUnavailable):
            self.controller.submit({**self.request, "allow_queue": False}, prompt=PROMPT)
        self.assertEqual(self.outbox.jobs(), [])

    def test_a_disabled_queue_rejects_the_request_outright(self) -> None:
        controller = self.build(config=outbox_config(enrolled(), enabled=False))
        with self.assertRaisesRegex(ValidationError, "disabled"):
            controller.submit(self.request, prompt=PROMPT)
        self.assertEqual(self.outbox.jobs(), [])

    def test_a_disabled_queue_leaves_legacy_behaviour_untouched(self) -> None:
        """Enrollment off with no queue flag is exactly the old contract."""
        controller = self.build(
            config=replace(
                outbox_config(two_host_config(), enabled=False),
                policy=PolicyConfig(enrollment="off"),
            )
        )
        request = {
            key: value for key, value in self.request.items() if key != "allow_queue"
        }
        result = controller.submit(
            {**request, "host": "linux-worker", "seat": "coder"}, prompt=PROMPT
        )
        self.assertEqual(result["state"], "running")

    def test_an_adopted_delegation_cannot_be_queued(self) -> None:
        with self.assertRaisesRegex(ValidationError, "delegation"):
            self.controller.submit(
                {**self.request, "delegation_id": "del-8f2c1a"}, prompt=PROMPT
            )
        self.assertEqual(self.outbox.jobs(), [])

    def test_a_successful_submit_never_touches_the_queue(self) -> None:
        result = self.controller.submit(self.request, prompt=PROMPT)
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.outbox.jobs(), [])

    def test_an_unknown_outcome_is_never_queued_for_a_second_run(self) -> None:
        """A lost transport after a mutation is not a pre-launch block."""
        self.transport.fail_on = "submit"
        with self.assertRaises(UnknownState):
            self.controller.submit(self.request, prompt=PROMPT)
        self.assertEqual(self.outbox.jobs(), [])

    def test_the_queued_result_carries_no_prompt(self) -> None:
        self.block()
        result = self.controller.submit(self.request, prompt=PROMPT)
        self.assertNotIn(PROMPT, json.dumps(result))


class OutboxTickTests(_OutboxControllerFixture, unittest.TestCase):
    def queue(self, **overrides: object) -> str:
        self.block()
        result = self.controller.submit({**self.request, **overrides}, prompt=PROMPT)
        self.unblock()
        return result["job_id"]

    def test_a_capacity_blocked_job_launches_once_when_capacity_frees(self) -> None:
        """The whole point: no second user submit, and exactly one execution."""
        job_id = self.queue()
        result = self.controller.tick()
        self.assertEqual(result["examined"], 1)
        self.assertEqual(len(result["dispatched"]), 1)
        self.assertEqual(result["dispatched"][0]["job_id"], job_id)
        self.assertEqual(self.outbox.get(job_id).state, "dispatched")
        submits = [call for call in self.transport.calls if call["action"] == "submit"]
        self.assertEqual(len(submits), 1)
        self.assertEqual(submits[0]["prompt"], PROMPT)

    def test_a_second_tick_does_not_dispatch_the_same_job_again(self) -> None:
        self.queue()
        self.controller.tick()
        again = self.controller.tick()
        self.assertEqual(again["examined"], 0)
        submits = [call for call in self.transport.calls if call["action"] == "submit"]
        self.assertEqual(len(submits), 1)

    def test_a_concurrent_tick_reports_busy_rather_than_racing(self) -> None:
        self.queue()
        self.assertTrue(self.outbox.acquire_tick("other-tick"))
        result = self.controller.tick()
        self.assertEqual(result["state"], "busy")
        self.assertEqual(result["examined"], 0)
        self.assertEqual(
            [call for call in self.transport.calls if call["action"] == "submit"], []
        )

    def test_a_restarted_tick_replays_the_same_execution_identity(self) -> None:
        """A crash mid-dispatch must not mint a second request id."""
        job_id = self.queue()
        self.transport.fail_on = "submit"
        first = self.controller.tick()
        self.assertEqual(len(first["held"]), 1)
        held = self.outbox.get(job_id)
        self.assertEqual(held.state, "held")
        self.assertEqual(held.execution_state, "unknown")
        self.assertEqual(held.request_id, "req-12345678")
        # Held work is not picked up again, so no second execution is started.
        self.transport.fail_on = None
        self.assertEqual(self.controller.tick()["examined"], 0)

    def test_a_retry_replans_instead_of_replaying_a_failed_machine(self) -> None:
        job_id = self.queue()
        self.policy.machine = "linux-worker"
        self.controller.tick()
        self.assertEqual(self.outbox.get(job_id).host, "linux-worker")
        # The Hub was asked again rather than the first selection being reused.
        self.assertGreaterEqual(len(self.policy.route_calls), 2)

    def test_a_still_blocked_job_backs_off_and_stays_queued(self) -> None:
        job_id = self.queue()
        self.block()
        result = self.controller.tick()
        self.assertEqual(len(result["blocked"]), 1)
        self.assertEqual(result["blocked"][0]["queue_reason"], "capacity-exhausted")
        job = self.outbox.get(job_id)
        self.assertEqual(job.state, "queued")
        self.assertEqual(job.disposition, "queued-retry")
        self.assertGreater(job.next_attempt_at, self.now[0])
        # Backing off means the next tick does not immediately retry it.
        self.assertEqual(self.controller.tick()["examined"], 0)

    def test_a_policy_refusal_is_terminal_rather_than_retried(self) -> None:
        job_id = self.queue()
        self.policy.route_error = PolicyRejected(
            "seat is not available to this consumer", reason="policy-seat-mismatch"
        )
        result = self.controller.tick()
        self.assertEqual(len(result["blocked"]), 1)
        self.assertEqual(result["blocked"][0]["queue_reason"], "policy-seat-mismatch")
        job = self.outbox.get(job_id)
        self.assertEqual(job.state, "failed")
        self.assertEqual(job.disposition, "failed-policy")
        self.assertIsNone(self.outbox.payload(job_id))

    def test_a_conflicting_identity_is_terminal(self) -> None:
        job_id = self.queue()
        self.policy.route_error = IdempotencyConflict("different payload digest")
        result = self.controller.tick()
        self.assertEqual(self.outbox.get(job_id).disposition, "failed-conflict")
        self.assertEqual(len(result["blocked"]), 1)

    def test_a_job_whose_configuration_moved_is_terminal(self) -> None:
        job_id = self.queue()
        self.policy.route_error = ValidationError("repository is not allowlisted")
        self.controller.tick()
        self.assertEqual(self.outbox.get(job_id).disposition, "failed-invalid")

    def test_the_drain_is_bounded_per_tick(self) -> None:
        for index in range(3):
            self.queue(request_id=f"req-1234567{index}", title=f"Fix issue {index}")
        result = self.controller.tick()
        self.assertEqual(result["examined"], 2)
        # Exactly one job was never looked at this tick. The other two were both
        # drained, whether that ended in a dispatch or in a repository that was
        # already busy with the first of them.
        untouched = [job for job in self.outbox.jobs() if job.attempts == 0]
        self.assertEqual(len(untouched), 1)
        self.assertEqual(untouched[0].state, "queued")

    def test_retention_expires_a_stale_job_before_it_is_drained(self) -> None:
        job_id = self.queue()
        self.outbox._clock = lambda: 100_000.0  # past the retention window
        result = self.controller.tick()
        self.assertEqual(result["examined"], 0)
        self.assertEqual(len(result["expired"]), 1)
        self.assertEqual(result["expired"][0]["job_id"], job_id)
        self.assertEqual(
            result["expired"][0]["execution_state"], "never-dispatched"
        )
        self.assertEqual(
            [call for call in self.transport.calls if call["action"] == "submit"], []
        )

    def test_the_tick_result_never_carries_the_prompt(self) -> None:
        self.queue()
        rendered = json.dumps(self.controller.tick())
        self.assertNotIn(PROMPT, rendered)
        self.assertNotIn("Fix issue 123", rendered)

    def test_the_tick_result_exposes_the_last_drain(self) -> None:
        result = self.controller.tick()
        self.assertEqual(result["state"], "eligible")
        self.assertIsNotNone(result["last_tick_at"])
        self.assertIsNone(result["last_failure_reason"])
        self.assertIn("counts", result)

    def test_a_tick_on_a_disabled_queue_is_refused(self) -> None:
        controller = self.build(config=outbox_config(enrolled(), enabled=False))
        with self.assertRaisesRegex(ValidationError, "disabled"):
            controller.tick()


class TickReservationTests(_OutboxControllerFixture, unittest.TestCase):
    """`tick` renews what this machine owns as a target, and only that."""

    class FakeTargetStore:
        def __init__(self, receipts: list[object]) -> None:
            self.receipts = receipts
            self.calls: list[str] = []

        def active_reservations(self, *, target_id: str) -> list[object]:
            self.calls.append(target_id)
            return [
                receipt
                for receipt in self.receipts
                if receipt.target_id == target_id
            ]

    class FakeReceipt:
        def __init__(
            self,
            request_id: str,
            target_id: str,
            state: str = "running",
            policy: dict | None = None,
        ) -> None:
            self.request_id = request_id
            self.target_id = target_id
            self.state = state
            self.reservation_state = "active"
            self.policy = policy or {
                "reservation_id": "reservation-1",
                "decision_id": "decision-1",
                "session_id": "t3f-" + "0" * 32,
            }

    class RenewingPolicy(FakePolicy):
        def __init__(self) -> None:
            super().__init__()
            self.renewals: list[dict] = []
            self.renew_reply: dict | Exception = {"state": "active"}

        def renew_reservation(self, **kwargs: object) -> dict:
            self.renewals.append(dict(kwargs))
            if isinstance(self.renew_reply, Exception):
                raise self.renew_reply
            return dict(self.renew_reply)

    def controller_with(self, target_store: object) -> FleetController:
        self.policy = self.RenewingPolicy()
        return FleetController(
            self.config,
            self.store,
            self.transport,
            admission=self.admission,
            policy=self.policy,
            outbox=self.outbox,
            target_store=target_store,
            wall_clock=lambda: POLICY_WALL_CLOCK,
            monotonic=lambda: self.now[0],
        )

    def test_a_target_owned_reservation_is_renewed_without_an_origin_waiting(
        self,
    ) -> None:
        target_store = self.FakeTargetStore(
            [self.FakeReceipt("linux-worker-controller-a-1", "linux-worker")]
        )
        controller = self.controller_with(target_store)
        result = controller.tick()
        self.assertEqual(target_store.calls, ["linux-worker"])
        self.assertEqual(len(self.policy.renewals), 1)
        self.assertEqual(result["reservations"][0]["renewed"], True)

    def test_another_targets_reservation_is_never_renewed_from_here(self) -> None:
        target_store = self.FakeTargetStore(
            [self.FakeReceipt("second-worker-controller-a-1", "second-worker")]
        )
        controller = self.controller_with(target_store)
        result = controller.tick()
        self.assertEqual(result["reservations"], [])
        self.assertEqual(self.policy.renewals, [])

    def test_an_unknown_execution_stays_counted_and_is_never_released(self) -> None:
        target_store = self.FakeTargetStore(
            [self.FakeReceipt("linux-worker-controller-a-1", "linux-worker", "unknown")]
        )
        controller = self.controller_with(target_store)
        self.policy.renew_reply = HostUnavailable("policy helper is unreachable")
        result = controller.tick()
        entry = result["reservations"][0]
        self.assertFalse(entry["renewed"])
        self.assertEqual(entry["reservation_state"], "active")

    def test_a_refused_renewal_never_forces_a_release(self) -> None:
        target_store = self.FakeTargetStore(
            [self.FakeReceipt("linux-worker-controller-a-1", "linux-worker")]
        )
        controller = self.controller_with(target_store)
        self.policy.renew_reply = PolicyRejected("reservation is gone")
        result = controller.tick()
        self.assertFalse(result["reservations"][0]["renewed"])

    def test_a_source_without_a_local_target_renews_nothing(self) -> None:
        result = self.build().tick()
        self.assertEqual(result["reservations"], [])


if __name__ == "__main__":
    unittest.main()


class OutboxAttemptAccountingTests(_OutboxControllerFixture, unittest.TestCase):
    """One drain is one attempt, wherever inside the dispatch it stopped."""

    def test_a_block_before_any_identity_still_counts_as_an_attempt(self) -> None:
        """A route that never reaches a request id must still burn budget.

        Counting at the point a dispatch names an execution identity would let
        a job that fails earlier than that retry forever.
        """
        self.block()
        job_id = self.controller.submit(self.request, prompt=PROMPT)["job_id"]
        self.controller.tick()
        self.assertEqual(self.outbox.get(job_id).attempts, 1)

    def test_a_drain_that_reaches_a_target_counts_exactly_one_attempt(self) -> None:
        self.block()
        job_id = self.controller.submit(self.request, prompt=PROMPT)["job_id"]
        self.unblock()
        # Blocked late: the dispatch names an identity and then finds the host
        # unusable, so both the claim and the identity write happen.
        self.transport.fail_on = "doctor"
        self.controller.tick()
        job = self.outbox.get(job_id)
        self.assertEqual(job.attempts, 1)
        self.assertEqual(job.state, "queued")

    def test_the_budget_runs_out_after_the_configured_number_of_drains(self) -> None:
        self.block()
        job_id = self.controller.submit(self.request, prompt=PROMPT)["job_id"]
        for _ in range(self.outbox.limits.max_attempts):
            job = self.outbox.get(job_id)
            if job.state != "queued":
                break
            self.outbox._clock = lambda at=job.next_attempt_at: at
            self.controller.tick()
        job = self.outbox.get(job_id)
        self.assertEqual(job.state, "failed")
        self.assertEqual(job.disposition, "failed-attempts")
        self.assertEqual(job.attempts, self.outbox.limits.max_attempts)
        self.assertIsNone(self.outbox.payload(job_id))


class PeerModeOutboxTests(_OutboxControllerFixture, unittest.TestCase):
    """In peer mode the execution identity is minted, so a replay must reuse it."""

    def setUp(self) -> None:
        super().setUp()
        from test_controller import two_host_peer_config

        self.config = outbox_config(enrolled(two_host_peer_config()))
        self.controller = self.build()
        # Peer mode mints the qualified id, so the caller supplies none.
        self.request = {
            key: value
            for key, value in self.request.items()
            if key != "request_id"
        }

    def test_a_queued_peer_submit_stores_no_minted_identity(self) -> None:
        self.block()
        result = self.controller.submit(self.request, prompt=PROMPT)
        job = self.outbox.get(result["job_id"])
        self.assertIsNone(job.request_id)
        payload = self.outbox.payload(job.job_id)
        assert payload is not None
        self.assertNotIn("request_id", payload.intent)

    def test_a_replay_reuses_the_identity_the_first_attempt_named(self) -> None:
        self.block()
        job_id = self.controller.submit(self.request, prompt=PROMPT)["job_id"]
        self.unblock()
        # First drain names an identity, then loses the host before dispatch.
        self.transport.fail_on = "doctor"
        self.controller.tick()
        first = self.outbox.get(job_id).request_id
        self.assertIsNotNone(first)
        self.transport.fail_on = None
        self.outbox._clock = lambda at=self.outbox.get(job_id).next_attempt_at: at
        self.controller.tick()
        job = self.outbox.get(job_id)
        self.assertEqual(job.state, "dispatched")
        # The same execution identity, not a second one.
        self.assertEqual(job.request_id, first)
        submits = [call for call in self.transport.calls if call["action"] == "claim"]
        self.assertEqual({call["request_id"] for call in submits}, {first})

    def test_a_replay_reuses_the_live_reservation_rather_than_routing_again(
        self,
    ) -> None:
        """The reservation is what keeps a replay on one machine.

        A job that already named an execution identity is replayed against the
        decision and reservation that identity was taken under, so the Hub is
        never asked to place the same work a second time while that reservation
        is live.
        """
        self.block()
        job_id = self.controller.submit(self.request, prompt=PROMPT)["job_id"]
        self.unblock()
        self.transport.fail_on = "doctor"
        self.controller.tick()
        named = self.outbox.get(job_id).request_id
        self.assertTrue(named.startswith("second-worker-"))
        self.transport.fail_on = None
        # Even asked to prefer another machine, the replay stays where its live
        # reservation already is.
        self.policy.machine = "linux-worker"
        self.outbox._clock = lambda at=self.outbox.get(job_id).next_attempt_at: at
        self.controller.tick()
        job = self.outbox.get(job_id)
        self.assertEqual(job.state, "dispatched")
        self.assertEqual(job.request_id, named)
        claims = [call for call in self.transport.calls if call["action"] == "claim"]
        self.assertEqual({call["request_id"] for call in claims}, {named})

    def test_a_replay_onto_another_machine_is_unknown_not_a_fresh_dispatch(
        self,
    ) -> None:
        """The guard behind that reuse, exercised directly.

        A target-qualified identity belongs to one machine. If a replay ever did
        plan onto a different one, the previous attempt could not be reconciled
        from here and could not be proved never to have started, so it is an
        unknown outcome rather than a request to dispatch afresh.
        """
        self.policy.machine = "linux-worker"
        stale = "second-worker-controller-a-" + "0" * 8 + "-0000-0000-0000-" + "0" * 12
        with self.assertRaises(UnknownState) as raised:
            self.controller._dispatch_submit(
                dict(self.request), prompt=PROMPT, resume_request_id=stale
            )
        self.assertEqual(raised.exception.request_id, stale)
        self.assertEqual(
            [call for call in self.transport.calls if call["action"] == "claim"], []
        )
