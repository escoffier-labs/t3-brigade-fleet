from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from t3_fleet.errors import (
    HostUnavailable,
    IdempotencyConflict,
    PolicyRejected,
    ValidationError,
)
from t3_fleet.store import FleetStore
from t3_fleet.transport import normalize_admission


def _admission(**overrides: object) -> dict[str, object]:
    admission: dict[str, object] = {
        "roster_revision": 42,
        "roster_digest": "sha256:" + "a" * 64,
        "seat": "coder",
        "provider": "openai",
        "model": "gpt-5.6-terra",
        "reasoning": "high",
        "provider_instance_id": "codex",
        "service_tier": "priority",
        "source": "hub",
        "expires_at": "2099-08-30T14:00:00Z",
    }
    admission.update(overrides)
    return admission


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = FleetStore(Path(self.directory.name) / "fleet.db")

    def test_same_id_and_digest_is_idempotent(self) -> None:
        first = self.store.create_request(
            "req-12345678",
            "digest-a",
            "linux-worker",
            "example-repo",
            prompt_bytes=12,
        )
        second = self.store.create_request(
            "req-12345678",
            "digest-a",
            "linux-worker",
            "example-repo",
            prompt_bytes=12,
        )
        self.assertEqual(first, second)

    def test_same_id_with_new_digest_is_conflict(self) -> None:
        self.store.create_request("req-12345678", "digest-a", "linux-worker", "example-repo")
        with self.assertRaises(IdempotencyConflict):
            self.store.create_request("req-12345678", "digest-b", "linux-worker", "example-repo")

    def test_create_request_rejects_admission_presence_mismatch_on_retry(self) -> None:
        first = self.store.create_request(
            "req-12345678",
            "digest-a",
            "linux-worker",
            "example-repo",
        )
        self.assertIsNone(first.admission)
        with self.assertRaises(IdempotencyConflict):
            self.store.create_request(
                "req-12345678",
                "digest-a",
                "linux-worker",
                "example-repo",
                admission=_admission(),
            )
        self.assertIsNone(self.store.get("req-12345678").admission)

        admitted = self.store.create_request(
            "req-87654321",
            "digest-b",
            "linux-worker",
            "example-repo",
            admission=_admission(),
        )
        self.assertEqual(admitted.admission, normalize_admission(_admission()))
        with self.assertRaises(IdempotencyConflict):
            self.store.create_request(
                "req-87654321",
                "digest-b",
                "linux-worker",
                "example-repo",
            )
        self.assertEqual(
            self.store.get("req-87654321").admission,
            normalize_admission(_admission()),
        )

    def test_create_request_matching_admission_retry_is_idempotent(self) -> None:
        first = self.store.create_request(
            "req-12345678",
            "digest-a",
            "linux-worker",
            "example-repo",
            admission=_admission(),
        )
        second = self.store.create_request(
            "req-12345678",
            "digest-a",
            "linux-worker",
            "example-repo",
            admission=_admission(expires_at="2099-08-30T14:05:00Z"),
        )
        self.assertEqual(first.request_id, second.request_id)
        self.assertEqual(first.admission, second.admission)

    def test_create_request_rejects_malformed_model_admission_before_write(self) -> None:
        with self.assertRaisesRegex(ValidationError, "model admission"):
            self.store.create_request(
                "req-12345678",
                "digest-a",
                "linux-worker",
                "example-repo",
                admission={"model": "gpt-5.6-terra"},
            )
        with self.assertRaisesRegex(ValidationError, "unknown request id"):
            self.store.get("req-12345678")

    def test_disconnect_after_dispatch_becomes_unknown(self) -> None:
        self.store.create_request("req-12345678", "digest-a", "linux-worker", "example-repo")
        self.store.transition("req-12345678", "preflight")
        self.store.transition("req-12345678", "dispatching")
        self.store.mark_transport_failure("req-12345678")
        self.assertEqual(self.store.get("req-12345678").state, "unknown")

    def test_disconnect_during_preflight_is_blocked_offline(self) -> None:
        self.store.create_request("req-12345678", "digest-a", "linux-worker", "example-repo")
        self.store.transition("req-12345678", "preflight")
        self.store.mark_transport_failure("req-12345678")
        self.assertEqual(self.store.get("req-12345678").state, "blocked_offline")

    def test_invalid_transition_is_rejected(self) -> None:
        self.store.create_request("req-12345678", "digest-a", "linux-worker", "example-repo")
        with self.assertRaisesRegex(ValidationError, "queued -> succeeded"):
            self.store.transition("req-12345678", "succeeded")

    def test_lease_blocks_another_request(self) -> None:
        self.assertTrue(self.store.acquire_lease("linux-worker", "example-repo", "req-a", ttl=900))
        self.assertFalse(self.store.acquire_lease("linux-worker", "example-repo", "req-b", ttl=900))
        self.store.release_lease("linux-worker", "example-repo", "req-a")
        self.assertTrue(self.store.acquire_lease("linux-worker", "example-repo", "req-b", ttl=900))

    def test_three_host_failures_open_circuit(self) -> None:
        self.assertFalse(self.store.record_host_failure("linux-worker"))
        self.assertFalse(self.store.record_host_failure("linux-worker"))
        self.assertTrue(self.store.record_host_failure("linux-worker"))
        self.assertTrue(self.store.circuit_is_open("linux-worker"))
        self.store.record_host_success("linux-worker")
        self.assertFalse(self.store.circuit_is_open("linux-worker"))

    def test_prompt_text_is_not_stored(self) -> None:
        self.store.create_request(
            "req-12345678",
            "digest-a",
            "linux-worker",
            "example-repo",
            prompt_bytes=24,
        )
        database_bytes = Path(self.store.path).read_bytes()
        self.assertNotIn(b"private prompt text", database_bytes)

    def test_import_receipt_upserts_authoritative_target_state(self) -> None:
        record = self.store.import_receipt(
            {
                "request_id": "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc",
                "target_id": "linux-worker",
                "repository": "example-repo",
                "request_digest": "a" * 64,
                "source_revision": "0123456789abcdef0123456789abcdef01234567",
                "state": "running",
                "thread_id": "thread-1",
                "prompt_sha256": "b" * 64,
                "prompt_bytes": 12,
                "created_at": "2026-07-28T01:00:00Z",
                "updated_at": "2026-07-28T01:01:00Z",
            }
        )
        self.assertEqual(record.state, "running")
        self.assertEqual(record.thread_id, "thread-1")

    def test_import_prelaunch_failure_preserves_plan_without_authorizing_launch(self) -> None:
        plan = dict(_admission(), state="planned", launch_model="gpt-5.6-terra",
                    launch_model_explicit=False)
        self.store.create_request("req-12345678", "a" * 64, "linux-worker",
                                  "example-repo", plan=plan)
        receipt = {
            "request_id": "req-12345678", "target_id": "linux-worker",
            "repository": "example-repo", "request_digest": "a" * 64,
            "state": "failed", "thread_id": "thread-1", "prompt_bytes": 12,
            "model_plan": plan,
            "created_at": "2026-07-28T01:00:00Z",
            "updated_at": "2026-07-28T01:01:00Z",
        }
        record = self.store.import_receipt(receipt)
        self.assertEqual(record.state, "failed")
        self.assertIsNone(record.admission)
        self.assertEqual(record.plan, plan)
        # A matching plan never authorizes a live or completed execution.
        for state in ("running", "needs_attention", "succeeded", "unknown"):
            with self.subTest(state=state), self.assertRaises(PolicyRejected):
                self.store.import_receipt(dict(receipt, state=state))
        # Even a failure must prove it came from the same planned selection.
        for bad in (None, dict(plan, seat="other")):
            with self.subTest(plan=bad), self.assertRaises(PolicyRejected):
                self.store.import_receipt(dict(receipt, model_plan=bad))
        self.assertEqual(self.store.get("req-12345678").state, "failed")

    def test_import_prelaunch_failure_rejects_binding_drift(self) -> None:
        plan = dict(
            _admission(), state="planned", launch_model="gpt-5.6-terra",
            launch_model_explicit=False,
        )
        for field, value, request_id in (
            ("target_id", "other-worker", "req-12345678"),
            ("repository", "other-repo", "req-87654321"),
        ):
            with self.subTest(field=field):
                self.store.create_request(
                    request_id, "a" * 64, "linux-worker", "example-repo", plan=plan,
                )
                self.store.transition(request_id, "preflight")
                self.store.transition(request_id, "dispatching")
                self.store.mark_transport_failure(request_id)
                before = self.store.get(request_id)
                self.assertEqual(before.state, "unknown")
                receipt = {
                    "request_id": request_id, "target_id": "linux-worker",
                    "repository": "example-repo", "request_digest": "a" * 64,
                    "state": "failed", "thread_id": "thread-1", "prompt_bytes": 12,
                    "model_plan": plan,
                    "created_at": "2026-07-28T01:00:00Z",
                    "updated_at": "2026-07-28T01:01:00Z",
                    field: value,
                }
                with self.assertRaises(PolicyRejected):
                    self.store.import_receipt(receipt)
                self.assertEqual(self.store.get(request_id), before)

    def test_import_prelaunch_failure_cannot_erase_running_admission(self) -> None:
        admission = _admission()
        plan = dict(
            admission, state="planned", launch_model="gpt-5.6-terra",
            launch_model_explicit=False,
        )
        request_id = "req-12345678"
        self.store.create_request(
            request_id, "a" * 64, "linux-worker", "example-repo",
            admission=admission, plan=plan,
        )
        self.store.transition(request_id, "preflight")
        self.store.transition(request_id, "dispatching")
        self.store.transition(request_id, "running", thread_id="thread-running")
        before = self.store.get(request_id)
        with self.assertRaisesRegex(PolicyRejected, "controller admission"):
            self.store.import_receipt({
                "request_id": request_id, "target_id": "linux-worker",
                "repository": "example-repo", "request_digest": "a" * 64,
                "state": "failed", "thread_id": "thread-1", "prompt_bytes": 12,
                "model_plan": plan,
                "created_at": "2026-07-28T01:00:00Z",
                "updated_at": "2026-07-28T01:01:00Z",
            })
        after = self.store.get(request_id)
        self.assertEqual(after.state, "running")
        self.assertEqual(after.admission, normalize_admission(admission))
        self.assertEqual(after, before)

    def test_import_receipt_rejects_malformed_model_admission(self) -> None:
        with self.assertRaisesRegex(ValidationError, "model admission"):
            self.store.import_receipt(
                {
                    "request_id": "req-12345678",
                    "target_id": "linux-worker",
                    "repository": "example-repo",
                    "request_digest": "a" * 64,
                    "source_revision": None,
                    "state": "running",
                    "thread_id": "thread-1",
                    "prompt_sha256": "b" * 64,
                    "prompt_bytes": 12,
                    "model_admission": {"model": "gpt-5.6-terra"},
                    "created_at": "2026-07-28T01:00:00Z",
                    "updated_at": "2026-07-28T01:01:00Z",
                }
            )

    def test_import_receipt_rejects_target_admission_drift(self) -> None:
        admission = {
            "roster_revision": 42,
            "roster_digest": "sha256:" + "a" * 64,
            "seat": "coder",
            "provider": "openai",
            "model": "gpt-5.6-terra",
            "reasoning": "high",
            "provider_instance_id": "codex",
            "service_tier": "priority",
            "source": "hub",
            "expires_at": "2099-08-30T14:00:00Z",
        }
        self.store.create_request(
            "req-12345678",
            "a" * 64,
            "linux-worker",
            "example-repo",
            admission=admission,
        )
        forged = dict(admission, model="gpt-5.6-luna")
        with self.assertRaises(PolicyRejected):
            self.store.import_receipt(
                {
                    "request_id": "req-12345678",
                    "target_id": "linux-worker",
                    "repository": "example-repo",
                    "request_digest": "a" * 64,
                    "source_revision": None,
                    "state": "running",
                    "thread_id": "thread-1",
                    "prompt_sha256": "b" * 64,
                    "prompt_bytes": 12,
                    "model_admission": forged,
                    "created_at": "2026-07-28T01:00:00Z",
                    "updated_at": "2026-07-28T01:01:00Z",
                }
            )

    def test_import_rejects_receipt_that_conflicts_with_local_digest(self) -> None:
        self.store.create_request("req-12345678", "digest-a", "linux-worker", "example-repo")
        with self.assertRaises(IdempotencyConflict):
            self.store.import_receipt({
                "request_id": "req-12345678", "target_id": "linux-worker",
                "repository": "example-repo", "request_digest": "digest-b",
                "source_revision": None, "state": "running", "thread_id": "thread-1",
                "prompt_sha256": "a" * 64, "prompt_bytes": 0,
                "created_at": "2026-07-28T01:00:00Z", "updated_at": "2026-07-28T01:00:00Z",
            })

    def test_unknown_requests_for_host_lists_only_unknown_ids(self) -> None:
        self.store.create_request("req-11111111", "digest-a", "linux-worker", "example-repo")
        self.store.transition("req-11111111", "preflight")
        self.store.transition("req-11111111", "dispatching")
        self.store.mark_transport_failure("req-11111111")
        self.assertEqual(
            self.store.unknown_requests_for_host("linux-worker"),
            ["req-11111111"],
        )

    def test_unknown_requests_for_host_excludes_other_hosts_and_states(self) -> None:
        self.store.create_request("req-11111111", "digest-a", "linux-worker", "example-repo")
        self.store.transition("req-11111111", "preflight")
        self.store.transition("req-11111111", "dispatching")
        self.store.mark_transport_failure("req-11111111")

        self.store.create_request("req-22222222", "digest-b", "linux-worker", "example-repo")
        self.store.transition("req-22222222", "preflight")
        self.store.transition("req-22222222", "dispatching")
        self.store.transition("req-22222222", "running", thread_id="thread-2")

        self.store.create_request("req-33333333", "digest-c", "second-worker", "example-repo")
        self.store.transition("req-33333333", "preflight")
        self.store.transition("req-33333333", "dispatching")
        self.store.mark_transport_failure("req-33333333")

        self.assertEqual(
            self.store.unknown_requests_for_host("linux-worker"),
            ["req-11111111"],
        )
        self.assertEqual(
            self.store.unknown_requests_for_host("second-worker"),
            ["req-33333333"],
        )
        self.assertEqual(
            self.store.unknown_requests_for_host("absent-worker"),
            [],
        )

    def test_followup_capacity_and_lease_reservation_is_atomic(self) -> None:
        for request_id, repository in (
            ("req-11111111", "repo-one"),
            ("req-22222222", "repo-two"),
        ):
            self.store.create_request(
                request_id,
                f"digest-{request_id}",
                "linux-worker",
                repository,
            )
            self.store.transition(request_id, "preflight")
            self.store.transition(request_id, "dispatching")
            self.store.transition(request_id, "running", thread_id=f"thread-{request_id}")
            self.store.transition(request_id, "succeeded")

        barrier = threading.Barrier(2)

        def reserve(request_id: str) -> str:
            barrier.wait()
            try:
                return self.store.reserve_followup(
                    request_id,
                    global_limit=1,
                    host_limit=1,
                    ttl=900,
                ).state
            except HostUnavailable:
                return "blocked"

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    reserve,
                    ("req-11111111", "req-22222222"),
                )
            )

        self.assertCountEqual(results, ["dispatching", "blocked"])
        states = {
            self.store.get("req-11111111").state,
            self.store.get("req-22222222").state,
        }
        self.assertEqual(states, {"dispatching", "succeeded"})


class DelegationBindingTests(unittest.TestCase):
    """One adoption is one execution, enforced by the durable store."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = FleetStore(Path(self.directory.name) / "fleet.db")

    def test_an_unbound_delegation_has_no_binding(self) -> None:
        self.assertIsNone(self.store.delegation_binding("del-8f2c1a"))

    def test_binding_is_idempotent_for_the_same_pair(self) -> None:
        for _ in range(2):
            self.assertEqual(
                self.store.record_delegation(
                    "del-8f2c1a",
                    request_id="worker-controller-a-1",
                    parent_request_id="run-7a1c9e",
                ),
                "worker-controller-a-1",
            )
        self.assertEqual(
            self.store.delegation_binding("del-8f2c1a"),
            ("worker-controller-a-1", "run-7a1c9e"),
        )

    def test_a_second_execution_for_one_delegation_conflicts(self) -> None:
        self.store.record_delegation(
            "del-8f2c1a",
            request_id="worker-controller-a-1",
            parent_request_id="run-7a1c9e",
        )
        with self.assertRaises(IdempotencyConflict):
            self.store.record_delegation(
                "del-8f2c1a",
                request_id="worker-controller-a-2",
                parent_request_id="run-7a1c9e",
            )

    def test_a_second_delegation_for_one_execution_conflicts(self) -> None:
        self.store.record_delegation(
            "del-8f2c1a",
            request_id="worker-controller-a-1",
            parent_request_id="run-7a1c9e",
        )
        with self.assertRaises(IdempotencyConflict):
            self.store.record_delegation(
                "del-other",
                request_id="worker-controller-a-1",
                parent_request_id="run-7a1c9e",
            )

    def test_a_changed_parent_request_id_conflicts(self) -> None:
        self.store.record_delegation(
            "del-8f2c1a",
            request_id="worker-controller-a-1",
            parent_request_id="run-7a1c9e",
        )
        with self.assertRaises(IdempotencyConflict):
            self.store.record_delegation(
                "del-8f2c1a",
                request_id="worker-controller-a-1",
                parent_request_id="run-other",
            )

    def test_concurrent_adoptions_of_one_delegation_bind_once(self) -> None:
        barrier = threading.Barrier(4)

        def bind(index: int) -> str | None:
            store = FleetStore(Path(self.directory.name) / "fleet.db")
            barrier.wait()
            try:
                return store.record_delegation(
                    "del-8f2c1a",
                    request_id=f"worker-controller-a-{index}",
                    parent_request_id="run-7a1c9e",
                )
            except IdempotencyConflict:
                return None

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(bind, range(4)))
        self.assertEqual(len([item for item in results if item is not None]), 1)


class RepositoryLeaseSlotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "fleet.db"
        self.now = [1000.0]
        self.store = FleetStore(self.path, clock=lambda: self.now[0])

    def acquire(self, request_id: str, *, limit: int, ttl: float = 900) -> bool:
        return self.store.acquire_lease(
            "linux-worker", "example-repo", request_id, ttl=ttl, limit=limit
        )

    def test_limit_n_allows_n_and_rejects_n_plus_one(self) -> None:
        self.assertTrue(self.acquire("req-a", limit=3))
        self.assertTrue(self.acquire("req-b", limit=3))
        self.assertTrue(self.acquire("req-c", limit=3))
        self.assertFalse(self.acquire("req-d", limit=3))
        # A holder can always refresh its own slot when the repository is full.
        self.assertTrue(self.acquire("req-b", limit=3))
        # Other repositories and hosts keep independent slots.
        self.assertTrue(
            self.store.acquire_lease("linux-worker", "other-repo", "req-d", ttl=900, limit=3)
        )
        self.assertTrue(
            self.store.acquire_lease("other-host", "example-repo", "req-e", ttl=900, limit=3)
        )

    def test_limit_one_keeps_single_slot_behavior(self) -> None:
        self.assertTrue(self.acquire("req-a", limit=1))
        self.assertFalse(self.acquire("req-b", limit=1))

    def test_release_frees_one_slot(self) -> None:
        self.assertTrue(self.acquire("req-a", limit=2))
        self.assertTrue(self.acquire("req-b", limit=2))
        self.assertFalse(self.acquire("req-c", limit=2))
        self.store.release_lease("linux-worker", "example-repo", "req-a")
        self.assertTrue(self.acquire("req-c", limit=2))

    def test_expired_slot_is_reclaimed(self) -> None:
        self.assertTrue(self.acquire("req-a", limit=1, ttl=60))
        self.now[0] += 59
        self.assertFalse(self.acquire("req-b", limit=1, ttl=60))
        self.now[0] += 1
        self.assertTrue(self.acquire("req-b", limit=1, ttl=60))

    def test_reserve_followup_uses_repository_limit(self) -> None:
        self.store.create_request("req-11111111", "digest-a", "linux-worker", "example-repo")
        for state in ("preflight", "dispatching", "running", "succeeded"):
            self.store.transition("req-11111111", state)
        self.assertTrue(self.acquire("req-other", limit=2))
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            self.store.reserve_followup(
                "req-11111111", global_limit=5, host_limit=5, ttl=900, repository_limit=1
            )
        record = self.store.reserve_followup(
            "req-11111111", global_limit=5, host_limit=5, ttl=900, repository_limit=2
        )
        self.assertEqual(record.state, "dispatching")

    def test_pre_concurrency_lease_table_migrates_in_place(self) -> None:
        legacy = Path(self.directory.name) / "legacy.db"
        with sqlite3.connect(legacy) as connection:
            connection.executescript(
                """
                CREATE TABLE leases (
                    host TEXT NOT NULL,
                    repository TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (host, repository)
                );
                INSERT INTO leases VALUES ('linux-worker', 'example-repo', 'req-old', 5000.0);
                """
            )
        store = FleetStore(legacy, clock=lambda: self.now[0])
        FleetStore(legacy, clock=lambda: self.now[0])  # second open is a no-op
        with sqlite3.connect(legacy) as connection:
            rows = connection.execute(
                "SELECT host, repository, request_id, expires_at FROM leases"
            ).fetchall()
            key = [
                row[1]
                for row in sorted(
                    (r for r in connection.execute("PRAGMA table_info(leases)") if r[5]),
                    key=lambda r: r[5],
                )
            ]
        self.assertEqual(rows, [("linux-worker", "example-repo", "req-old", 5000.0)])
        self.assertEqual(key, ["host", "repository", "request_id"])
        self.assertFalse(
            store.acquire_lease("linux-worker", "example-repo", "req-new", ttl=60, limit=1)
        )
        self.assertTrue(
            store.acquire_lease("linux-worker", "example-repo", "req-new", ttl=60, limit=2)
        )


if __name__ == "__main__":
    unittest.main()
