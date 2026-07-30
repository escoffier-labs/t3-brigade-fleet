from __future__ import annotations

import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from t3_fleet.errors import HostUnavailable, IdempotencyConflict, ValidationError
from t3_fleet.store import FleetStore


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


if __name__ == "__main__":
    unittest.main()
