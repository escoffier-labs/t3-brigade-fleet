from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from t3_fleet.errors import HostUnavailable, IdempotencyConflict
from t3_fleet.target_store import TargetClaim, TargetStore


class TargetStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "target.db"

    def claim(self, request_id: str, repository: str = "example-repo") -> TargetClaim:
        return TargetClaim(
            request_id=request_id,
            digest=f"digest-{request_id}",
            target_id="linux-worker",
            repository=repository,
            source_revision="0123456789abcdef0123456789abcdef01234567",
            prompt_sha256="a" * 64,
            prompt_bytes=12,
        )

    def test_two_connections_claim_one_request_once(self) -> None:
        first, second = TargetStore(self.path), TargetStore(self.path)
        with ThreadPoolExecutor(max_workers=2) as pool:
            receipts = list(
                pool.map(
                    lambda store: store.claim(
                        self.claim("linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"),
                        active_limit=1,
                        reservation_ttl=30,
                    ),
                    (first, second),
                )
            )
        self.assertEqual({item.request_id for item in receipts}, {
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        })
        self.assertEqual(first.active_count(), 1)

    def test_claim_rejects_capacity_and_repository_conflicts(self) -> None:
        store = TargetStore(self.path)
        store.claim(self.claim("linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"), active_limit=1, reservation_ttl=30)
        with self.assertRaisesRegex(HostUnavailable, "capacity"):
            store.claim(self.claim("linux-worker-controller-b-12345678-1234-1234-1234-123456789abc", "other-repo"), active_limit=1, reservation_ttl=30)
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            store.claim(self.claim("linux-worker-controller-b-22345678-1234-1234-1234-123456789abc"), active_limit=2, reservation_ttl=30)

    def test_matching_retry_returns_receipt_and_mismatched_retry_conflicts(self) -> None:
        store = TargetStore(self.path)
        claim = self.claim("linux-worker-controller-a-12345678-1234-1234-1234-123456789abc")
        self.assertEqual(
            store.claim(claim, active_limit=1, reservation_ttl=30),
            store.claim(claim, active_limit=1, reservation_ttl=30),
        )
        with self.assertRaises(IdempotencyConflict):
            store.claim(
                TargetClaim(**{**claim.__dict__, "digest": "b" * 64}),
                active_limit=1,
                reservation_ttl=30,
            )

    def test_terminal_reconcile_releases_reservation_and_lease(self) -> None:
        store = TargetStore(self.path)
        claim = self.claim("linux-worker-controller-a-12345678-1234-1234-1234-123456789abc")
        store.claim(claim, active_limit=1, reservation_ttl=30)
        store.attach_thread(claim.request_id, "thread-1")
        store.reconcile(claim.request_id, state="succeeded", thread_id="thread-1")
        store.claim(self.claim("linux-worker-controller-b-12345678-1234-1234-1234-123456789abc"), active_limit=1, reservation_ttl=30)

    def test_terminal_reconcile_is_monotonic(self) -> None:
        store = TargetStore(self.path)
        claim = self.claim("linux-worker-controller-a-12345678-1234-1234-1234-123456789abc")
        store.claim(claim, active_limit=1, reservation_ttl=30)
        store.attach_thread(claim.request_id, "thread-1")
        first = store.reconcile(claim.request_id, state="succeeded", thread_id="thread-1")
        for stale_state in ("unknown", "running", "failed"):
            with self.subTest(stale_state=stale_state):
                receipt = store.reconcile(
                    claim.request_id,
                    state=stale_state,
                    thread_id="thread-1",
                )
                self.assertEqual(receipt.state, "succeeded")
                self.assertEqual(receipt.updated_at, first.updated_at)
        self.assertEqual(store.active_count(), 0)
        store.claim(
            self.claim("linux-worker-controller-b-12345678-1234-1234-1234-123456789abc"),
            active_limit=1,
            reservation_ttl=30,
        )

    def test_legacy_json_receipt_migrates_without_prompt_or_path(self) -> None:
        state_dir = Path(self.directory.name)
        (state_dir / "req-12345678-submit.json").write_text(
            '{"schema":"t3-fleet.agent-receipt.v1","action":"submit",'
            '"request_id":"req-12345678","thread_id":"thread-1",'
            '"prompt_sha256":"' + "c" * 64 + '","prompt_bytes":19,'
            '"outcome":"accepted","created_at":"2026-07-28T01:00:00Z"}\n',
            encoding="utf-8",
        )
        store = TargetStore(self.path)
        receipt = store.migrate_legacy_receipt(
            state_dir / "req-12345678-submit.json",
            target_id="linux-worker",
            repository="example-repo",
            source_revision="0123456789abcdef0123456789abcdef01234567",
        )
        self.assertTrue(receipt.legacy)
        self.assertEqual(receipt.digest, "legacy:" + "c" * 64)
        self.assertNotIn("private prompt", repr(receipt))
        self.assertNotIn("/srv/repos/example", repr(receipt))

    def test_expired_unattached_claim_is_released_for_matching_retry(self) -> None:
        now = [0.0]
        store = TargetStore(self.path, monotonic=lambda: now[0])
        claim = self.claim("linux-worker-controller-a-12345678-1234-1234-1234-123456789abc")
        store.claim(claim, active_limit=1, reservation_ttl=5)
        now[0] = 6.0
        self.assertEqual(
            store.claim(claim, active_limit=1, reservation_ttl=5).state,
            "preflight",
        )
