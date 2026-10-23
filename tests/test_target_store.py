from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from t3_fleet.errors import HostUnavailable, IdempotencyConflict, TargetBusy
import sqlite3
from t3_fleet.target_store import TargetClaim, TargetStore


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
            store.claim(
                self.claim("linux-worker-controller-b-22345678-1234-1234-1234-123456789abc"),
                active_limit=2,
                reservation_ttl=30,
                repository_limit=1,
            )

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

    def test_claim_rejects_admission_presence_mismatch_on_retry(self) -> None:
        store = TargetStore(self.path)
        claim = self.claim(
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        first = store.claim(claim, active_limit=1, reservation_ttl=30)
        self.assertIsNone(first.admission)
        with self.assertRaises(IdempotencyConflict):
            store.claim(
                TargetClaim(**{**claim.__dict__, "admission": _admission()}),
                active_limit=1,
                reservation_ttl=30,
            )
        self.assertIsNone(store.lookup(claim.request_id).admission)

        admitted_path = Path(self.directory.name) / "admitted.db"
        admitted_store = TargetStore(admitted_path)
        admitted_claim = self.claim(
            "linux-worker-controller-b-12345678-1234-1234-1234-123456789abc"
        )
        admitted = admitted_store.claim(
            TargetClaim(**{**admitted_claim.__dict__, "admission": _admission()}),
            active_limit=1,
            reservation_ttl=30,
        )
        self.assertIsNotNone(admitted.admission)
        with self.assertRaises(IdempotencyConflict):
            admitted_store.claim(admitted_claim, active_limit=1, reservation_ttl=30)
        self.assertIsNotNone(admitted_store.lookup(admitted_claim.request_id).admission)

    def test_claim_rejects_malformed_model_admission_before_reservation(self) -> None:
        store = TargetStore(self.path)
        claim = self.claim(
            "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        )
        malformed = TargetClaim(
            **{**claim.__dict__, "admission": {"model": "gpt-5.6-terra"}}
        )
        with self.assertRaisesRegex(Exception, "model admission"):
            store.claim(malformed, active_limit=1, reservation_ttl=30)
        self.assertIsNone(store.lookup(claim.request_id))

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

    def test_stale_preflight_lease_is_released_for_a_different_request(self) -> None:
        # Regression: reservation expiry is wall-clock, so a preflight lease from
        # an earlier request (e.g. one left behind across an agent restart) must
        # expire and free the repository for a NEW request instead of wedging
        # every later submit with "repository is busy" (surfaced as unknown_state).
        now = [1000.0]
        store = TargetStore(self.path, clock=lambda: now[0])
        first = self.claim("linux-worker-controller-a-12345678-1234-1234-1234-123456789abc")
        store.claim(first, active_limit=4, reservation_ttl=5)  # stays preflight, never attaches
        now[0] = 1000.0 + 6.0  # first reservation's wall-clock TTL has passed
        second = self.claim("linux-worker-controller-b-99995678-1234-1234-1234-123456789abc")
        self.assertEqual(
            store.claim(second, active_limit=4, reservation_ttl=5).state,
            "preflight",
        )

    def test_expired_lease_purges_even_when_request_is_not_preflight(self) -> None:
        # A thread-less request stuck in a non-preflight state (e.g. 'unknown')
        # with a lapsed reservation must still release its repository lease.
        now = [1000.0]
        store = TargetStore(self.path, clock=lambda: now[0])
        stuck = self.claim("linux-worker-controller-a-12345678-1234-1234-1234-123456789abc")
        store.claim(stuck, active_limit=4, reservation_ttl=5)
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "UPDATE target_requests SET state = 'unknown' WHERE request_id = ?",
                (stuck.request_id,),
            )
        now[0] = 1000.0 + 6.0
        other = self.claim("linux-worker-controller-b-77775678-1234-1234-1234-123456789abc")
        self.assertEqual(
            store.claim(other, active_limit=4, reservation_ttl=5).state,
            "preflight",
        )

    def test_expired_unattached_claim_is_released_for_matching_retry(self) -> None:
        now = [0.0]
        store = TargetStore(self.path, clock=lambda: now[0])
        claim = self.claim("linux-worker-controller-a-12345678-1234-1234-1234-123456789abc")
        store.claim(claim, active_limit=1, reservation_ttl=5)
        now[0] = 6.0
        self.assertEqual(
            store.claim(claim, active_limit=1, reservation_ttl=5).state,
            "preflight",
        )


def _peer_id(index: int) -> str:
    return f"linux-worker-controller-a-{index:08d}-1234-1234-1234-123456789abc"


class RepositoryLeaseSlotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "target.db"
        self.now = [1000.0]
        self.store = TargetStore(self.path, clock=lambda: self.now[0])

    def claim(self, index: int, *, repository_limit: int, lease_ttl: float = 600, repository: str = "example-repo"):
        return self.store.claim(
            TargetClaim(
                request_id=_peer_id(index),
                digest=f"digest-{index}",
                target_id="linux-worker",
                repository=repository,
                source_revision="0123456789abcdef0123456789abcdef01234567",
                prompt_sha256="a" * 64,
                prompt_bytes=12,
            ),
            active_limit=10,
            reservation_ttl=30,
            repository_limit=repository_limit,
            lease_ttl=lease_ttl,
        )

    def start(self, index: int, *, repository_limit: int, lease_ttl: float = 600) -> None:
        self.claim(index, repository_limit=repository_limit, lease_ttl=lease_ttl)
        self.store.attach_thread(_peer_id(index), f"thread-{index}")

    def lease_rows(self) -> list[tuple[str, str]]:
        with sqlite3.connect(self.path) as connection:
            return connection.execute(
                "SELECT repository, request_id FROM repository_leases ORDER BY request_id"
            ).fetchall()

    def test_limit_n_allows_n_and_rejects_n_plus_one(self) -> None:
        for index in range(1, 4):
            self.start(index, repository_limit=3)
        with self.assertRaisesRegex(HostUnavailable, r"repository is busy on this target \(3"):
            self.claim(4, repository_limit=3)
        # The rejected claim did not take a slot or a reservation.
        self.assertEqual(len(self.lease_rows()), 3)
        self.assertEqual(self.store.active_count(), 3)
        # A different repository on the same target is independent.
        self.claim(5, repository_limit=3, repository="other-repo")

    def test_limit_one_keeps_single_slot_behavior(self) -> None:
        self.start(1, repository_limit=1)
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            self.claim(2, repository_limit=1)

    def test_lease_ttl_expiry_frees_a_slot_for_a_stuck_attached_request(self) -> None:
        self.start(1, repository_limit=1, lease_ttl=600)
        self.now[0] += 599
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            self.claim(2, repository_limit=1)
        self.now[0] += 1
        self.assertEqual(self.claim(2, repository_limit=1).state, "preflight")
        # Host capacity still counts the stuck thread until it reconciles.
        self.assertEqual(self.store.active_count(), 2)

    def test_reconcile_of_a_running_request_renews_an_expired_lease(self) -> None:
        self.start(1, repository_limit=1, lease_ttl=600)
        # Unpolled past the lease lifetime: the slot is freed and taken.
        self.now[0] += 601
        self.start(2, repository_limit=1, lease_ttl=600)
        self.assertEqual([row[1] for row in self.lease_rows()], [_peer_id(2)])
        # The first request is still running when it is next polled, so it
        # holds its repository again rather than being treated as lost.
        receipt = self.store.reconcile(
            _peer_id(1), state="running", thread_id="thread-1", lease_ttl=600
        )
        self.assertEqual(receipt.state, "running")
        with sqlite3.connect(self.path) as connection:
            expiry = connection.execute(
                "SELECT expires_at FROM repository_leases WHERE request_id = ?",
                (_peer_id(1),),
            ).fetchone()[0]
        self.assertEqual(expiry, self.now[0] + 600)
        with self.assertRaises(TargetBusy):
            self.claim(3, repository_limit=2)

    def test_reconcile_keeps_a_polled_lease_alive_past_the_original_window(self) -> None:
        self.start(1, repository_limit=1, lease_ttl=600)
        for _ in range(3):
            self.now[0] += 500
            self.store.reconcile(_peer_id(1), state="running", thread_id=None, lease_ttl=600)
        with self.assertRaisesRegex(TargetBusy, "repository is busy") as raised:
            self.claim(2, repository_limit=1)
        self.assertEqual(raised.exception.reason, "repository-busy")

    def test_terminal_reconcile_with_a_ttl_still_releases(self) -> None:
        self.start(1, repository_limit=1)
        self.store.reconcile(_peer_id(1), state="succeeded", thread_id=None, lease_ttl=600)
        self.assertEqual(self.lease_rows(), [])

    def test_refresh_lease_recreates_an_expired_row(self) -> None:
        self.start(1, repository_limit=1, lease_ttl=600)
        self.now[0] += 601
        self.claim(2, repository_limit=2)  # purges the expired row
        self.assertNotIn(_peer_id(1), [row[1] for row in self.lease_rows()])
        self.store.refresh_lease(_peer_id(1), lease_ttl=600)
        self.assertIn(_peer_id(1), [row[1] for row in self.lease_rows()])

    def test_forget_unstarted_drops_only_a_threadless_claim(self) -> None:
        self.claim(1, repository_limit=2)
        self.start(2, repository_limit=2)
        self.store.forget_unstarted(_peer_id(1))
        self.store.forget_unstarted(_peer_id(2))
        self.assertIsNone(self.store.lookup(_peer_id(1)))
        self.assertEqual(self.store.lookup(_peer_id(2)).thread_id, "thread-2")
        self.assertEqual([row[1] for row in self.lease_rows()], [_peer_id(2)])

    def test_refresh_lease_extends_the_window(self) -> None:
        self.start(1, repository_limit=1, lease_ttl=600)
        self.now[0] += 500
        self.store.refresh_lease(_peer_id(1), lease_ttl=600)
        self.now[0] += 500
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            self.claim(2, repository_limit=1)

    def test_terminal_states_and_release_free_slots(self) -> None:
        for index in range(1, 4):
            self.start(index, repository_limit=4)
        self.claim(4, repository_limit=4)  # thread-less reservation
        for index, state in ((1, "succeeded"), (2, "failed"), (3, "interrupted")):
            with self.subTest(state=state):
                self.store.reconcile(_peer_id(index), state=state, thread_id=f"thread-{index}")
                self.assertNotIn(_peer_id(index), [row[1] for row in self.lease_rows()])
        self.store.release(_peer_id(4))
        self.assertEqual(self.lease_rows(), [])
        self.assertEqual(self.store.active_count(), 0)

    def test_non_terminal_reconcile_keeps_the_slot(self) -> None:
        self.start(1, repository_limit=1)
        self.store.reconcile(_peer_id(1), state="needs_attention", thread_id="thread-1")
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            self.claim(2, repository_limit=1)

    def test_reserve_followup_uses_repository_limit(self) -> None:
        self.start(1, repository_limit=2)
        self.store.reconcile(_peer_id(1), state="succeeded", thread_id="thread-1")
        self.start(2, repository_limit=2)
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            self.store.reserve_followup(
                _peer_id(1), active_limit=10, reservation_ttl=30, repository_limit=1
            )
        receipt = self.store.reserve_followup(
            _peer_id(1), active_limit=10, reservation_ttl=30, repository_limit=2
        )
        self.assertEqual(receipt.state, "dispatching")

    def test_pre_concurrency_target_db_migrates_in_place(self) -> None:
        legacy = Path(self.directory.name) / "legacy.db"
        with sqlite3.connect(legacy) as connection:
            connection.executescript(
                """
                CREATE TABLE target_requests (
                    request_id TEXT PRIMARY KEY,
                    digest TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    repository TEXT,
                    source_revision TEXT,
                    state TEXT NOT NULL,
                    thread_id TEXT,
                    prompt_sha256 TEXT NOT NULL,
                    prompt_bytes INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    legacy INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE reservations (
                    request_id TEXT PRIMARY KEY REFERENCES target_requests(request_id),
                    expires_at REAL NOT NULL
                );
                CREATE TABLE repository_leases (
                    repository TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL REFERENCES target_requests(request_id)
                );
                """
            )
            connection.execute(
                "INSERT INTO target_requests VALUES (?, 'digest-old', 'linux-worker', "
                "'example-repo', NULL, 'running', 'thread-old', ?, 3, "
                "'2026-07-28T01:00:00Z', '2026-07-28T01:00:00Z', 0)",
                (_peer_id(9), "b" * 64),
            )
            connection.execute(
                "INSERT INTO repository_leases VALUES ('example-repo', ?)",
                (_peer_id(9),),
            )
        store = TargetStore(legacy, clock=lambda: self.now[0])
        TargetStore(legacy, clock=lambda: self.now[0])  # second open is a no-op
        with sqlite3.connect(legacy) as connection:
            rows = connection.execute(
                "SELECT repository, request_id, expires_at FROM repository_leases"
            ).fetchall()
        self.assertEqual(rows, [("example-repo", _peer_id(9), 1000.0 + 6 * 60 * 60)])
        self.assertEqual(store.lookup(_peer_id(9)).thread_id, "thread-old")
        claim = TargetClaim(
            request_id=_peer_id(10),
            digest="digest-10",
            target_id="linux-worker",
            repository="example-repo",
            source_revision="0123456789abcdef0123456789abcdef01234567",
            prompt_sha256="a" * 64,
            prompt_bytes=12,
        )
        with self.assertRaisesRegex(HostUnavailable, "repository is busy"):
            store.claim(claim, active_limit=5, reservation_ttl=30, repository_limit=1)
        self.assertEqual(
            store.claim(claim, active_limit=5, reservation_ttl=30, repository_limit=2).state,
            "preflight",
        )
