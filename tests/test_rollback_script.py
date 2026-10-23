"""The reverse migration must leave databases the pre-concurrency code can use."""
from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from t3_fleet.config import load_config
from t3_fleet.store import FleetStore
from t3_fleet.target_store import TargetClaim, TargetStore

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "rollback-concurrency-schema.py"

# Statements the pre-concurrency release runs against these tables, verbatim.
OLD_TARGET_LEASE_UPSERT = """
    INSERT INTO repository_leases (repository, request_id)
    VALUES (?, ?)
    ON CONFLICT(repository) DO UPDATE SET request_id = excluded.request_id
"""
OLD_CONTROLLER_LEASE_UPSERT = """
    INSERT INTO leases (host, repository, request_id, expires_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(host, repository) DO UPDATE SET
        request_id = excluded.request_id,
        expires_at = excluded.expires_at
"""

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "fleet.example.toml"
# The shipped example with both concurrency keys switched on.
CONFIG = (
    EXAMPLE.read_text(encoding="utf-8")
    .replace("# max_concurrent_per_repository = 0", "max_concurrent_per_repository = 0")
    .replace("# repository_lease_ttl_seconds = 21600", "repository_lease_ttl_seconds = 21600")
)


def _dump(path: Path) -> list[str]:
    # Compare logical content: WAL checkpoints rewrite the file's bytes.
    with sqlite3.connect(path) as connection:
        return list(connection.iterdump())


def _peer(index: int) -> str:
    return f"linux-worker-controller-a-{index:08d}-1234-1234-1234-123456789abc"


class RollbackScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.target_db = self.root / "target.db"
        self.controller_db = self.root / "fleet.db"
        self.target = TargetStore(self.target_db)
        self.controller = FleetStore(self.controller_db)

    def run_script(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

    def claim(self, index: int, *, lease_ttl: float = 600) -> None:
        self.target.claim(
            TargetClaim(
                request_id=_peer(index),
                digest=f"digest-{index}",
                target_id="linux-worker",
                repository="example-repo",
                source_revision="0" * 40,
                prompt_sha256="a" * 64,
                prompt_bytes=1,
            ),
            active_limit=5,
            reservation_ttl=600,
            repository_limit=5,
            lease_ttl=lease_ttl,
        )
        self.target.attach_thread(_peer(index), f"thread-{index}")

    def test_single_live_lease_rolls_back_to_the_old_schema(self) -> None:
        self.claim(1)
        self.assertTrue(
            self.controller.acquire_lease("linux-worker", "example-repo", "req-1", ttl=600, limit=3)
        )
        result = self.run_script(
            "--controller-db", str(self.controller_db), "--target-db", str(self.target_db)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        with sqlite3.connect(self.target_db) as connection:
            columns = [row[1] for row in connection.execute("PRAGMA table_info(repository_leases)")]
            pk = [row[1] for row in connection.execute("PRAGMA table_info(repository_leases)") if row[5]]
            self.assertEqual(columns, ["repository", "request_id"])
            self.assertEqual(pk, ["repository"])
            self.assertEqual(
                connection.execute("SELECT repository, request_id FROM repository_leases").fetchall(),
                [("example-repo", _peer(1))],
            )
            # The old release's own upsert works again.
            connection.execute(OLD_TARGET_LEASE_UPSERT, ("example-repo", _peer(1)))
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
        with sqlite3.connect(self.controller_db) as connection:
            pk = sorted(
                (row[5], row[1]) for row in connection.execute("PRAGMA table_info(leases)") if row[5]
            )
            self.assertEqual([name for _, name in pk], ["host", "repository"])
            connection.execute(
                OLD_CONTROLLER_LEASE_UPSERT, ("linux-worker", "example-repo", "req-1", time.time() + 60)
            )
        # Backups were taken beside each database.
        self.assertEqual(len(list(self.root.glob("target.db.before-rollback-*"))), 1)
        self.assertEqual(len(list(self.root.glob("fleet.db.before-rollback-*"))), 1)
        # A second run is a no-op, and the current release upgrades it again.
        again = self.run_script("--target-db", str(self.target_db))
        self.assertIn("already pre-concurrency", again.stdout)
        TargetStore(self.target_db)
        FleetStore(self.controller_db)
        with sqlite3.connect(self.target_db) as connection:
            self.assertIn(
                "expires_at",
                [row[1] for row in connection.execute("PRAGMA table_info(repository_leases)")],
            )

    def test_several_live_leases_for_one_repository_refuse_without_force(self) -> None:
        self.claim(1, lease_ttl=600)
        self.claim(2, lease_ttl=1200)
        before = _dump(self.target_db)
        refused = self.run_script("--target-db", str(self.target_db))
        self.assertEqual(refused.returncode, 2)
        self.assertIn("more than one live lease", refused.stderr)
        self.assertEqual(_dump(self.target_db), before)
        self.assertEqual(list(self.root.glob("*.before-rollback-*")), [])

        forced = self.run_script("--target-db", str(self.target_db), "--force")
        self.assertEqual(forced.returncode, 0, forced.stderr)
        self.assertIn(f"dropped_conflicting=['{_peer(1)}']", forced.stdout)
        with sqlite3.connect(self.target_db) as connection:
            self.assertEqual(
                connection.execute("SELECT request_id FROM repository_leases").fetchall(),
                [(_peer(2),)],
            )

    def test_expired_leases_are_dropped(self) -> None:
        self.claim(1)
        with sqlite3.connect(self.target_db) as connection:
            connection.execute("UPDATE repository_leases SET expires_at = 1")
        result = self.run_script("--target-db", str(self.target_db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("dropped_expired=1", result.stdout)
        with sqlite3.connect(self.target_db) as connection:
            self.assertEqual(connection.execute("SELECT * FROM repository_leases").fetchall(), [])

    def test_dry_run_changes_nothing(self) -> None:
        self.claim(1)
        config = self.root / "config.toml"
        config.write_text(CONFIG, encoding="utf-8")
        before = (_dump(self.target_db), config.read_bytes())
        result = self.run_script(
            "--target-db", str(self.target_db), "--config", str(config), "--dry-run"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((_dump(self.target_db), config.read_bytes()), before)
        self.assertEqual(list(self.root.glob("*.before-rollback-*")), [])

    def test_config_keys_are_removed_and_the_file_still_loads(self) -> None:
        config = self.root / "config.toml"
        config.write_bytes(CONFIG.replace("\n", "\r\n").encode("utf-8"))
        config.chmod(0o600)
        result = self.run_script("--config", str(config))
        self.assertEqual(result.returncode, 0, result.stderr)
        text = config.read_bytes().decode("utf-8")
        self.assertNotIn("\nmax_concurrent_per_repository", text)
        self.assertNotIn("\nrepository_lease_ttl_seconds", text)
        self.assertIn("lines=2", result.stdout)
        self.assertIn("\r\nactive_limit = 1\r\n", text)
        # Comments that only mention the keys are left alone.
        self.assertIn("# Optional. Per-repository cap on this host.", text)
        self.assertEqual(len(list(self.root.glob("config.toml.before-rollback-*"))), 1)
        self.assertEqual(load_config(config).hosts["controller-a"].active_limit, 1)


if __name__ == "__main__":
    unittest.main()
