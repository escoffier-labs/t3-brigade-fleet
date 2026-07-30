from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .errors import HostUnavailable, IdempotencyConflict, ValidationError

TRANSITIONS: dict[str, frozenset[str]] = {
    "queued": frozenset({"preflight", "blocked_offline"}),
    "preflight": frozenset({"dispatching", "queued", "blocked_offline", "failed"}),
    "dispatching": frozenset({"running", "needs_attention", "unknown", "failed"}),
    "running": frozenset({"needs_attention", "succeeded", "failed", "interrupted", "unknown"}),
    "needs_attention": frozenset(
        {"running", "succeeded", "failed", "interrupted", "unknown"}
    ),
    "blocked_offline": frozenset({"queued", "preflight"}),
    "unknown": frozenset({"running", "needs_attention", "succeeded", "failed", "interrupted"}),
    "succeeded": frozenset({"dispatching", "unknown"}),
    "failed": frozenset({"dispatching", "unknown"}),
    "interrupted": frozenset({"dispatching", "unknown"}),
}


@dataclass(frozen=True)
class RequestRecord:
    request_id: str
    digest: str
    host: str
    repository: str
    state: str
    prompt_bytes: int
    thread_id: str | None
    created_at: float
    updated_at: float
    source_revision: str | None = None
    prompt_sha256: str | None = None
    legacy: bool = False


class FleetStore:
    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time) -> None:
        self.path = str(path)
        self._clock = clock
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS requests (
                    request_id TEXT PRIMARY KEY,
                    digest TEXT NOT NULL,
                    host TEXT NOT NULL,
                    repository TEXT NOT NULL,
                    state TEXT NOT NULL,
                    prompt_bytes INTEGER NOT NULL,
                    thread_id TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL REFERENCES requests(request_id),
                    old_state TEXT,
                    new_state TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS leases (
                    host TEXT NOT NULL,
                    repository TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (host, repository)
                );
                CREATE TABLE IF NOT EXISTS host_failures (
                    host TEXT PRIMARY KEY,
                    failure_count INTEGER NOT NULL,
                    circuit_until REAL
                );
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(requests)")
            }
            if "source_revision" not in columns:
                connection.execute(
                    "ALTER TABLE requests ADD COLUMN source_revision TEXT"
                )
            if "prompt_sha256" not in columns:
                connection.execute(
                    "ALTER TABLE requests ADD COLUMN prompt_sha256 TEXT"
                )
            if "legacy" not in columns:
                connection.execute(
                    "ALTER TABLE requests ADD COLUMN legacy INTEGER NOT NULL DEFAULT 0"
                )

    @staticmethod
    def _record(row: sqlite3.Row) -> RequestRecord:
        return RequestRecord(
            request_id=row["request_id"],
            digest=row["digest"],
            host=row["host"],
            repository=row["repository"],
            state=row["state"],
            prompt_bytes=row["prompt_bytes"],
            thread_id=row["thread_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            source_revision=row["source_revision"],
            prompt_sha256=row["prompt_sha256"],
            legacy=bool(row["legacy"]),
        )

    def create_request(
        self,
        request_id: str,
        digest: str,
        host: str,
        repository: str,
        *,
        prompt_bytes: int = 0,
    ) -> RequestRecord:
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                record = self._record(existing)
                if record.digest != digest:
                    raise IdempotencyConflict(
                        f"request {request_id} already exists with a different payload digest"
                    )
                return record
            connection.execute(
                """
                INSERT INTO requests
                    (request_id, digest, host, repository, state, prompt_bytes, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)
                """,
                (request_id, digest, host, repository, prompt_bytes, now, now),
            )
            connection.execute(
                "INSERT INTO events (request_id, old_state, new_state, created_at) VALUES (?, NULL, 'queued', ?)",
                (request_id, now),
            )
        return self.get(request_id)

    def import_receipt(self, receipt: Mapping[str, Any]) -> RequestRecord:
        request_id = _receipt_string(receipt, "request_id")
        digest = _receipt_string(receipt, "request_digest")
        host = _receipt_string(receipt, "target_id")
        repository = _receipt_string(receipt, "repository")
        state = _receipt_string(receipt, "state")
        thread_id = receipt.get("thread_id")
        source_revision = receipt.get("source_revision")
        prompt_sha256 = receipt.get("prompt_sha256")
        prompt_bytes = receipt.get("prompt_bytes")
        legacy = receipt.get("legacy") is True
        if thread_id is not None and not isinstance(thread_id, str):
            raise ValidationError("target receipt contains invalid thread_id")
        if source_revision is not None and not isinstance(source_revision, str):
            raise ValidationError("target receipt contains invalid source_revision")
        if prompt_sha256 is not None and not isinstance(prompt_sha256, str):
            raise ValidationError("target receipt contains invalid prompt_sha256")
        if isinstance(prompt_bytes, bool) or not isinstance(prompt_bytes, int) or prompt_bytes < 0:
            raise ValidationError("target receipt contains invalid prompt_bytes")
        created_at = _receipt_time(receipt.get("created_at"), "created_at")
        updated_at = _receipt_time(receipt.get("updated_at"), "updated_at")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT digest, legacy FROM requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if (
                existing is not None
                and existing["digest"] != digest
                and not bool(existing["legacy"])
            ):
                raise IdempotencyConflict(
                    f"request {request_id} already exists with a different payload digest"
                )
            connection.execute(
                """
                INSERT INTO requests (
                    request_id, digest, host, repository, state, prompt_bytes,
                    thread_id, created_at, updated_at, source_revision,
                    prompt_sha256, legacy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    digest = excluded.digest,
                    host = excluded.host,
                    repository = excluded.repository,
                    state = excluded.state,
                    prompt_bytes = excluded.prompt_bytes,
                    thread_id = excluded.thread_id,
                    updated_at = excluded.updated_at,
                    source_revision = excluded.source_revision,
                    prompt_sha256 = excluded.prompt_sha256,
                    legacy = excluded.legacy
                """,
                (
                    request_id, digest, host, repository, state, prompt_bytes,
                    thread_id, created_at, updated_at, source_revision,
                    prompt_sha256, int(legacy),
                ),
            )
        return self.get(request_id)

    def get(self, request_id: str) -> RequestRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        if row is None:
            raise ValidationError(f"unknown request id: {request_id}")
        return self._record(row)

    def transition(
        self,
        request_id: str,
        new_state: str,
        *,
        thread_id: str | None = None,
    ) -> RequestRecord:
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown request id: {request_id}")
            old_state = row["state"]
            if new_state not in TRANSITIONS.get(old_state, frozenset()):
                raise ValidationError(f"invalid state transition {old_state} -> {new_state}")
            next_thread_id = thread_id if thread_id is not None else row["thread_id"]
            connection.execute(
                "UPDATE requests SET state = ?, thread_id = ?, updated_at = ? WHERE request_id = ?",
                (new_state, next_thread_id, now, request_id),
            )
            connection.execute(
                "INSERT INTO events (request_id, old_state, new_state, created_at) VALUES (?, ?, ?, ?)",
                (request_id, old_state, new_state, now),
            )
        return self.get(request_id)

    def mark_transport_failure(self, request_id: str) -> RequestRecord:
        record = self.get(request_id)
        next_state = (
            "unknown"
            if record.state in {"dispatching", "running", "needs_attention"}
            else "blocked_offline"
        )
        return self.transition(request_id, next_state)

    def acquire_lease(
        self,
        host: str,
        repository: str,
        request_id: str,
        *,
        ttl: float,
    ) -> bool:
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM leases WHERE expires_at <= ?", (now,))
            existing = connection.execute(
                "SELECT request_id FROM leases WHERE host = ? AND repository = ?",
                (host, repository),
            ).fetchone()
            if existing is not None and existing["request_id"] != request_id:
                return False
            connection.execute(
                """
                INSERT INTO leases (host, repository, request_id, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(host, repository) DO UPDATE SET
                    request_id = excluded.request_id,
                    expires_at = excluded.expires_at
                """,
                (host, repository, request_id, now + ttl),
            )
            return True

    def release_lease(self, host: str, repository: str, request_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM leases WHERE host = ? AND repository = ? AND request_id = ?",
                (host, repository, request_id),
            )

    def reserve_followup(
        self,
        request_id: str,
        *,
        global_limit: int,
        host_limit: int,
        ttl: float,
    ) -> RequestRecord:
        now = self._clock()
        active_states = (
            "preflight",
            "dispatching",
            "running",
            "needs_attention",
            "unknown",
        )
        placeholders = ",".join("?" for _ in active_states)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown request id: {request_id}")
            if row["state"] not in {"succeeded", "failed", "interrupted"}:
                raise ValidationError(
                    f"cannot reserve follow-up from state {row['state']}"
                )
            global_active = connection.execute(
                f"""
                SELECT COUNT(*) AS active
                FROM requests
                WHERE state IN ({placeholders}) AND request_id != ?
                """,
                (*active_states, request_id),
            ).fetchone()["active"]
            host_active = connection.execute(
                f"""
                SELECT COUNT(*) AS active
                FROM requests
                WHERE state IN ({placeholders}) AND request_id != ? AND host = ?
                """,
                (*active_states, request_id, row["host"]),
            ).fetchone()["active"]
            if global_active >= global_limit or host_active >= host_limit:
                raise HostUnavailable(
                    f"dispatch capacity is exhausted for host {row['host']}"
                )
            connection.execute("DELETE FROM leases WHERE expires_at <= ?", (now,))
            lease = connection.execute(
                "SELECT request_id FROM leases WHERE host = ? AND repository = ?",
                (row["host"], row["repository"]),
            ).fetchone()
            if lease is not None and lease["request_id"] != request_id:
                raise HostUnavailable(
                    f"repository is busy on host {row['host']}"
                )
            connection.execute(
                """
                INSERT INTO leases (host, repository, request_id, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(host, repository) DO UPDATE SET
                    request_id = excluded.request_id,
                    expires_at = excluded.expires_at
                """,
                (row["host"], row["repository"], request_id, now + ttl),
            )
            connection.execute(
                "UPDATE requests SET state = 'dispatching', updated_at = ? WHERE request_id = ?",
                (now, request_id),
            )
            connection.execute(
                """
                INSERT INTO events (request_id, old_state, new_state, created_at)
                VALUES (?, ?, 'dispatching', ?)
                """,
                (request_id, row["state"], now),
            )
        return self.get(request_id)

    def active_counts(self, *, host: str, exclude_request_id: str) -> tuple[int, int]:
        active_states = (
            "preflight",
            "dispatching",
            "running",
            "needs_attention",
            "unknown",
        )
        placeholders = ",".join("?" for _ in active_states)
        with self._connect() as connection:
            global_row = connection.execute(
                f"""
                SELECT COUNT(*) AS active
                FROM requests
                WHERE state IN ({placeholders}) AND request_id != ?
                """,
                (*active_states, exclude_request_id),
            ).fetchone()
            host_row = connection.execute(
                f"""
                SELECT COUNT(*) AS active
                FROM requests
                WHERE state IN ({placeholders}) AND request_id != ? AND host = ?
                """,
                (*active_states, exclude_request_id, host),
            ).fetchone()
        return int(global_row["active"]), int(host_row["active"])

    def record_host_failure(self, host: str, *, circuit_seconds: float = 300) -> bool:
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT failure_count FROM host_failures WHERE host = ?",
                (host,),
            ).fetchone()
            count = (row["failure_count"] if row is not None else 0) + 1
            circuit_until = now + circuit_seconds if count >= 3 else None
            connection.execute(
                """
                INSERT INTO host_failures (host, failure_count, circuit_until)
                VALUES (?, ?, ?)
                ON CONFLICT(host) DO UPDATE SET
                    failure_count = excluded.failure_count,
                    circuit_until = excluded.circuit_until
                """,
                (host, count, circuit_until),
            )
        return count >= 3

    def record_host_success(self, host: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM host_failures WHERE host = ?", (host,))

    def circuit_is_open(self, host: str) -> bool:
        now = self._clock()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT circuit_until FROM host_failures WHERE host = ?",
                (host,),
            ).fetchone()
        return bool(row is not None and row["circuit_until"] is not None and row["circuit_until"] > now)

    def unknown_requests_for_host(self, host: str) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT request_id FROM requests WHERE host = ? AND state = 'unknown' ORDER BY created_at, request_id",
                (host,),
            ).fetchall()
        return [row["request_id"] for row in rows]


def _receipt_string(receipt: Mapping[str, Any], key: str) -> str:
    value = receipt.get(key)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"target receipt omitted {key}")
    return value


def _receipt_time(value: object, label: str) -> float:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"target receipt omitted {label}")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError as error:
        raise ValidationError(f"target receipt contains invalid {label}") from error
