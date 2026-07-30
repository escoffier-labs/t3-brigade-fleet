from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from .errors import HostUnavailable, IdempotencyConflict, ValidationError

_TERMINAL_STATES = frozenset({"succeeded", "failed", "interrupted"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class TargetClaim:
    request_id: str
    digest: str
    target_id: str
    repository: str
    source_revision: str
    prompt_sha256: str
    prompt_bytes: int


@dataclass(frozen=True)
class TargetReceipt:
    request_id: str
    target_id: str
    repository: str | None
    digest: str
    source_revision: str | None
    state: str
    thread_id: str | None
    prompt_sha256: str
    prompt_bytes: int
    created_at: str
    updated_at: str
    legacy: bool = False


def receipt_payload(receipt: TargetReceipt) -> dict[str, Any]:
    return {
        "schema": "t3-fleet.agent-result.v1",
        "request_id": receipt.request_id,
        "target_id": receipt.target_id,
        "repository": receipt.repository,
        "request_digest": receipt.digest,
        "source_revision": receipt.source_revision,
        "state": receipt.state,
        "thread_id": receipt.thread_id,
        "prompt_sha256": receipt.prompt_sha256,
        "prompt_bytes": receipt.prompt_bytes,
        "created_at": receipt.created_at,
        "updated_at": receipt.updated_at,
        "needs_attention": receipt.state == "needs_attention",
    }


class TargetStore:
    def __init__(
        self,
        path: str | Path,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], str] = _utc_now,
    ) -> None:
        self.path = str(path)
        self._monotonic = monotonic
        self._now = now
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
                CREATE TABLE IF NOT EXISTS target_requests (
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
                CREATE TABLE IF NOT EXISTS reservations (
                    request_id TEXT PRIMARY KEY REFERENCES target_requests(request_id),
                    expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS repository_leases (
                    repository TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL REFERENCES target_requests(request_id)
                );
                """
            )

    @staticmethod
    def _receipt(row: sqlite3.Row) -> TargetReceipt:
        return TargetReceipt(
            request_id=row["request_id"],
            target_id=row["target_id"],
            repository=row["repository"],
            digest=row["digest"],
            source_revision=row["source_revision"],
            state=row["state"],
            thread_id=row["thread_id"],
            prompt_sha256=row["prompt_sha256"],
            prompt_bytes=row["prompt_bytes"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            legacy=bool(row["legacy"]),
        )

    @staticmethod
    def _purge_expired(connection: sqlite3.Connection, now_mono: float) -> None:
        connection.execute(
            """
            DELETE FROM repository_leases
            WHERE request_id IN (
                SELECT r.request_id FROM reservations r
                JOIN target_requests t ON t.request_id = r.request_id
                WHERE r.expires_at <= ? AND t.thread_id IS NULL AND t.state = 'preflight'
            )
            """,
            (now_mono,),
        )
        connection.execute(
            """
            DELETE FROM reservations
            WHERE expires_at <= ?
              AND request_id IN (
                  SELECT request_id FROM target_requests
                  WHERE thread_id IS NULL AND state = 'preflight'
              )
            """,
            (now_mono,),
        )

    def _active_count(
        self,
        connection: sqlite3.Connection,
        exclude_request_id: str | None,
    ) -> int:
        row = connection.execute(
            """
            SELECT COUNT(*) AS active
            FROM target_requests t
            WHERE t.state NOT IN ('succeeded', 'failed', 'interrupted')
              AND t.request_id IS NOT ?
              AND (
                  t.thread_id IS NOT NULL
                  OR EXISTS (
                      SELECT 1 FROM reservations r
                      WHERE r.request_id = t.request_id AND r.expires_at > ?
                  )
              )
            """,
            (exclude_request_id, self._monotonic()),
        ).fetchone()
        return int(row["active"])

    @staticmethod
    def _upsert_reservation(
        connection: sqlite3.Connection,
        request_id: str,
        expires_at: float,
    ) -> None:
        connection.execute(
            """
            INSERT INTO reservations (request_id, expires_at)
            VALUES (?, ?)
            ON CONFLICT(request_id) DO UPDATE SET expires_at = excluded.expires_at
            """,
            (request_id, expires_at),
        )

    @staticmethod
    def _ensure_lease(
        connection: sqlite3.Connection,
        repository: str | None,
        request_id: str,
    ) -> None:
        if repository is None:
            return
        lease = connection.execute(
            "SELECT request_id FROM repository_leases WHERE repository = ?",
            (repository,),
        ).fetchone()
        if lease is not None and lease["request_id"] != request_id:
            raise HostUnavailable("repository is busy on this target")
        connection.execute(
            """
            INSERT INTO repository_leases (repository, request_id)
            VALUES (?, ?)
            ON CONFLICT(repository) DO UPDATE SET request_id = excluded.request_id
            """,
            (repository, request_id),
        )

    def claim(
        self,
        claim: TargetClaim,
        *,
        active_limit: int,
        reservation_ttl: float,
    ) -> TargetReceipt:
        now_mono = self._monotonic()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._purge_expired(connection, now_mono)
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?",
                (claim.request_id,),
            ).fetchone()
            if row is not None:
                if row["legacy"]:
                    raise IdempotencyConflict(
                        f"request {claim.request_id} is a legacy import and cannot be claimed"
                    )
                if row["digest"] != claim.digest:
                    raise IdempotencyConflict(
                        f"request {claim.request_id} already exists with a different payload digest"
                    )
                if (
                    row["repository"] != claim.repository
                    or row["source_revision"] != claim.source_revision
                    or row["target_id"] != claim.target_id
                ):
                    raise IdempotencyConflict(
                        f"request {claim.request_id} has mismatched immutable target identity"
                    )
            if self._active_count(connection, claim.request_id) >= active_limit:
                raise HostUnavailable(
                    f"target capacity is exhausted at {active_limit} active requests"
                )
            if row is None:
                now = self._now()
                connection.execute(
                    """
                    INSERT INTO target_requests
                        (request_id, digest, target_id, repository, source_revision,
                         state, thread_id, prompt_sha256, prompt_bytes,
                         created_at, updated_at, legacy)
                    VALUES (?, ?, ?, ?, ?, 'preflight', NULL, ?, ?, ?, ?, 0)
                    """,
                    (
                        claim.request_id,
                        claim.digest,
                        claim.target_id,
                        claim.repository,
                        claim.source_revision,
                        claim.prompt_sha256,
                        claim.prompt_bytes,
                        now,
                        now,
                    ),
                )
            self._ensure_lease(connection, claim.repository, claim.request_id)
            self._upsert_reservation(
                connection,
                claim.request_id,
                now_mono + reservation_ttl,
            )
        receipt = self.lookup(claim.request_id)
        if receipt is None:
            raise ValidationError(f"unknown request id: {claim.request_id}")
        return receipt

    def attach_thread(self, request_id: str, thread_id: str) -> TargetReceipt:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "UPDATE target_requests SET thread_id = ?, state = 'running', updated_at = ? WHERE request_id = ?",
                (thread_id, self._now(), request_id),
            )
            if cursor.rowcount != 1:
                raise ValidationError(f"unknown request id: {request_id}")
        receipt = self.lookup(request_id)
        if receipt is None:
            raise ValidationError(f"unknown request id: {request_id}")
        return receipt

    def reconcile(
        self,
        request_id: str,
        *,
        state: str,
        thread_id: str | None,
    ) -> TargetReceipt:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown request id: {request_id}")
            if row["state"] in _TERMINAL_STATES:
                return self._receipt(row)
            next_thread_id = thread_id if thread_id is not None else row["thread_id"]
            connection.execute(
                "UPDATE target_requests SET state = ?, thread_id = ?, updated_at = ? WHERE request_id = ?",
                (state, next_thread_id, self._now(), request_id),
            )
            if state in _TERMINAL_STATES:
                connection.execute(
                    "DELETE FROM reservations WHERE request_id = ?",
                    (request_id,),
                )
                connection.execute(
                    "DELETE FROM repository_leases WHERE request_id = ?",
                    (request_id,),
                )
        receipt = self.lookup(request_id)
        if receipt is None:
            raise ValidationError(f"unknown request id: {request_id}")
        return receipt

    def reserve_followup(
        self,
        request_id: str,
        *,
        active_limit: int,
        reservation_ttl: float,
    ) -> TargetReceipt:
        now_mono = self._monotonic()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._purge_expired(connection, now_mono)
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown request id: {request_id}")
            if row["state"] not in _TERMINAL_STATES:
                raise ValidationError(
                    f"cannot reserve follow-up from state {row['state']}"
                )
            if self._active_count(connection, request_id) >= active_limit:
                raise HostUnavailable(
                    f"target capacity is exhausted at {active_limit} active requests"
                )
            self._ensure_lease(connection, row["repository"], request_id)
            connection.execute(
                "UPDATE target_requests SET state = 'dispatching', updated_at = ? WHERE request_id = ?",
                (self._now(), request_id),
            )
            self._upsert_reservation(connection, request_id, now_mono + reservation_ttl)
        receipt = self.lookup(request_id)
        if receipt is None:
            raise ValidationError(f"unknown request id: {request_id}")
        return receipt

    def migrate_legacy_receipt(
        self,
        receipt_path: Path,
        *,
        target_id: str,
        repository: str | None,
        source_revision: str | None,
    ) -> TargetReceipt:
        try:
            payload = json.loads(Path(receipt_path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValidationError("legacy target receipt could not be read") from error
        if not isinstance(payload, dict) or payload.get("schema") != "t3-fleet.agent-receipt.v1":
            raise ValidationError("legacy target receipt has an unsupported schema")
        request_id = payload.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise ValidationError("legacy target receipt has no request id")
        prompt_sha256 = payload.get("prompt_sha256")
        if not isinstance(prompt_sha256, str) or not _SHA256.fullmatch(prompt_sha256):
            raise ValidationError("legacy target receipt has no prompt digest")
        prompt_bytes = payload.get("prompt_bytes")
        if isinstance(prompt_bytes, bool) or not isinstance(prompt_bytes, int) or prompt_bytes < 0:
            raise ValidationError("legacy target receipt has an invalid prompt size")
        thread_id = payload.get("thread_id")
        if thread_id is not None and not isinstance(thread_id, str):
            raise ValidationError("legacy target receipt has an invalid thread id")
        created_at = payload.get("created_at")
        if not isinstance(created_at, str) or not created_at:
            created_at = self._now()
        digest = f"legacy:{prompt_sha256}"
        state = "running" if payload.get("outcome") == "accepted" else "unknown"
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is not None:
                if row["digest"] != digest:
                    raise IdempotencyConflict(
                        f"request {request_id} already exists with a different payload digest"
                    )
                return self._receipt(row)
            connection.execute(
                """
                INSERT INTO target_requests
                    (request_id, digest, target_id, repository, source_revision,
                     state, thread_id, prompt_sha256, prompt_bytes,
                     created_at, updated_at, legacy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    request_id,
                    digest,
                    target_id,
                    repository,
                    source_revision,
                    state,
                    thread_id,
                    prompt_sha256,
                    prompt_bytes,
                    created_at,
                    self._now(),
                ),
            )
        receipt = self.lookup(request_id)
        if receipt is None:
            raise ValidationError(f"unknown request id: {request_id}")
        return receipt

    def release(self, request_id: str) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM reservations WHERE request_id = ?",
                (request_id,),
            )
            connection.execute(
                "DELETE FROM repository_leases WHERE request_id = ?",
                (request_id,),
            )

    def lookup(self, request_id: str) -> TargetReceipt | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return None if row is None else self._receipt(row)

    def active_count(self) -> int:
        with self._connect() as connection:
            return self._active_count(connection, None)
