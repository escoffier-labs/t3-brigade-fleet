from __future__ import annotations

import sqlite3
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .errors import (
    MISSING_RECEIPT_GRACE_SECONDS,
    bounded_target_error,
    bounded_resolution,
    HostUnavailable,
    IdempotencyConflict,
    PolicyRejected,
    PolicyRevisionConflict,
    ValidationError,
)
from .policy import load_stored_decision, load_stored_envelope
from .transport import (
    admission_identity,
    admission_matches_plan,
    normalize_admission,
    normalize_model_plan,
    plan_identity,
)

TRANSITIONS: dict[str, frozenset[str]] = {
    "queued": frozenset({"preflight", "blocked_offline"}),
    "preflight": frozenset({"dispatching", "queued", "blocked_offline", "failed"}),
    # `queued` only when the target answered TargetBusy: it started nothing.
    "dispatching": frozenset({"running", "needs_attention", "unknown", "failed", "queued"}),
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
class PolicySessionRecord:
    """One durable Fleet Hub policy session, linked to the local request."""

    session_id: str
    consumer: str
    repository: str
    origin: str
    workload: str
    policy_version: int
    policy_digest: str
    envelope: dict[str, Any]
    decision: dict[str, Any] | None
    reservation_state: str
    request_id: str | None
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class SettleReceipt:
    """The controller's record that a request's thread was settled (or found gone).

    `request_updated_at` is the request's own `updated_at` when it was settled.
    A follow-up moves the request out of and back into a terminal state with a
    newer `updated_at`, which makes the same request a settle candidate again.
    """

    request_id: str
    thread_id: str
    host: str
    outcome: str
    sequence: int | None
    unlinked: int
    request_updated_at: float
    settled_at: float


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
    # A planned seat and an admitted one are different facts and are kept
    # in different columns. Under enrollment the controller only ever holds
    # the plan; the admission is the target's, taken at launch.
    admission: dict[str, Any] | None = None
    plan: dict[str, Any] | None = None
    target_error: dict[str, str] | None = None
    resolution: dict[str, Any] | None = None


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
                    PRIMARY KEY (host, repository, request_id)
                );
                CREATE TABLE IF NOT EXISTS host_failures (
                    host TEXT PRIMARY KEY,
                    failure_count INTEGER NOT NULL,
                    circuit_until REAL
                );
                CREATE TABLE IF NOT EXISTS policy_sessions (
                    session_id TEXT PRIMARY KEY,
                    consumer TEXT NOT NULL,
                    repository TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    workload TEXT NOT NULL,
                    policy_version INTEGER NOT NULL,
                    policy_digest TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    decision_json TEXT,
                    reservation_state TEXT NOT NULL DEFAULT 'active',
                    request_id TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS policy_sessions_request
                    ON policy_sessions(request_id);
                CREATE TABLE IF NOT EXISTS delegations (
                    delegation_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    parent_request_id TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS settle_receipts (
                    request_id TEXT PRIMARY KEY REFERENCES requests(request_id),
                    thread_id TEXT NOT NULL,
                    host TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    sequence INTEGER,
                    unlinked INTEGER NOT NULL DEFAULT 0,
                    request_updated_at REAL NOT NULL,
                    settled_at REAL NOT NULL
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
            if "admission_json" not in columns:
                connection.execute("ALTER TABLE requests ADD COLUMN admission_json TEXT")
            if "plan_json" not in columns:
                connection.execute("ALTER TABLE requests ADD COLUMN plan_json TEXT")
            for name in ("target_error_json", "resolution_json"):
                if name not in columns:
                    connection.execute(f"ALTER TABLE requests ADD COLUMN {name} TEXT")

        self._migrate_leases()

    def _migrate_leases(self) -> None:
        """Rebuild a pre-concurrency ``leases`` table in place.

        The original table keyed leases by ``(host, repository)``, which
        allowed only one active request per repository per host. The current
        key adds ``request_id`` so a host can hold several slots for one
        repository. Existing rows are copied unchanged.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            key = [
                row["name"]
                for row in sorted(
                    (
                        row
                        for row in connection.execute("PRAGMA table_info(leases)")
                        if row["pk"]
                    ),
                    key=lambda row: row["pk"],
                )
            ]
            if key == ["host", "repository", "request_id"]:
                return
            connection.execute(
                """
                CREATE TABLE leases_multi_slot (
                    host TEXT NOT NULL,
                    repository TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (host, repository, request_id)
                )
                """
            )
            connection.execute(
                """
                INSERT INTO leases_multi_slot (host, repository, request_id, expires_at)
                SELECT host, repository, request_id, expires_at FROM leases
                """
            )
            connection.execute("DROP TABLE leases")
            connection.execute("ALTER TABLE leases_multi_slot RENAME TO leases")

    @staticmethod
    def _record(row: sqlite3.Row) -> RequestRecord:
        admission = (
            normalize_admission(json.loads(row["admission_json"]))
            if row["admission_json"]
            else None
        )
        plan = (
            normalize_model_plan(json.loads(row["plan_json"]))
            if row["plan_json"]
            else None
        )
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
            admission=admission,
            plan=plan,
            target_error=bounded_target_error(json.loads(row["target_error_json"]))
            if row["target_error_json"] else None,
            resolution=bounded_resolution(json.loads(row["resolution_json"]))
            if row["resolution_json"] else None,
        )

    def record_diagnostics(
        self, request_id: str, *, target_error: object = None, resolution: object = None
    ) -> None:
        target_error = bounded_target_error(target_error)
        resolution = bounded_resolution(resolution)
        with self._connect() as connection:
            if target_error is not None:
                connection.execute(
                    "UPDATE requests SET target_error_json = ? WHERE request_id = ?",
                    (json.dumps(target_error), request_id),
                )
            connection.execute(
                "UPDATE requests SET resolution_json = ? WHERE request_id = ?",
                (json.dumps(resolution) if resolution is not None else None, request_id),
            )

    def blocking_requests(self, host: str) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM requests WHERE host = ? ORDER BY created_at, request_id", (host,)
            ).fetchall()
        return [record.request_id for row in rows
                if (record := self._record(row)).state == "unknown"
                or (record.resolution is not None and record.resolution.get("blocking", True))]

    def blocking_resolution(self, host: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM requests WHERE host = ? ORDER BY created_at, request_id", (host,)
            ).fetchall()
        for row in rows:
            record = self._record(row)
            if record.resolution is not None and record.resolution.get("blocking", True):
                return record.resolution
            if record.state == "unknown":
                # Uses the same timestamp and grace as recover's missing receipt path.
                resolution = {"reason": "submit-unresolved"}
                if record.thread_id is None:
                    resolution["deadline"] = datetime.fromtimestamp(
                        record.updated_at + MISSING_RECEIPT_GRACE_SECONDS, UTC
                    ).isoformat().replace("+00:00", "Z")
                return resolution
        if self.circuit_is_open(host):
            return {"reason": "host-circuit-open"}
        return None

    def create_request(
        self,
        request_id: str,
        digest: str,
        host: str,
        repository: str,
        *,
        prompt_bytes: int = 0,
        admission: Mapping[str, Any] | None = None,
        plan: Mapping[str, Any] | None = None,
        replan: bool = False,
    ) -> RequestRecord:
        """Journal one request, or return the durable one that already exists.

        `replan` is for a retried attempt at work that never started. The
        request identity is the caller's payload digest and it still has to
        match exactly, but the machine and the seat a previous attempt was
        planned onto are not identity: they are the output of a route that
        failed. An unstarted record therefore takes the new selection instead
        of conflicting with it, which is what stops a retry from replaying the
        placement that already could not run.
        """
        normalized_admission = (
            normalize_admission(dict(admission)) if admission is not None else None
        )
        normalized_plan = (
            normalize_model_plan(dict(plan)) if plan is not None else None
        )
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
                if replan and record.state in {"queued", "blocked_offline"}:
                    connection.execute(
                        """
                        UPDATE requests
                           SET host = ?, repository = ?, admission_json = ?,
                               plan_json = ?, updated_at = ?
                         WHERE request_id = ?
                        """,
                        (
                            host,
                            repository,
                            json.dumps(
                                normalized_admission,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                            if normalized_admission is not None
                            else None,
                            json.dumps(
                                normalized_plan, sort_keys=True, separators=(",", ":")
                            )
                            if normalized_plan is not None
                            else None,
                            now,
                            request_id,
                        ),
                    )
                    # Read back on this connection, inside the transaction. A
                    # second connection would still be seeing the old row.
                    return self._record(
                        connection.execute(
                            "SELECT * FROM requests WHERE request_id = ?",
                            (request_id,),
                        ).fetchone()
                    )
                existing_identity = (
                    admission_identity(record.admission)
                    if record.admission is not None
                    else None
                )
                incoming_identity = (
                    admission_identity(normalized_admission)
                    if normalized_admission is not None
                    else None
                )
                if existing_identity != incoming_identity:
                    raise IdempotencyConflict(
                        f"request {request_id} already exists with a different model admission"
                    )
                existing_plan = (
                    plan_identity(record.plan) if record.plan is not None else None
                )
                incoming_plan = (
                    plan_identity(normalized_plan)
                    if normalized_plan is not None
                    else None
                )
                if existing_plan != incoming_plan:
                    raise IdempotencyConflict(
                        f"request {request_id} already exists with a different model plan"
                    )
                return record
            connection.execute(
                """
                INSERT INTO requests
                    (request_id, digest, host, repository, state, prompt_bytes, created_at,
                     updated_at, admission_json, plan_json)
                VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)
                """,
                (request_id, digest, host, repository, prompt_bytes, now, now,
                 json.dumps(normalized_admission, sort_keys=True, separators=(",", ":"))
                 if normalized_admission is not None else None,
                 json.dumps(normalized_plan, sort_keys=True, separators=(",", ":"))
                 if normalized_plan is not None else None),
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
        admission_value = receipt.get("model_admission")
        admission = (
            normalize_admission(admission_value)
            if admission_value is not None
            else None
        )
        plan_value = receipt.get("model_plan")
        plan = normalize_model_plan(plan_value) if plan_value is not None else None
        if plan is not None and (
            (admission is None and state != "failed")
            or (admission is not None and not admission_matches_plan(admission, plan))
        ):
            raise PolicyRejected(
                "target receipt model admission does not match its model plan"
            )
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
                "SELECT digest, host, repository, legacy, admission_json, plan_json "
                "FROM requests WHERE request_id = ?",
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
            if existing is not None and existing["admission_json"]:
                existing_admission = normalize_admission(
                    json.loads(existing["admission_json"])
                )
                if (
                    admission is None
                    or admission_identity(existing_admission)
                    != admission_identity(admission)
                ):
                    raise PolicyRejected(
                        "target receipt model admission does not match controller admission"
                    )
            if existing is not None and existing["plan_json"]:
                # Under enrollment the controller holds a plan, never an
                # admission. The target is the only party that may admit, so
                # what comes back has to be a real admission for exactly the
                # seat identity that was planned.
                existing_plan = normalize_model_plan(json.loads(existing["plan_json"]))
                if plan is not None and plan_identity(plan) != plan_identity(
                    existing_plan
                ):
                    raise PolicyRejected(
                        "target receipt model plan does not match controller model plan"
                    )
                if admission is None:
                    # A target can fail after allocating a thread but before
                    # launch admission. Reconcile that failure without turning
                    # its plan into proof that any provider turn was admitted.
                    if host != existing["host"] or repository != existing["repository"]:
                        raise PolicyRejected(
                            "target receipt does not match the controller request binding"
                        )
                    incoming_plan = plan
                    if (
                        state != "failed"
                        or incoming_plan is None
                        or plan_identity(incoming_plan) != plan_identity(existing_plan)
                    ):
                        raise PolicyRejected(
                            "target receipt model admission does not match the controller model plan"
                        )
                elif not admission_matches_plan(admission, existing_plan):
                    raise PolicyRejected(
                        "target receipt model admission does not match the controller model plan"
                    )
                plan = existing_plan if plan is None else plan
            connection.execute(
                """
                INSERT INTO requests (
                    request_id, digest, host, repository, state, prompt_bytes,
                    thread_id, created_at, updated_at, source_revision,
                    prompt_sha256, legacy, admission_json, plan_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    legacy = excluded.legacy,
                    admission_json = excluded.admission_json,
                    plan_json = excluded.plan_json
                """,
                (
                    request_id,
                    digest,
                    host,
                    repository,
                    state,
                    prompt_bytes,
                    thread_id,
                    created_at,
                    updated_at,
                    source_revision,
                    prompt_sha256,
                    int(legacy),
                    json.dumps(admission, sort_keys=True, separators=(",", ":"))
                    if admission is not None
                    else None,
                    json.dumps(plan, sort_keys=True, separators=(",", ":"))
                    if plan is not None
                    else None,
                ),
            )
        return self.get(request_id)

    @staticmethod
    def _policy_session(row: sqlite3.Row) -> PolicySessionRecord:
        return PolicySessionRecord(
            session_id=row["session_id"],
            consumer=row["consumer"],
            repository=row["repository"],
            origin=row["origin"],
            workload=row["workload"],
            policy_version=row["policy_version"],
            policy_digest=row["policy_digest"],
            envelope=load_stored_envelope(json.loads(row["envelope_json"])),
            decision=(
                load_stored_decision(json.loads(row["decision_json"]))
                if row["decision_json"]
                else None
            ),
            reservation_state=row["reservation_state"],
            request_id=row["request_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def record_policy_envelope(
        self,
        session_id: str,
        *,
        consumer: str,
        repository: str,
        origin: str,
        workload: str,
        envelope: Mapping[str, Any],
        preserve_decision: bool = False,
    ) -> PolicySessionRecord:
        """Persist the resolved envelope, dropping a decision made under an
        older policy version so it can never be replayed as current."""
        stored = load_stored_envelope(dict(envelope))
        encoded = json.dumps(stored, sort_keys=True, separators=(",", ":"))
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM policy_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO policy_sessions
                        (session_id, consumer, repository, origin, workload,
                         policy_version, policy_digest, envelope_json,
                         decision_json, request_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                    """,
                    (
                        session_id,
                        consumer,
                        repository,
                        origin,
                        workload,
                        stored["version"],
                        stored["digest"],
                        encoded,
                        now,
                        now,
                    ),
                )
            else:
                if existing["repository"] != repository or existing["consumer"] != consumer:
                    raise IdempotencyConflict(
                        f"policy session {session_id} already exists for another request"
                    )
                unchanged = (
                    existing["policy_version"] == stored["version"]
                    and existing["policy_digest"] == stored["digest"]
                )
                if (
                    preserve_decision
                    and not unchanged
                    and existing["decision_json"] is not None
                ):
                    # A late resolver cannot erase a newer owner's reservation.
                    # Outbox retries use a new session once the old one ends.
                    raise HostUnavailable(
                        "saved reservation awaits release or expiry under the new policy",
                        reason="capacity-exhausted",
                    )
                connection.execute(
                    """
                    UPDATE policy_sessions
                       SET origin = ?, workload = ?, policy_version = ?,
                           policy_digest = ?, envelope_json = ?,
                           decision_json = CASE WHEN ? THEN decision_json ELSE NULL END,
                           updated_at = ?
                     WHERE session_id = ?
                    """,
                    (
                        origin,
                        workload,
                        stored["version"],
                        stored["digest"],
                        encoded,
                        1 if unchanged else 0,
                        now,
                        session_id,
                    ),
                )
        session = self.get_policy_session(session_id)
        if session is None:
            raise ValidationError(f"unknown policy session: {session_id}")
        return session

    def record_policy_decision(
        self,
        session_id: str,
        decision: Mapping[str, Any],
    ) -> PolicySessionRecord:
        stored = load_stored_decision(dict(decision))
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT policy_version, policy_digest FROM policy_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if existing is None:
                raise ValidationError(f"unknown policy session: {session_id}")
            if (
                existing["policy_version"] != stored["policy_version"]
                or existing["policy_digest"] != stored["policy_digest"]
            ):
                raise PolicyRevisionConflict(
                    "fleet policy decision does not match the resolved policy"
                )
            connection.execute(
                """
                UPDATE policy_sessions
                   SET decision_json = ?, reservation_state = 'active', updated_at = ?
                 WHERE session_id = ?
                """,
                (
                    json.dumps(stored, sort_keys=True, separators=(",", ":")),
                    now,
                    session_id,
                ),
            )
        session = self.get_policy_session(session_id)
        if session is None:
            raise ValidationError(f"unknown policy session: {session_id}")
        return session

    def record_delegation(
        self,
        delegation_id: str,
        *,
        request_id: str,
        parent_request_id: str,
    ) -> str:
        """Bind one delegation to exactly one local execution request.

        One adoption is one execution. Both directions are unique in the
        schema, so a second submit for the same delegation either finds the
        same binding and replays it, or loses the race and conflicts; it can
        never open a second execution. The parent's own request id is kept in
        its own column, because it is the parent's identity and not this
        controller's.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT request_id, parent_request_id FROM delegations "
                "WHERE delegation_id = ?",
                (delegation_id,),
            ).fetchone()
            if row is not None:
                if (
                    row["request_id"] != request_id
                    or row["parent_request_id"] != parent_request_id
                ):
                    raise IdempotencyConflict(
                        f"delegation {delegation_id} is already bound to another request"
                    )
                return row["request_id"]
            claimed = connection.execute(
                "SELECT delegation_id FROM delegations WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if claimed is not None:
                raise IdempotencyConflict(
                    f"request {request_id} is already bound to another delegation"
                )
            connection.execute(
                "INSERT INTO delegations "
                "(delegation_id, request_id, parent_request_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (delegation_id, request_id, parent_request_id, self._clock()),
            )
        return request_id

    def delegation_binding(self, delegation_id: str) -> tuple[str, str] | None:
        """The request id and parent request id one delegation already adopted."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT request_id, parent_request_id FROM delegations "
                "WHERE delegation_id = ?",
                (delegation_id,),
            ).fetchone()
        return None if row is None else (row["request_id"], row["parent_request_id"])

    def set_reservation_state(self, session_id: str, state: str) -> None:
        """Record a reservation the Fleet Hub confirmed it took back."""
        if state not in {"active", "released"}:
            raise ValidationError(f"unsupported reservation state: {state}")
        with self._connect() as connection:
            connection.execute(
                "UPDATE policy_sessions SET reservation_state = ?, updated_at = ? WHERE session_id = ?",
                (state, self._clock(), session_id),
            )

    def link_policy_request(self, session_id: str, request_id: str) -> None:
        """Bind the session to the first local request that used it."""
        with self._connect() as connection:
            connection.execute(
                "UPDATE policy_sessions SET request_id = ? WHERE session_id = ? AND request_id IS NULL",
                (request_id, session_id),
            )

    def get_policy_session(self, session_id: str) -> PolicySessionRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM policy_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        return self._policy_session(row) if row is not None else None

    def policy_session_for_request(self, request_id: str) -> PolicySessionRecord | None:
        """Return the latest binding, including renewed fixed-ID placement.

        Prior expired sessions remain evidence. Insertion order also handles
        equal wall-clock timestamps without selecting the old reservation.
        """
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM policy_sessions WHERE request_id = ? ORDER BY rowid DESC LIMIT 1",
                (request_id,),
            ).fetchone()
        return self._policy_session(row) if row is not None else None

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

    @staticmethod
    def _claim_lease_slot(
        connection: sqlite3.Connection,
        *,
        host: str,
        repository: str,
        request_id: str,
        expires_at: float,
        now: float,
        limit: int,
    ) -> bool:
        """Take or refresh one repository slot inside an open transaction."""
        connection.execute("DELETE FROM leases WHERE expires_at <= ?", (now,))
        held = connection.execute(
            """
            SELECT 1 FROM leases
            WHERE host = ? AND repository = ? AND request_id = ?
            """,
            (host, repository, request_id),
        ).fetchone()
        if held is None:
            others = connection.execute(
                "SELECT COUNT(*) AS active FROM leases WHERE host = ? AND repository = ?",
                (host, repository),
            ).fetchone()["active"]
            if others >= limit:
                return False
        connection.execute(
            """
            INSERT INTO leases (host, repository, request_id, expires_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(host, repository, request_id) DO UPDATE SET
                expires_at = excluded.expires_at
            """,
            (host, repository, request_id, expires_at),
        )
        return True

    def acquire_lease(
        self,
        host: str,
        repository: str,
        request_id: str,
        *,
        ttl: float,
        limit: int = 1,
    ) -> bool:
        if limit < 1:
            raise ValidationError("repository lease limit must be at least 1")
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            return self._claim_lease_slot(
                connection,
                host=host,
                repository=repository,
                request_id=request_id,
                expires_at=now + ttl,
                now=now,
                limit=limit,
            )

    def renew_lease(
        self,
        host: str,
        repository: str,
        request_id: str,
        *,
        ttl: float,
    ) -> None:
        """Refresh, or re-create, the lease of a request that is still live.

        There is no limit check. A running request occupies its repository
        whether or not its lease row expired, so it is counted again rather
        than reported as lost.
        """
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO leases (host, repository, request_id, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(host, repository, request_id) DO UPDATE SET
                    expires_at = excluded.expires_at
                """,
                (host, repository, request_id, now + ttl),
            )

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
        repository_limit: int = 1,
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
            if not self._claim_lease_slot(
                connection,
                host=row["host"],
                repository=row["repository"],
                request_id=request_id,
                expires_at=now + ttl,
                now=now,
                limit=repository_limit,
            ):
                raise HostUnavailable(
                    f"repository is busy on host {row['host']}"
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

    def settle_candidates(
        self, *, states: frozenset[str], terminal_before: float
    ) -> list[RequestRecord]:
        """Terminal requests with a thread that have not been settled since they last finished."""
        placeholders = ",".join("?" for _ in states)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT r.* FROM requests r
                LEFT JOIN settle_receipts s ON s.request_id = r.request_id
                WHERE r.state IN ({placeholders})
                  AND r.thread_id IS NOT NULL
                  AND r.updated_at <= ?
                  AND (s.request_id IS NULL OR s.request_updated_at < r.updated_at)
                ORDER BY r.updated_at, r.request_id
                """,
                (*sorted(states), terminal_before),
            ).fetchall()
        return [self._record(row) for row in rows]

    def record_settle(
        self,
        record: RequestRecord,
        *,
        outcome: str,
        sequence: int | None = None,
        unlinked: int = 0,
    ) -> SettleReceipt:
        """Write the settle receipt for the request state the sweep acted on.

        Keyed by the `updated_at` the sweep read, not by a fresh read, so a
        follow-up that landed mid-sweep still leaves the request a candidate.
        """
        now = self._clock()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO settle_receipts
                    (request_id, thread_id, host, outcome, sequence, unlinked,
                     request_updated_at, settled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.request_id,
                    record.thread_id,
                    record.host,
                    outcome,
                    sequence,
                    unlinked,
                    record.updated_at,
                    now,
                ),
            )
        receipt = self.settle_receipt(record.request_id)
        assert receipt is not None
        return receipt

    def settle_receipt(self, request_id: str) -> SettleReceipt | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM settle_receipts WHERE request_id = ?", (request_id,)
            ).fetchone()
        if row is None:
            return None
        return SettleReceipt(
            request_id=row["request_id"],
            thread_id=row["thread_id"],
            host=row["host"],
            outcome=row["outcome"],
            sequence=row["sequence"],
            unlinked=row["unlinked"],
            request_updated_at=row["request_updated_at"],
            settled_at=row["settled_at"],
        )

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
