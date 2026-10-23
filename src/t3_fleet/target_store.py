from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from .config import DEFAULT_REPOSITORY_LEASE_TTL_SECONDS
from .errors import IdempotencyConflict, TargetBusy, ValidationError
from .policy import normalize_policy_receipt
from .reporting import HEARTBEAT_SECONDS, SCHEMA, validate_context
from .transport import (
    admission_identity,
    admission_matches_plan,
    normalize_admission,
    normalize_model_plan,
    plan_identity,
)

_TERMINAL_STATES = frozenset({"succeeded", "failed", "interrupted"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
LAUNCH_GRACE_SECONDS = 30


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
    admission: dict[str, Any] | None = None
    plan: dict[str, Any] | None = None
    policy: dict[str, Any] | None = None
    execution_context: dict[str, Any] | None = None


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
    # The plan is what this target was told to launch; the admission is
    # what the Fleet Hub actually granted at launch. A claimed request has
    # a plan and no admission until the launch admission lands.
    admission: dict[str, Any] | None = None
    plan: dict[str, Any] | None = None
    policy: dict[str, Any] | None = None
    execution_context: dict[str, Any] | None = None
    reservation_state: str = "active"
    observation_applied: bool = field(default=True, compare=False)
    observation_version: int = 0
    lifecycle: dict[str, Any] | None = None


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
        "execution_context": receipt.execution_context,
        **(
            {"model_admission": receipt.admission}
            if receipt.admission is not None
            else {}
        ),
        **({"model_plan": receipt.plan} if receipt.plan is not None else {}),
        **({"resolution": receipt.lifecycle["resolution"]}
           if (receipt.lifecycle or {}).get("resolution") else {}),
    }


class TargetStore:
    def __init__(
        self,
        path: str | Path,
        *,
        # Wall-clock, not time.monotonic: reservation expiry is an absolute
        # timestamp compared across agent process restarts and reboots. A
        # monotonic value written by a previous process is meaningless to a
        # new one, so stale reservations never expired and their repository
        # lease wedged every later submit (surfaced as unknown_state).
        clock: Callable[[], float] = time.time,
        now: Callable[[], str] = _utc_now,
    ) -> None:
        self.path = str(path)
        self._clock = clock
        self._now = now
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        self._enable_wal(connection)
        return connection

    @staticmethod
    def _enable_wal(connection: sqlite3.Connection) -> None:
        """Put the journal in WAL mode, tolerating parallel first opens.

        Converting a new database to WAL takes an exclusive lock, and SQLite
        does not apply the busy timeout to that pragma. Agent processes that
        open a fresh target store at the same moment would otherwise fail with
        "database is locked". Once the file is in WAL mode the pragma is a
        no-op and returns at once.
        """
        deadline = time.monotonic() + 10
        while True:
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as error:
                if "locked" not in str(error) or time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

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
                    ,admission_json TEXT
                    ,plan_json TEXT
                    ,policy_json TEXT
                    ,reservation_state TEXT NOT NULL DEFAULT 'active'
                );
                CREATE TABLE IF NOT EXISTS reservations (
                    request_id TEXT PRIMARY KEY REFERENCES target_requests(request_id),
                    expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS repository_leases (
                    repository TEXT NOT NULL,
                    request_id TEXT NOT NULL REFERENCES target_requests(request_id),
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (repository, request_id)
                );
                """
            )
            connection.execute("BEGIN IMMEDIATE")
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(target_requests)")}
            if "admission_json" not in columns:
                connection.execute("ALTER TABLE target_requests ADD COLUMN admission_json TEXT")
            if "policy_json" not in columns:
                connection.execute("ALTER TABLE target_requests ADD COLUMN policy_json TEXT")
            if "plan_json" not in columns:
                connection.execute("ALTER TABLE target_requests ADD COLUMN plan_json TEXT")
            if "reservation_state" not in columns:
                connection.execute(
                    "ALTER TABLE target_requests "
                    "ADD COLUMN reservation_state TEXT NOT NULL DEFAULT 'active'"
                )
            for name, declaration in (
                ("execution_context_json", "TEXT"),
                ("lifecycle_json", "TEXT"),
                ("observation_version", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE target_requests ADD COLUMN {name} {declaration}"
                    )
        self._migrate_repository_leases()

    def _migrate_repository_leases(self) -> None:
        """Rebuild a pre-concurrency ``repository_leases`` table in place.

        The original table was keyed by repository alone, so a target could
        hold only one active request per repository, and leases had no expiry.
        Existing rows are copied with a fresh default lease lifetime so they
        keep their slot until a terminal reconcile or the new expiry.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(repository_leases)")
            }
            if "expires_at" in columns:
                return
            connection.execute(
                """
                CREATE TABLE repository_leases_multi_slot (
                    repository TEXT NOT NULL,
                    request_id TEXT NOT NULL REFERENCES target_requests(request_id),
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (repository, request_id)
                )
                """
            )
            connection.execute(
                """
                INSERT INTO repository_leases_multi_slot (repository, request_id, expires_at)
                SELECT repository, request_id, ? FROM repository_leases
                """,
                (self._clock() + DEFAULT_REPOSITORY_LEASE_TTL_SECONDS,),
            )
            connection.execute("DROP TABLE repository_leases")
            connection.execute(
                "ALTER TABLE repository_leases_multi_slot RENAME TO repository_leases"
            )

    @staticmethod
    def _receipt(row: sqlite3.Row) -> TargetReceipt:
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
        policy = (
            normalize_policy_receipt(json.loads(row["policy_json"]))
            if row["policy_json"]
            else None
        )
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
            admission=admission,
            plan=plan,
            policy=policy,
            reservation_state=row["reservation_state"] or "active",
            execution_context=json.loads(row["execution_context_json"])
            if row["execution_context_json"]
            else None,
            lifecycle=json.loads(row["lifecycle_json"])
            if row["lifecycle_json"]
            else {},
            observation_version=row["observation_version"],
        )

    @staticmethod
    def _purge_expired(connection: sqlite3.Connection, now_wall: float) -> None:
        connection.execute(
            "DELETE FROM repository_leases WHERE expires_at <= ?",
            (now_wall,),
        )
        connection.execute(
            """
            DELETE FROM repository_leases
            WHERE request_id IN (
                SELECT r.request_id FROM reservations r
                JOIN target_requests t ON t.request_id = r.request_id
                WHERE r.expires_at <= ? AND t.thread_id IS NULL
            )
            """,
            (now_wall,),
        )
        connection.execute(
            """
            DELETE FROM reservations
            WHERE expires_at <= ?
              AND request_id IN (
                  SELECT request_id FROM target_requests
                  WHERE thread_id IS NULL
              )
            """,
            (now_wall,),
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
            (exclude_request_id, self._clock()),
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
        *,
        limit: int,
        expires_at: float,
    ) -> None:
        if repository is None:
            return
        held = connection.execute(
            "SELECT 1 FROM repository_leases WHERE repository = ? AND request_id = ?",
            (repository, request_id),
        ).fetchone()
        if held is None:
            others = connection.execute(
                "SELECT COUNT(*) AS active FROM repository_leases WHERE repository = ?",
                (repository,),
            ).fetchone()["active"]
            if others >= limit:
                raise TargetBusy(
                    "repository is busy on this target "
                    f"({limit} concurrent request limit)",
                    reason="repository-busy",
                )
        connection.execute(
            """
            INSERT INTO repository_leases (repository, request_id, expires_at)
            VALUES (?, ?, ?)
            ON CONFLICT(repository, request_id) DO UPDATE SET
                expires_at = excluded.expires_at
            """,
            (repository, request_id, expires_at),
        )

    @staticmethod
    def _check_resolution(connection: sqlite3.Connection, *, exclude: str | None = None) -> None:
        for row in connection.execute(
            "SELECT lifecycle_json FROM target_requests WHERE request_id IS NOT ?", (exclude,)
        ):
            resolution = json.loads(row["lifecycle_json"] or "{}").get("resolution")
            if resolution is not None and resolution.get("blocking", True):
                raise TargetBusy("target has an unresolved terminal projection", reason="host-ineligible")

    def claim(
        self,
        claim: TargetClaim,
        *,
        active_limit: int | None,
        reservation_ttl: float,
        repository_limit: int = 1,
        lease_ttl: float = DEFAULT_REPOSITORY_LEASE_TTL_SECONDS,
    ) -> TargetReceipt:
        context = (
            validate_context(claim.execution_context)
            if claim.execution_context is not None
            else None
        )
        admission = (
            normalize_admission(claim.admission)
            if claim.admission is not None
            else None
        )
        plan = normalize_model_plan(claim.plan) if claim.plan is not None else None
        policy = (
            normalize_policy_receipt(claim.policy)
            if claim.policy is not None
            else None
        )
        now_wall = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._purge_expired(connection, now_wall)
            self._check_resolution(connection, exclude=claim.request_id)
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
                stored_admission = (
                    normalize_admission(json.loads(row["admission_json"]))
                    if row["admission_json"]
                    else None
                )
                stored_plan = (
                    normalize_model_plan(json.loads(row["plan_json"]))
                    if row["plan_json"]
                    else None
                )
                if (
                    plan_identity(stored_plan) if stored_plan is not None else None
                ) != (plan_identity(plan) if plan is not None else None):
                    raise IdempotencyConflict(
                        f"request {claim.request_id} has mismatched model plan"
                    )
                if plan is None:
                    # Off enrollment the claim carries the admission itself, so
                    # the admission is the immutable identity of the request.
                    stored_identity = (
                        admission_identity(stored_admission)
                        if stored_admission is not None
                        else None
                    )
                    incoming_identity = (
                        admission_identity(admission) if admission is not None else None
                    )
                    if stored_identity != incoming_identity:
                        raise IdempotencyConflict(
                            f"request {claim.request_id} has mismatched model admission"
                        )
                elif stored_admission is not None and not admission_matches_plan(
                    stored_admission, plan
                ):
                    # Under enrollment the admission was recorded at launch and
                    # is not the claim's to restate. A replay of the same plan
                    # must still name the seat that was actually admitted.
                    raise IdempotencyConflict(
                        f"request {claim.request_id} was admitted against another model plan"
                    )
            if active_limit is not None and self._active_count(connection, claim.request_id) >= active_limit:
                raise TargetBusy(
                    f"target capacity is exhausted at {active_limit} active requests",
                    reason="capacity-exhausted",
                )
            if row is not None and row["execution_context_json"] is not None:
                # Upgraded controllers add derived fields on exact replay.
                # Preserve existing fields when an older controller replays,
                # but reject any changed value already attached to this run.
                stored_context = json.loads(row["execution_context_json"])
                merged = dict(stored_context)
                for key, value in (context or {}).items():
                    if key in merged and key in {"controller_id", "origin_node"}:
                        # These fields are derived from controller config, not
                        # caller identity. Retain original launch attribution.
                        continue
                    if key in merged and merged[key] != value:
                        raise IdempotencyConflict("request execution context changed")
                    merged[key] = value
                context = validate_context(merged)
            if row is not None and row["thread_id"] is None and row["state"] in _TERMINAL_STATES:
                # A reconciled pre-launch failure is final on exact claim replay.
                return self._receipt(row)
            if row is None:
                now = self._now()
                connection.execute(
                    """
                    INSERT INTO target_requests
                        (request_id, digest, target_id, repository, source_revision,
                         state, thread_id, prompt_sha256, prompt_bytes,
                         created_at, updated_at, legacy, admission_json, plan_json,
                         policy_json, reservation_state)
                    VALUES (?, ?, ?, ?, ?, 'preflight', NULL, ?, ?, ?, ?, 0, ?, ?, ?, 'active')
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
                        json.dumps(admission, sort_keys=True, separators=(",", ":"))
                        if admission is not None else None,
                        json.dumps(plan, sort_keys=True, separators=(",", ":"))
                        if plan is not None else None,
                        json.dumps(policy, sort_keys=True, separators=(",", ":"))
                        if policy is not None else None,
                    ),
                )
            if context is not None:
                connection.execute(
                    "UPDATE target_requests SET execution_context_json = ? WHERE request_id = ?",
                    (json.dumps(context, sort_keys=True), claim.request_id),
                )
            if active_limit is None:
                # Direct submit keeps the legacy live-T3 admission path. Track
                # its occupancy for claims without adding a second admission.
                self._renew_lease(
                    connection, claim.repository, claim.request_id,
                    expires_at=now_wall + lease_ttl,
                )
            else:
                self._ensure_lease(
                    connection,
                    claim.repository,
                    claim.request_id,
                    limit=repository_limit,
                    expires_at=now_wall + lease_ttl,
                )
            self._upsert_reservation(
                connection,
                claim.request_id,
                now_wall + reservation_ttl,
            )
        receipt = self.lookup(claim.request_id)
        if receipt is None:
            raise ValidationError(f"unknown request id: {claim.request_id}")
        return receipt

    def attach_thread(self, request_id: str, thread_id: str) -> TargetReceipt:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "UPDATE target_requests SET thread_id = ?, state = 'running', updated_at = ?, observation_version = observation_version + 1 WHERE request_id = ?",
                (thread_id, self._now(), request_id),
            )
            if cursor.rowcount != 1:
                raise ValidationError(f"unknown request id: {request_id}")
        receipt = self.lookup(request_id)
        if receipt is None:
            raise ValidationError(f"unknown request id: {request_id}")
        return receipt

    def record_admission(
        self,
        request_id: str,
        admission: dict[str, Any],
    ) -> TargetReceipt:
        """Write down the launch admission this target was actually granted.

        It lands before the provider turn starts, so a durable record of a
        started turn always has the admission that paid for it. Re-recording the
        same admission is an exact retry and is allowed; a different one is a
        conflict, because one request never spends two launches.
        """
        normalized = normalize_admission(admission)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT admission_json, plan_json FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown request id: {request_id}")
            if row["admission_json"]:
                stored = normalize_admission(json.loads(row["admission_json"]))
                if admission_identity(stored) != admission_identity(normalized):
                    raise IdempotencyConflict(
                        f"request {request_id} has mismatched model admission"
                    )
            if row["plan_json"]:
                plan = normalize_model_plan(json.loads(row["plan_json"]))
                if not admission_matches_plan(normalized, plan):
                    raise IdempotencyConflict(
                        f"request {request_id} was admitted against another model plan"
                    )
            connection.execute(
                "UPDATE target_requests SET admission_json = ?, updated_at = ? "
                "WHERE request_id = ?",
                (
                    json.dumps(normalized, sort_keys=True, separators=(",", ":")),
                    self._now(),
                    request_id,
                ),
            )
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
        lease_ttl: float | None = None,
        expected_version: int | None = None,
        rejected_turn: bool = False,
        turn_ref: str | None = None,
        dispatch_finished: bool = False,
        launch_grace_started_at: str | None = None,
        resolution: dict[str, Any] | None = None,
    ) -> TargetReceipt:
        """Record the observed state, and keep a live request's lease alive.

        With `lease_ttl`, a request that is still not terminal gets a fresh
        repository lease window, re-created if it had already expired. A
        request the fleet keeps polling is still occupying its repository, and
        the lease lifetime exists only to free slots that nobody watches.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown request id: {request_id}")
            if (
                expected_version is not None
                and row["observation_version"] != expected_version
            ):
                return replace(self._receipt(row), observation_applied=False)
            lifecycle = json.loads(row["lifecycle_json"]) if row["lifecycle_json"] else {}
            if row["state"] in _TERMINAL_STATES and not lifecycle.get("resolution", {}).get("blocking", False):
                return self._receipt(row)
            if resolution is not None:
                lifecycle["resolution"] = resolution
                if (state == "unknown" and datetime.fromisoformat(self._now().replace("Z", "+00:00"))
                        >= datetime.fromisoformat(resolution["deadline"].replace("Z", "+00:00"))):
                    # This is a failed resolution decision, not proof of the
                    # execution outcome. Admissions remain quarantined until
                    # cessation or fresh identity evidence is confirmed.
                    state = "failed"
            if resolution is not None or lifecycle.pop("resolution", None) is not None:
                connection.execute(
                    "UPDATE target_requests SET lifecycle_json = ? WHERE request_id = ?",
                    (json.dumps(lifecycle, sort_keys=True), request_id),
                )
            if launch_grace_started_at and lifecycle.get("waiting_for_previous_turn"):
                # This lifecycle update shares the observation CAS above. A
                # stale poll must never arm a newer epoch's grace clock.
                lifecycle.update(
                    waiting_for_previous_turn=False,
                    launch_deadline=(
                        datetime.fromisoformat(launch_grace_started_at.replace("Z", "+00:00"))
                        + timedelta(seconds=LAUNCH_GRACE_SECONDS)
                    ).isoformat().replace("+00:00", "Z"),
                )
                connection.execute(
                    "UPDATE target_requests SET lifecycle_json = ? WHERE request_id = ?",
                    (json.dumps(lifecycle, sort_keys=True), request_id),
                )
            if dispatch_finished:
                lifecycle["dispatch_finished"] = True
                if not lifecycle.get("waiting_for_previous_turn"):
                    lifecycle["launch_deadline"] = (
                        datetime.fromisoformat(self._now().replace("Z", "+00:00"))
                        + timedelta(seconds=LAUNCH_GRACE_SECONDS)
                    ).isoformat().replace("+00:00", "Z")
                connection.execute(
                    "UPDATE target_requests SET lifecycle_json = ? WHERE request_id = ?",
                    (json.dumps(lifecycle, sort_keys=True), request_id),
                )
            if turn_ref and turn_ref != lifecycle.get("previous_turn_ref") and turn_ref not in lifecycle.get("previous_turn_refs", []):
                # Identity is launch evidence even while an older event awaits
                # delivery. Never let grace expiry close a newly observed run.
                lifecycle.update(turn_ref=turn_ref, launching=False)
                connection.execute(
                    "UPDATE target_requests SET lifecycle_json = ? WHERE request_id = ?",
                    (json.dumps(lifecycle, sort_keys=True), request_id),
                )
            if rejected_turn:
                state = lifecycle.get("prior_state", state)
                lifecycle["launching"] = False
                if lifecycle.get("previous_turn_ref"):
                    lifecycle["turn_ref"] = lifecycle["previous_turn_ref"]
                connection.execute(
                    "UPDATE target_requests SET lifecycle_json = ? WHERE request_id = ?",
                    (json.dumps(lifecycle, sort_keys=True), request_id),
                )
            if (
                lifecycle.get("reserved_followup")
                and lifecycle.get("reserved_followup_expires_at", float("inf")) <= self._clock()
                and lifecycle.get("reserved_followup_state") in _TERMINAL_STATES
            ):
                # No turn began before this reservation expired. Restore the
                # target's prior terminal state rather than leak capacity.
                state = lifecycle.pop("reserved_followup_state")
                lifecycle.pop("reserved_followup", None)
                lifecycle.pop("reserved_followup_expires_at", None)
                lifecycle["launching"] = False
                connection.execute(
                    "UPDATE target_requests SET lifecycle_json = ? WHERE request_id = ?",
                    (json.dumps(lifecycle, sort_keys=True), request_id),
                )
            next_thread_id = thread_id if thread_id is not None else row["thread_id"]
            connection.execute(
                "UPDATE target_requests SET state = ?, thread_id = ?, updated_at = ?, observation_version = observation_version + 1 WHERE request_id = ?",
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
            elif lease_ttl is not None and next_thread_id is not None:
                self._renew_lease(
                    connection,
                    row["repository"],
                    request_id,
                    expires_at=self._clock() + lease_ttl,
                )
            updated = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            return self._receipt(updated)

    def reserve_followup(
        self,
        request_id: str,
        *,
        active_limit: int,
        reservation_ttl: float,
        repository_limit: int = 1,
        lease_ttl: float = DEFAULT_REPOSITORY_LEASE_TTL_SECONDS,
    ) -> TargetReceipt:
        now_wall = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._check_resolution(connection)
            self._purge_expired(connection, now_wall)
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
                raise TargetBusy(
                    f"target capacity is exhausted at {active_limit} active requests",
                    reason="capacity-exhausted",
                )
            self._ensure_lease(
                connection,
                row["repository"],
                request_id,
                limit=repository_limit,
                expires_at=now_wall + lease_ttl,
            )
            lifecycle = (
                json.loads(row["lifecycle_json"]) if row["lifecycle_json"] else {}
            )
            lifecycle.update(
                reserved_followup=True,
                reserved_followup_expires_at=now_wall + reservation_ttl,
                reserved_followup_state=row["state"],
                launching=True,
                dispatch_finished=False,
                previous_turn_ref=lifecycle.get("turn_ref"),
            )
            connection.execute(
                "UPDATE target_requests SET state = 'dispatching', lifecycle_json = ?, updated_at = ?, observation_version = observation_version + 1 WHERE request_id = ?",
                (json.dumps(lifecycle, sort_keys=True), self._now(), request_id),
            )
            self._upsert_reservation(connection, request_id, now_wall + reservation_ttl)
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
        admission_value = payload.get("model_admission")
        admission = (
            normalize_admission(admission_value)
            if admission_value is not None
            else None
        )
        plan_value = payload.get("model_plan")
        plan = normalize_model_plan(plan_value) if plan_value is not None else None
        policy_value = payload.get("fleet_policy")
        policy = (
            normalize_policy_receipt(policy_value)
            if policy_value is not None
            else None
        )
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
                     created_at, updated_at, legacy, admission_json, plan_json,
                     policy_json, reservation_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, 'active')
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
                    (
                        json.dumps(admission, sort_keys=True, separators=(",", ":"))
                        if admission is not None
                        else None
                    ),
                    (
                        json.dumps(plan, sort_keys=True, separators=(",", ":"))
                        if plan is not None
                        else None
                    ),
                    (
                        json.dumps(policy, sort_keys=True, separators=(",", ":"))
                        if policy is not None
                        else None
                    ),
                ),
            )
        receipt = self.lookup(request_id)
        if receipt is None:
            raise ValidationError(f"unknown request id: {request_id}")
        return receipt

    def set_reservation_state(self, request_id: str, state: str) -> TargetReceipt:
        """Record the Hub reservation generation this target still holds.

        A released reservation is never resurrected here: the target only ever
        moves it forward, so an ambiguous later reading cannot re-take capacity
        the fleet already handed back.
        """
        if state not in {"active", "released"}:
            raise ValidationError(f"unsupported reservation state: {state}")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT reservation_state FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown request id: {request_id}")
            if row["reservation_state"] != "released":
                connection.execute(
                    """
                    UPDATE target_requests
                       SET reservation_state = ?, updated_at = ?
                     WHERE request_id = ?
                    """,
                    (state, self._now(), request_id),
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

    @staticmethod
    def _renew_lease(
        connection: sqlite3.Connection,
        repository: str | None,
        request_id: str,
        *,
        expires_at: float,
    ) -> None:
        """Upsert one lease row for a request that is already running.

        No limit check: the request holds its repository whether or not its
        lease row survived, so the count must include it again.
        """
        if repository is None:
            return
        connection.execute(
            """
            INSERT INTO repository_leases (repository, request_id, expires_at)
            VALUES (?, ?, ?)
            ON CONFLICT(repository, request_id) DO UPDATE SET
                expires_at = excluded.expires_at
            """,
            (repository, request_id, expires_at),
        )

    def forget_unstarted(self, request_id: str) -> None:
        """Drop a claim that was declined before it created anything.

        Only a claim without a thread qualifies. Its row, reservation, and
        lease all go, so a retry of the same request is a fresh claim and is
        not held to the identity of an attempt that never ran.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT thread_id FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None or row["thread_id"] is not None:
                return
            connection.execute(
                "DELETE FROM reservations WHERE request_id = ?",
                (request_id,),
            )
            connection.execute(
                "DELETE FROM repository_leases WHERE request_id = ?",
                (request_id,),
            )
            connection.execute(
                "DELETE FROM target_requests WHERE request_id = ?",
                (request_id,),
            )

    def refresh_lease(self, request_id: str, *, lease_ttl: float) -> None:
        """Give a non-terminal request a fresh repository lease window."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT repository, state FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None or row["state"] in _TERMINAL_STATES:
                return
            self._renew_lease(
                connection,
                row["repository"],
                request_id,
                expires_at=self._clock() + lease_ttl,
            )

    def lookup(self, request_id: str) -> TargetReceipt | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return None if row is None else self._receipt(row)

    def latest_admission(self, repository: str) -> TargetReceipt | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM target_requests
                WHERE repository = ? AND admission_json IS NOT NULL
                ORDER BY updated_at DESC, request_id DESC
                LIMIT 1
                """,
                (repository,),
            ).fetchone()
        return None if row is None else self._receipt(row)

    def active_reservations(self, *, target_id: str) -> list[TargetReceipt]:
        """Receipts this machine owns that still hold a live Hub reservation.

        Ownership is `target_id`, not the request's origin: a source that
        dispatched work elsewhere has no reservation to renew, and a target
        renews only what it is actually running. A request whose outcome is
        unknown is included on purpose, because unknown capacity stays held
        rather than being quietly handed back.
        """
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM target_requests
                 WHERE target_id = ?
                   AND reservation_state = 'active'
                   AND state IN ('running', 'needs_attention', 'unknown')
                   AND policy_json IS NOT NULL
                 ORDER BY created_at ASC, request_id ASC
                """,
                (target_id,),
            ).fetchall()
        return [self._receipt(row) for row in rows]

    def unresolved(self) -> list[dict[str, str]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT lifecycle_json FROM target_requests ORDER BY created_at, request_id"
            ).fetchall()
        return [resolution for row in rows
                if (resolution := json.loads(row["lifecycle_json"] or "{}").get("resolution"))
                and resolution.get("blocking", True)]

    def active_count(self) -> int:
        with self._connect() as connection:
            return self._active_count(connection, None)

    def begin_turn(
        self, request_id: str, *, previous_turn_ref: str | None = None
    ) -> TargetReceipt:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._check_resolution(connection)
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                raise ValidationError("unknown target request")
            lifecycle = (
                json.loads(row["lifecycle_json"]) if row["lifecycle_json"] else {}
            )
            lifecycle.pop("resolution", None)
            epoch = lifecycle.get("epoch", 0) + 1
            started = self._now()
            history = set(lifecycle.get("previous_turn_refs", []))
            recorded_ref = lifecycle.get("turn_ref")
            if recorded_ref and not recorded_ref.startswith("epoch:"):
                history.add(recorded_ref)
            if previous_turn_ref:
                history.add(previous_turn_ref)
            prior_state = lifecycle.get("reserved_followup_state", row["state"])
            waiting_for_previous = bool(
                row["thread_id"] and prior_state not in _TERMINAL_STATES
            )
            lifecycle.update(
                epoch=epoch,
                followup=row["thread_id"] is not None,
                turn_ref=f"epoch:{epoch}",
                previous_turn_ref=previous_turn_ref,
                previous_turn_refs=sorted(history),
                prior_state=prior_state,
                launching=True,
                dispatch_finished=False,
                epoch_started=started,
                waiting_for_previous_turn=waiting_for_previous,
                launch_deadline=None if waiting_for_previous else (
                    datetime.fromisoformat(started.replace("Z", "+00:00"))
                    + timedelta(seconds=LAUNCH_GRACE_SECONDS)
                ).isoformat().replace("+00:00", "Z"),
            )
            lifecycle.pop("reserved_followup", None)
            lifecycle.pop("reserved_followup_expires_at", None)
            lifecycle.pop("reserved_followup_state", None)
            connection.execute(
                "UPDATE target_requests SET state = 'dispatching', lifecycle_json = ?, observation_version = observation_version + 1 WHERE request_id = ?",
                (json.dumps(lifecycle, sort_keys=True), request_id),
            )
        return self.lookup(request_id)

    def confirm_turn(self, request_id: str) -> TargetReceipt:
        """Commit launch progress even when event reporting is unavailable."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                raise ValidationError("unknown target request")
            lifecycle = (
                json.loads(row["lifecycle_json"]) if row["lifecycle_json"] else {}
            )
            lifecycle["launching"] = False
            lifecycle["dispatch_finished"] = True
            state = row["state"] if row["state"] in _TERMINAL_STATES else "running"
            connection.execute(
                "UPDATE target_requests SET lifecycle_json = ?, state = ?, observation_version = observation_version + 1 WHERE request_id = ?",
                (json.dumps(lifecycle, sort_keys=True), state, request_id),
            )
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            return self._receipt(row)

    def mark_observed(self, request_id: str, observed_at: str) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT lifecycle_json FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is not None:
                lifecycle = json.loads(row[0]) if row[0] else {}
                lifecycle["checked_at"] = observed_at
                connection.execute(
                    "UPDATE target_requests SET lifecycle_json = ? WHERE request_id = ?",
                    (json.dumps(lifecycle, sort_keys=True), request_id),
                )

    def mark_interrupt_requested(self, thread_id: str, requested_at: str) -> None:
        """Record that this target interrupted the current epoch of a thread.

        Thread timestamps cannot attribute an interrupted turn to a new epoch,
        so only this explicit marker lets an unconfirmed launch settle early.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT request_id, lifecycle_json FROM target_requests WHERE thread_id = ? AND state NOT IN ('succeeded', 'failed', 'interrupted')",
                (thread_id,),
            ).fetchall()
            for row in rows:
                lifecycle = json.loads(row[1]) if row[1] else {}
                lifecycle["interrupt_epoch"] = lifecycle.get("epoch", 0)
                lifecycle["interrupt_requested_at"] = requested_at
                connection.execute(
                    "UPDATE target_requests SET lifecycle_json = ?, observation_version = observation_version + 1 WHERE request_id = ?",
                    (json.dumps(lifecycle, sort_keys=True), row[0]),
                )

    def observation_batch(self, *, target_id: str, maximum: int) -> list[TargetReceipt]:
        # Round robin by last visit prevents a permanently failing helper from
        # starving later requests. Also select a terminal whose state commit
        # survived a crash before its event could be frozen, or whose policy
        # reservation still needs release after the event was acknowledged.
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM target_requests WHERE target_id = ? AND (
                    state NOT IN ('succeeded', 'failed', 'interrupted')
                    OR (policy_json IS NOT NULL AND reservation_state = 'active')
                    OR (json_extract(lifecycle_json, '$.resolution') IS NOT NULL
                        AND COALESCE(json_extract(lifecycle_json, '$.resolution.blocking'), 1) != 0)
                    OR (lifecycle_json IS NOT NULL AND (
                    COALESCE(json_extract(lifecycle_json, '$.reported_sequence'), -1)
                       != COALESCE(json_extract(lifecycle_json, '$.sequence'), 0)
                    OR json_extract(lifecycle_json, '$.event.state') IS NOT CASE state
                        WHEN 'succeeded' THEN 'external.completed'
                        WHEN 'failed' THEN 'external.failed'
                        WHEN 'interrupted' THEN 'external.canceled' END))
                ) ORDER BY COALESCE(json_extract(lifecycle_json, '$.checked_at'), ''), created_at, request_id
                LIMIT ?
            """,
                (target_id, maximum),
            ).fetchall()
        return [self._receipt(row) for row in rows]

    def lifecycle_event(
        self,
        request_id: str,
        *,
        expected_version: int,
        state: str,
        observed_at: str,
        turn_ref: str | None = None,
        confirmed_start: bool = False,
        repo_identity: str | None = None,
    ) -> dict[str, Any] | None:
        """Atomically freeze digest inputs on the existing receipt before I/O."""
        states = {
            "preflight": "run.created",
            "dispatching": "external.unknown",
            "running": "run.dispatch.observed",
            "needs_attention": "run.paused",
            "succeeded": "external.completed",
            "failed": "external.failed",
            "interrupted": "external.canceled",
            "unknown": "external.unknown",
        }
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM target_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None or row["observation_version"] != expected_version:
                return None
            receipt = self._receipt(row)
            lifecycle = receipt.lifecycle or {}
            # A sequenced event is immutable until durable delivery or spooling
            # is acknowledged, even if replay enriches the receipt's context.
            if (
                lifecycle.get("event") is not None
                and lifecycle.get("reported_sequence") != lifecycle.get("sequence")
            ):
                return lifecycle["event"]
            if turn_ref and turn_ref != lifecycle.get("previous_turn_ref"):
                lifecycle["turn_ref"] = turn_ref
            event_state = "run.started" if confirmed_start else states[state]
            old = lifecycle.get("event")
            elapsed = (
                (
                    datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
                    - datetime.fromisoformat(old["ts"].replace("Z", "+00:00"))
                ).total_seconds()
                if old
                else HEARTBEAT_SECONDS
            )
            context = dict(receipt.execution_context or {"schema": SCHEMA})
            context.update(
                request_id=request_id,
                target_host=receipt.target_id,
                turn_ref=lifecycle.get("turn_ref", "epoch:0"),
            )
            context.setdefault("initiator", {"kind": "unknown", "label": "unknown"})
            if receipt.thread_id:
                context["thread_id"] = receipt.thread_id
            if receipt.policy:
                context["policy"] = {
                    key: receipt.policy[key]
                    for key in (
                        "session_id",
                        "decision_id",
                        "reservation_id",
                        "delegation_id",
                    )
                    if receipt.policy.get(key) is not None
                }
            context = validate_context(context)
            # run.started and a live refresh represent the same observed state.
            same_state = old and (
                old["state"] == event_state
                or old["state"] == "run.started"
                and event_state == "run.dispatch.observed"
            )
            changed = not same_state or old["context"] != context
            if changed or (
                state not in _TERMINAL_STATES
                and elapsed >= HEARTBEAT_SECONDS
                and lifecycle.get("reported_sequence") == lifecycle.get("sequence")
            ):
                sequence = lifecycle.get("sequence", 0) + 1
                event = {
                    "run_id": f"t3f:{request_id}",
                    "state": event_state,
                    "ts": observed_at,
                    "sequence": sequence,
                    "harness": "t3-fleet",
                    "context": context,
                }
                if receipt.repository:
                    event["repo"] = receipt.repository
                identity = (
                    receipt.policy.get("repo_identity")
                    if receipt.policy
                    else repo_identity
                )
                if identity:
                    event["repo_identity"] = identity
                credential = receipt.admission or receipt.plan
                if credential:
                    event["seat"] = credential["seat"]
                lifecycle.update(sequence=sequence, event=event)
            connection.execute(
                "UPDATE target_requests SET lifecycle_json = ? WHERE request_id = ?",
                (json.dumps(lifecycle, sort_keys=True), request_id),
            )
            if lifecycle.get("reported_sequence") != lifecycle.get("sequence"):
                return lifecycle.get("event")
            return None

    def acknowledge_event(self, request_id: str, sequence: int, *, rejected: bool = False) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT lifecycle_json FROM target_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            lifecycle = json.loads(row[0]) if row and row[0] else {}
            if lifecycle.get("sequence") == sequence:
                lifecycle["reported_sequence"] = sequence
                if rejected:
                    lifecycle["rejected_sequence"] = sequence
                connection.execute(
                    "UPDATE target_requests SET lifecycle_json = ? WHERE request_id = ?",
                    (json.dumps(lifecycle, sort_keys=True), request_id),
                )
