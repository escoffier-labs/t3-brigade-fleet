"""The bounded durable local outbox for ordinary work that could not launch yet.

The client has no resident scheduler. A `submit` that meets a pre-launch
capacity or offline block used to fail and lose the request: `wait` polls one
request, and the durable request record keeps only a digest and a prompt size,
so nothing in the fleet could replay the work later.

This module is the missing durable half. It stores the caller's own original
intent and prompt, exactly as submitted, so a later `t3-fleet tick` can retry
that intent from scratch rather than replaying a machine selection that already
failed. It is deliberately narrow:

* Only an explicit user submission enters it. There is no backlog, issue, or
  deck intake, and nothing here creates work in order to spend quota.
* An adopted delegation never enters it. A delegation is one execution that
  already exists centrally; re-queueing it locally would make it two.
* The prompt is local-only. It lives in one owner-only file, it is never
  carried into a Hub call, a status payload, a receipt, an error, or a tick
  result, and it is logically removed as soon as the work is no longer queued.

Retention bounds how long a payload remains in live records. Logical removal
never proves physical erasure from SQLite/WAL or storage copies, or that an
execution stopped running.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from .errors import ValidationError

# The whole vocabulary a queued or drained job may report. Like the policy
# reasons, this is an allowlist rather than free text, so a helper's message, a
# path, or a prompt can never ride out on a disposition field.
DISPOSITIONS = frozenset(
    {
        # Still queued, waiting for the block that stopped it to clear.
        "queued-blocked",
        # Still queued, backing off after a drain attempt that stayed blocked.
        "queued-retry",
        # Handed to the controller and accepted by a target.
        "dispatched",
        # A drain attempt lost the transport after the target may already have
        # accepted the work. Never retried and never freed speculatively.
        "held-acceptance-unknown",
        # Live policy, privacy, consumer, or seat refused it on the way out.
        "failed-policy",
        # The stored intent no longer validates against live configuration.
        "failed-invalid",
        # The same identity already exists under a different payload.
        "failed-conflict",
        # The bounded attempt budget ran out while still blocked.
        "failed-attempts",
        # The retention window closed before the work ever left the queue.
        "expired-retention",
    }
)

# What is known about the execution behind a record, kept separate from the
# record's own state so a purge or an expiry can never be read as a claim that
# a run stopped.
EXECUTION_STATES = frozenset({"never-dispatched", "dispatched", "unknown"})

_STATES = frozenset({"queued", "dispatched", "held", "failed", "expired"})

_JOB_ID = "job-"


class StaleLease(ValidationError):
    """A fenced write lost its claim to another tick, a restart, or an expiry."""


@dataclass(frozen=True)
class OutboxLimits:
    """The bounds an outbox is allowed to grow to, all operator-configured."""

    max_records: int = 64
    max_prompt_bytes: int = 120_000
    max_intent_bytes: int = 16_384
    retention_seconds: float = 86_400.0
    max_attempts: int = 5
    backoff_seconds: float = 60.0
    max_backoff_seconds: float = 3_600.0
    max_jobs_per_tick: int = 4
    max_tick_seconds: float = 60.0
    lease_seconds: float = 120.0


@dataclass(frozen=True)
class OutboxJob:
    """One queued unit of ordinary work, with its three identities kept apart.

    `job_id` is this machine's own work identity and it never moves. It is
    minted here, before any machine is chosen, so it cannot be confused with
    `request_id`, which is the target-qualified execution identity and exists
    only once a dispatch attempt has reached the point of naming one.
    `parent_request_id` is a third fact entirely and belongs to delegated work,
    which never enters this queue; the column exists so the three can never be
    collapsed into one by a later reader.
    """

    job_id: str
    digest: str
    state: str
    disposition: str
    execution_state: str
    attempts: int
    request_id: str | None
    parent_request_id: str | None
    host: str | None
    repository: str | None
    payload_purged: bool
    lease_generation: int
    created_at: float
    updated_at: float
    expires_at: float
    next_attempt_at: float

    def redacted(self) -> dict[str, Any]:
        """The machine-readable view that may leave this process.

        Identities, states, and counts only. No prompt, no title, no checkout
        path, no helper message, and no error text.
        """
        return {
            "job_id": self.job_id,
            # Named apart from the dispatch state on purpose. A job that is
            # `queued` here is not a request queued on a target, and collapsing
            # the two would be the easiest way to misreport a run as started.
            "job_state": self.state,
            # `disposition` on a result already means something else entirely
            # (a retained delegated worktree), so the queue gets its own key
            # rather than widening a frozen one.
            "queue_disposition": self.disposition,
            "execution_state": self.execution_state,
            "attempts": self.attempts,
            "request_id": self.request_id,
            "parent_request_id": self.parent_request_id,
            "host": self.host,
            "repository": self.repository,
            "payload_purged": self.payload_purged,
        }


@dataclass(frozen=True)
class OutboxPayload:
    """The immutable original caller intent and prompt for one queued job."""

    intent: dict[str, Any]
    prompt: str


def _secure_directory(path: Path) -> None:
    """Create the outbox directory owner-only, refusing an unsafe path.

    A symlink here would let anything that can write the parent directory
    redirect the prompt store somewhere world-readable, so an existing symlink
    is refused rather than followed. On Windows the mode bits do not apply and
    the directory inherits its parent ACL; that is a documented limitation
    rather than a silent one.
    """
    if path.is_symlink():
        raise ValidationError("fleet outbox path must not be a symlink")
    if os.name == "nt":
        path.mkdir(parents=True, exist_ok=True)
        return
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def _secure_file(path: Path) -> None:
    if path.is_symlink():
        raise ValidationError("fleet outbox store must not be a symlink")
    if os.name == "nt":
        return
    if path.exists():
        os.chmod(path, 0o600)


class OutboxStore:
    """Durable storage for queued ordinary work, fenced for concurrent ticks.

    Everything is one SQLite database inside an owner-only directory. The index
    and the payload live in the same transactional store on purpose: a claim, a
    disposition, and a payload purge are one atomic fact, so a crash between
    them cannot leave a job that looks drained while its prompt remains in
    the live record, or a job that looks queued with nothing left to replay.
    `payload_purged` means logical removal from live records. Secure deletion
    is enabled, but SQLite/WAL, snapshots, storage journals and backups may
    retain prior bytes. Physical erasure is not guaranteed.
    """

    def __init__(
        self,
        directory: str | Path,
        *,
        limits: OutboxLimits | None = None,
        clock: Callable[[], float] = time.time,
        uuid_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        self.directory = Path(directory)
        self.limits = limits or OutboxLimits()
        self._clock = clock
        self._uuid = uuid_factory
        _secure_directory(self.directory)
        self.path = self.directory / "outbox.db"
        _secure_file(self.path)
        self._initialize()
        _secure_file(self.path)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA secure_delete = ON")
        if connection.execute("PRAGMA secure_delete").fetchone()[0] != 1:
            connection.close()
            raise ValidationError("outbox secure deletion setting is unavailable")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS outbox_jobs (
                    job_id TEXT PRIMARY KEY,
                    digest TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    disposition TEXT NOT NULL,
                    execution_state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    request_id TEXT,
                    parent_request_id TEXT,
                    host TEXT,
                    repository TEXT,
                    intent_json TEXT,
                    prompt TEXT,
                    payload_purged INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_expires_at REAL,
                    lease_generation INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    next_attempt_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS outbox_jobs_ready
                    ON outbox_jobs(state, next_attempt_at);
                CREATE TABLE IF NOT EXISTS outbox_tick (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    lease_owner TEXT,
                    lease_expires_at REAL,
                    last_tick_at REAL,
                    last_failure_at REAL,
                    last_failure_reason TEXT
                );
                INSERT OR IGNORE INTO outbox_tick (id) VALUES (1);
                """
            )

    # -- reading -------------------------------------------------------

    @staticmethod
    def _job(row: sqlite3.Row) -> OutboxJob:
        return OutboxJob(
            job_id=row["job_id"],
            digest=row["digest"],
            state=row["state"],
            disposition=row["disposition"],
            execution_state=row["execution_state"],
            attempts=row["attempts"],
            request_id=row["request_id"],
            parent_request_id=row["parent_request_id"],
            host=row["host"],
            repository=row["repository"],
            payload_purged=bool(row["payload_purged"]),
            lease_generation=row["lease_generation"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            expires_at=row["expires_at"],
            next_attempt_at=row["next_attempt_at"],
        )

    def get(self, job_id: str) -> OutboxJob | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM outbox_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return None if row is None else self._job(row)

    def payload(self, job_id: str) -> OutboxPayload | None:
        """The live intent and prompt, or nothing after logical payload removal."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT intent_json, prompt FROM outbox_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None or row["intent_json"] is None or row["prompt"] is None:
            return None
        return OutboxPayload(intent=json.loads(row["intent_json"]), prompt=row["prompt"])

    def jobs(self, *, state: str | None = None) -> list[OutboxJob]:
        query = "SELECT * FROM outbox_jobs"
        parameters: tuple[Any, ...] = ()
        if state is not None:
            query += " WHERE state = ?"
            parameters = (state,)
        query += " ORDER BY created_at ASC, job_id ASC"
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [self._job(row) for row in rows]

    def find_by_digest(self, digest: str) -> OutboxJob | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM outbox_jobs WHERE digest = ?", (digest,)
            ).fetchone()
        return None if row is None else self._job(row)

    # -- writing -------------------------------------------------------

    def enqueue(
        self,
        *,
        digest: str,
        intent: dict[str, Any],
        prompt: str,
        host: str | None,
        repository: str | None,
        request_id: str | None = None,
        disposition: str = "queued-blocked",
    ) -> OutboxJob:
        """Store one explicitly submitted job, or return the one already stored.

        The digest is the caller's own payload identity, so submitting the same
        work twice while it is still queued is one job rather than two. Nothing
        about the machine that just refused it is part of that identity: the
        stored host is a record of where the attempt went, never an input to
        the retry.
        """
        if disposition not in DISPOSITIONS:
            raise ValidationError("unsupported outbox disposition")
        if intent.get("delegation_id") is not None:
            # One adoption is one execution. A delegated request already has a
            # central decision and reservation, so it is never re-queued here as
            # fresh routable ordinary work.
            raise ValidationError("an adopted delegation cannot be queued locally")
        encoded_prompt = prompt.encode("utf-8")
        if not prompt or len(encoded_prompt) > self.limits.max_prompt_bytes:
            raise ValidationError("outbox prompt exceeds the configured size limit")
        encoded_intent = json.dumps(intent, sort_keys=True, separators=(",", ":"))
        if len(encoded_intent.encode("utf-8")) > self.limits.max_intent_bytes:
            raise ValidationError("outbox request exceeds the configured size limit")
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM outbox_jobs WHERE digest = ?", (digest,)
            ).fetchone()
            if existing is not None:
                return self._job(existing)
            self._purge_expired(connection, now)
            live = connection.execute(
                "SELECT COUNT(*) AS total FROM outbox_jobs WHERE state = 'queued'"
            ).fetchone()["total"]
            if live >= self.limits.max_records:
                raise ValidationError("fleet outbox is full")
            job_id = f"{_JOB_ID}{self._uuid()}"
            connection.execute(
                """
                INSERT INTO outbox_jobs
                    (job_id, digest, state, disposition, execution_state, attempts,
                     request_id, parent_request_id, host, repository, intent_json,
                     prompt, payload_purged, lease_generation, created_at, updated_at,
                     expires_at, next_attempt_at)
                VALUES (?, ?, 'queued', ?, 'never-dispatched', 0, ?, NULL, ?, ?, ?, ?,
                        0, 0, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    digest,
                    disposition,
                    request_id,
                    host,
                    repository,
                    encoded_intent,
                    prompt,
                    now,
                    now,
                    now + self.limits.retention_seconds,
                    now,
                ),
            )
        job = self.get(job_id)
        assert job is not None
        return job

    @staticmethod
    def _purge_expired(connection: sqlite3.Connection, now: float) -> None:
        """Drop the payload of everything past its retention window.

        A queued job that never left the queue becomes `expired`, and it keeps
        `never-dispatched` because that is what is actually known about it: no
        drain attempt ever reached a target with it. A held job keeps its own
        state and its `unknown` execution state; only its payload goes. The
        window bounds prompt retention in live records. It guarantees neither
        physical erasure from storage copies nor that a run has stopped.
        """
        connection.execute(
            """
            UPDATE outbox_jobs
               SET state = 'expired',
                   disposition = 'expired-retention',
                   intent_json = NULL,
                   prompt = NULL,
                   payload_purged = 1,
                   lease_owner = NULL,
                   lease_expires_at = NULL,
                   updated_at = ?
             WHERE state = 'queued' AND expires_at <= ?
            """,
            (now, now),
        )
        connection.execute(
            """
            UPDATE outbox_jobs
               SET intent_json = NULL,
                   prompt = NULL,
                   payload_purged = 1,
                   updated_at = ?
             WHERE state != 'queued' AND expires_at <= ? AND payload_purged = 0
            """,
            (now, now),
        )

    def purge_expired(self) -> int:
        """Apply retention. Returns how many queued jobs aged out."""
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            before = connection.execute(
                "SELECT COUNT(*) AS total FROM outbox_jobs WHERE state = 'expired'"
            ).fetchone()["total"]
            self._purge_expired(connection, now)
            after = connection.execute(
                "SELECT COUNT(*) AS total FROM outbox_jobs WHERE state = 'expired'"
            ).fetchone()["total"]
        return after - before

    def claim(self, job_id: str, owner: str) -> tuple[OutboxJob, int] | None:
        """Take a fenced, expiring claim on one queued job.

        The job stays `queued` while it is claimed. A tick that crashes mid
        drain therefore leaves work that is still replayable rather than work
        stranded in an in-flight state, and the lease simply expires. Every
        later write for this job must present the generation returned here, so
        a slow tick that wakes up after its lease expired cannot overwrite the
        disposition a newer tick already recorded.

        The attempt is counted here rather than at the point a dispatch names an
        execution identity. A claim is the one event that happens on every
        attempt: a block raised before any identity exists is still an attempt,
        and so is a tick that crashed before it could record anything. Counting
        anywhere later would let a job that fails early retry without bound.
        """
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM outbox_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if row is None or row["state"] != "queued":
                return None
            held = row["lease_owner"] is not None and (row["lease_expires_at"] or 0) > now
            if held:
                return None
            generation = row["lease_generation"] + 1
            connection.execute(
                """
                UPDATE outbox_jobs
                   SET lease_owner = ?,
                       lease_expires_at = ?,
                       lease_generation = ?,
                       attempts = attempts + 1,
                       updated_at = ?
                 WHERE job_id = ? AND lease_generation = ?
                """,
                (
                    owner,
                    now + self.limits.lease_seconds,
                    generation,
                    now,
                    job_id,
                    row["lease_generation"],
                ),
            )
        job = self.get(job_id)
        if job is None:
            return None
        return job, generation

    def ready(self, *, limit: int) -> list[OutboxJob]:
        """Queued jobs whose backoff has elapsed, oldest first."""
        now = self._clock()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM outbox_jobs
                 WHERE state = 'queued'
                   AND next_attempt_at <= ?
                   AND expires_at > ?
                   AND (lease_owner IS NULL OR lease_expires_at <= ?)
                 ORDER BY created_at ASC, job_id ASC
                 LIMIT ?
                """,
                (now, now, now, max(0, limit)),
            ).fetchall()
        return [self._job(row) for row in rows]

    def _fenced(
        self,
        job_id: str,
        generation: int,
        owner: str,
        assignments: str,
        parameters: Iterable[Any],
    ) -> OutboxJob:
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                f"""
                UPDATE outbox_jobs
                   SET {assignments}, updated_at = ?
                 WHERE job_id = ?
                   AND lease_generation = ?
                   AND lease_owner = ?
                """,
                (*parameters, now, job_id, generation, owner),
            )
            if cursor.rowcount != 1:
                raise StaleLease("fleet outbox lease is no longer this tick's")
        job = self.get(job_id)
        if job is None:
            raise StaleLease("fleet outbox job disappeared under its lease")
        return job

    def record_execution_identity(
        self,
        job_id: str,
        generation: int,
        owner: str,
        *,
        request_id: str,
        host: str | None = None,
    ) -> OutboxJob:
        """Record the execution identity a drain attempt is about to use.

        Written before the attempt can mutate anything on a target, so a crash
        during dispatch still leaves the identity behind. The next tick replays
        that exact identity instead of minting a second one, which is what makes
        a duplicate execution impossible across a restart. The attempt itself
        was already counted at claim time; this records only the identity.
        """
        return self._fenced(
            job_id,
            generation,
            owner,
            "request_id = ?, host = COALESCE(?, host)",
            (request_id, host),
        )

    def mark_dispatched(
        self,
        job_id: str,
        generation: int,
        owner: str,
        *,
        request_id: str,
        host: str | None,
    ) -> OutboxJob:
        """The work launched. The payload is no longer needed, so it goes."""
        return self._fenced(
            job_id,
            generation,
            owner,
            (
                "state = 'dispatched', disposition = 'dispatched', "
                "execution_state = 'dispatched', request_id = ?, "
                "host = COALESCE(?, host), intent_json = NULL, prompt = NULL, "
                "payload_purged = 1, lease_owner = NULL, lease_expires_at = NULL"
            ),
            (request_id, host),
        )

    def mark_held(
        self,
        job_id: str,
        generation: int,
        owner: str,
        *,
        request_id: str | None,
        host: str | None,
    ) -> OutboxJob:
        """Acceptance is unknown, so the job is parked rather than retried.

        The payload is deliberately kept: it is the only local record of what
        this identity was asked to do, and an operator reconciling the durable
        target request may still need it. Nothing here retries and nothing here
        frees capacity, because neither is provable from a lost transport.
        """
        return self._fenced(
            job_id,
            generation,
            owner,
            (
                "state = 'held', disposition = 'held-acceptance-unknown', "
                "execution_state = 'unknown', request_id = COALESCE(?, request_id), "
                "host = COALESCE(?, host), lease_owner = NULL, lease_expires_at = NULL"
            ),
            (request_id, host),
        )

    def mark_failed(
        self,
        job_id: str,
        generation: int,
        owner: str,
        *,
        disposition: str,
    ) -> OutboxJob:
        """A terminal refusal. The payload goes; the truthful reason stays."""
        if disposition not in DISPOSITIONS:
            raise ValidationError("unsupported outbox disposition")
        return self._fenced(
            job_id,
            generation,
            owner,
            (
                "state = 'failed', disposition = ?, intent_json = NULL, "
                "prompt = NULL, payload_purged = 1, lease_owner = NULL, "
                "lease_expires_at = NULL"
            ),
            (disposition,),
        )

    def reschedule(
        self,
        job_id: str,
        generation: int,
        owner: str,
        *,
        host: str | None = None,
    ) -> OutboxJob:
        """Still blocked. Back off and stay queued, within the attempt budget.

        The backoff doubles per attempt up to the configured ceiling. When the
        budget runs out the job fails with its own disposition rather than
        being retried forever or silently dropped.
        """
        job = self.get(job_id)
        if job is None:
            raise StaleLease("fleet outbox job disappeared under its lease")
        if job.attempts >= self.limits.max_attempts:
            return self.mark_failed(
                job_id, generation, owner, disposition="failed-attempts"
            )
        delay = min(
            self.limits.backoff_seconds * (2 ** max(0, job.attempts - 1)),
            self.limits.max_backoff_seconds,
        )
        return self._fenced(
            job_id,
            generation,
            owner,
            (
                "disposition = 'queued-retry', host = COALESCE(?, host), "
                "next_attempt_at = ?, lease_owner = NULL, lease_expires_at = NULL"
            ),
            (host, self._clock() + delay),
        )

    def release(self, job_id: str, generation: int, owner: str) -> None:
        """Drop a claim without changing the job, for a bounded tick that stopped."""
        try:
            self._fenced(
                job_id,
                generation,
                owner,
                "lease_owner = NULL, lease_expires_at = NULL",
                (),
            )
        except StaleLease:
            return

    # -- tick ownership ------------------------------------------------

    def acquire_tick(self, owner: str) -> bool:
        """Take the single-owner tick lease, or report that another tick has it."""
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT lease_owner, lease_expires_at FROM outbox_tick WHERE id = 1"
            ).fetchone()
            if (
                row is not None
                and row["lease_owner"] is not None
                and row["lease_owner"] != owner
                and (row["lease_expires_at"] or 0) > now
            ):
                return False
            connection.execute(
                "UPDATE outbox_tick SET lease_owner = ?, lease_expires_at = ? WHERE id = 1",
                (owner, now + self.limits.lease_seconds),
            )
        return True

    def finish_tick(
        self,
        owner: str,
        *,
        failure_reason: str | None = None,
    ) -> None:
        now = self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if failure_reason is None:
                connection.execute(
                    """
                    UPDATE outbox_tick
                       SET lease_owner = NULL, lease_expires_at = NULL, last_tick_at = ?
                     WHERE id = 1 AND lease_owner = ?
                    """,
                    (now, owner),
                )
            else:
                connection.execute(
                    """
                    UPDATE outbox_tick
                       SET lease_owner = NULL, lease_expires_at = NULL,
                           last_tick_at = ?, last_failure_at = ?,
                           last_failure_reason = ?
                     WHERE id = 1 AND lease_owner = ?
                    """,
                    (now, now, failure_reason, owner),
                )

    def tick_status(self) -> dict[str, Any]:
        """What the UI needs to tell a pending queue from an actually drained one."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT last_tick_at, last_failure_at, last_failure_reason
                  FROM outbox_tick WHERE id = 1
                """
            ).fetchone()
            counts = {
                state: connection.execute(
                    "SELECT COUNT(*) AS total FROM outbox_jobs WHERE state = ?",
                    (state,),
                ).fetchone()["total"]
                for state in sorted(_STATES)
            }
        return {
            "last_tick_at": None if row is None else row["last_tick_at"],
            "last_failure_at": None if row is None else row["last_failure_at"],
            "last_failure_reason": None if row is None else row["last_failure_reason"],
            "counts": counts,
        }
