"""Private relay ledger. This never opens the fleet, Brigade or T3 databases."""
from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import sqlite3
import stat
from typing import Iterator


class BridgeError(Exception):
    """Only fixed reason codes may cross the operator boundary."""

    REASONS = frozenset({
        "unsafe-file", "invalid-config", "invalid-oauth", "expired-oauth",
        "brigade-prerequisite", "hub-authority", "credential-changed", "config-drift",
        "payload-changed", "invalid-message", "route-denied", "sender-denied",
        "recipient-denied", "t3-identity", "t3-transport", "t3-protocol",
        "t3-pending", "run-failed", "result-ambiguous", "result-oversized",
        "reply-mismatch", "ack-mismatch", "upstream-pending", "ledger-invalid",
        "bounded-limit", "none",
    })

    def __init__(self, reason: str):
        self.reason = reason if reason in self.REASONS else "upstream-pending"
        super().__init__(self.reason)


def absolute_path(path: Path) -> Path:
    # Normalize dot components, never resolve through links.
    return Path(os.path.abspath(path))


@contextlib.contextmanager
def private_parent(path: Path) -> Iterator[int]:
    """Walk directory descriptors without following any symlink component."""
    if os.name != "posix" or not all(hasattr(os, k) for k in ("O_NOFOLLOW", "O_DIRECTORY", "geteuid")):
        raise BridgeError("unsafe-file")
    path = absolute_path(path)
    fd = None
    try:
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        for part in path.parent.parts[1:]:
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = following
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise BridgeError("unsafe-file")
        yield fd
    except OSError:
        raise BridgeError("unsafe-file") from None
    finally:
        if fd is not None:
            os.close(fd)


def checked_fd(path: Path, *, create: bool = False, writable: bool = False) -> int:
    path = absolute_path(path)
    with private_parent(path) as directory:
        flags = (os.O_RDWR if writable else os.O_RDONLY) | os.O_NOFOLLOW | os.O_NONBLOCK
        if create:
            # O_CREAT opens an existing ledger without replacing it.
            flags |= os.O_CREAT
        try:
            fd = os.open(path.name, flags, 0o600, dir_fd=directory)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
                os.close(fd)
                raise BridgeError("unsafe-file")
            return fd
        except OSError:
            raise BridgeError("unsafe-file") from None


def private_read(path: Path, limit: int = 65536) -> bytes:
    fd = checked_fd(path)
    try:
        if os.fstat(fd).st_size > limit:
            raise BridgeError("unsafe-file")
        value = os.read(fd, limit + 1)
        if len(value) > limit:
            raise BridgeError("unsafe-file")
        return value
    finally:
        os.close(fd)


class BridgeStore:
    """Flock liveness plus a committed SQLite claim, never a transaction over I/O.

    A process crash releases the OS lock. Its next owner replaces the diagnostic
    claim in a short transaction. There is no time lease that can expire during
    a slow send and allow another process to send concurrently.
    """

    def __init__(self, path: Path, *, readonly: bool = False):
        self.path = absolute_path(path)
        self.fd = checked_fd(self.path, create=not readonly, writable=not readonly)
        self.db = None
        try:
            # Refuse hostile SQLite sidecars before SQLite can open them.
            for suffix in ("-journal", "-wal", "-shm"):
                sidecar = Path(str(self.path) + suffix)
                if os.path.lexists(sidecar):
                    os.close(checked_fd(sidecar))
            uri = self.path.as_uri() + ("?mode=ro" if readonly else "?mode=rw")
            self.db = sqlite3.connect(uri, uri=True, timeout=0, isolation_level=None)
            self.db.row_factory = sqlite3.Row
            opened = self.path.stat(follow_symlinks=False)
            original = os.fstat(self.fd)
            if (opened.st_ino, opened.st_dev) != (original.st_ino, original.st_dev):
                raise BridgeError("unsafe-file")
            if not readonly:
                self.db.execute("PRAGMA journal_mode=DELETE")
                self.db.execute("PRAGMA synchronous=FULL")
                self.db.executescript("""
                    CREATE TABLE IF NOT EXISTS deliveries (
                        delivery_key TEXT PRIMARY KEY, record TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS drainer (
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1), owner INTEGER
                    );
                """)
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    @contextlib.contextmanager
    def claim(self) -> Iterator[bool]:
        import fcntl
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            with self.transaction():
                self.db.execute("INSERT OR REPLACE INTO drainer VALUES (1,?)", (os.getpid(),))
            yield True
        finally:
            # Release even when a simulated or actual process interrupt occurs.
            fcntl.flock(self.fd, fcntl.LOCK_UN)

    @contextlib.contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def get(self, key: str) -> dict | None:
        row = self.db.execute("SELECT record FROM deliveries WHERE delivery_key=?", (key,)).fetchone()
        return json.loads(row[0]) if row is not None else None

    def save(self, record: dict) -> None:
        with self.transaction():
            self.db.execute("INSERT INTO deliveries VALUES (?,?) ON CONFLICT(delivery_key) DO UPDATE SET record=excluded.record",
                            (record["delivery_key"], json.dumps(record, sort_keys=True, separators=(",", ":"))))

    def pending(self) -> dict | None:
        # The first retained work blocks new intake, including operator-held work.
        for row in self.db.execute("SELECT record FROM deliveries ORDER BY rowid"):
            record = json.loads(row[0])
            if record["state"] not in {"acked", "rejected"}:
                return record
        return None

    def status(self) -> list[dict]:
        rows = self.db.execute("SELECT record FROM deliveries ORDER BY rowid DESC LIMIT 100")
        return [safe_record(json.loads(row[0])) for row in rows]


def safe_record(record: dict) -> dict:
    # IDs are generated locally, never reflected bearer strings or upstream text.
    return {"delivery_id": record["delivery_key"], "state": record["state"],
            "reason": record.get("reason", "none") if record.get("reason", "none") in BridgeError.REASONS else "ledger-invalid"}
