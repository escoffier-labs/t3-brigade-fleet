"""Private lane bindings and at-most-once mutation reservations."""
from __future__ import annotations

import contextlib
import json
import os
import sqlite3
import stat
import uuid
from pathlib import Path

from .errors import IdempotencyConflict, ValidationError


def private_directory(path: Path, *, create: bool = False) -> None:
    """Refuse links and shared directories instead of changing their permissions."""
    path = path.absolute()
    for part in reversed((path, *path.parents)):
        if part.is_symlink():
            raise ValidationError('private path must not contain symlinks')
    if create and not path.exists():
        if not path.parent.exists():
            private_directory(path.parent, create=True)
        path.mkdir(mode=0o700, exist_ok=True)
    info = path.stat()
    if (not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077
            or hasattr(os, 'getuid') and info.st_uid != os.getuid()):
        raise ValidationError('directory must be owner-only')


class LaneStore:
    def __init__(self, path: Path):
        self.path = Path(path).expanduser().absolute()
        private_directory(self.path.parent, create=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        except FileExistsError:
            pass
        else:
            os.close(fd)
        info = self.path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) & 0o077
                or hasattr(os, 'getuid') and info.st_uid != os.getuid()):
            raise ValidationError('lane database must be an owner-only regular file')
        with self._db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS bindings (
                    host TEXT NOT NULL, thread TEXT NOT NULL, environment TEXT NOT NULL,
                    project TEXT NOT NULL, workspace TEXT NOT NULL,
                    PRIMARY KEY(host, thread));
                CREATE TABLE IF NOT EXISTS operations (
                    environment TEXT NOT NULL, thread TEXT NOT NULL, event TEXT NOT NULL,
                    digest TEXT NOT NULL, receipt TEXT NOT NULL,
                    PRIMARY KEY(environment, thread, event));
            ''')

    @contextlib.contextmanager
    def _db(self):
        with contextlib.closing(sqlite3.connect(self.path, timeout=10)) as db:
            db.row_factory = sqlite3.Row
            with db:
                yield db

    def bind(self, host: str, binding: dict) -> dict:
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT * FROM bindings WHERE host=? AND thread=?',
                             (host, binding['thread_id'])).fetchone()
            values = (binding['environment_id'], binding['project_id'], binding['workspace'])
            if old and (old['environment'], old['project'], old['workspace']) != values:
                raise IdempotencyConflict('lane is already bound to a different identity')
            if not old:
                if db.execute('SELECT count(*) FROM bindings').fetchone()[0] >= 128:
                    raise ValidationError('lane binding limit reached')
                db.execute('INSERT INTO bindings VALUES (?,?,?,?,?)', (host, binding['thread_id'], *values))
        return {'status': 'bound', **binding}

    def bindings(self, host: str) -> list[dict]:
        with self._db() as db:
            return [{'thread_id': row['thread'], 'environment_id': row['environment'],
                     'project_id': row['project'], 'workspace': row['workspace']}
                    for row in db.execute('SELECT * FROM bindings WHERE host=? ORDER BY thread LIMIT 128', (host,))]

    def get(self, host: str, thread: str) -> dict:
        for binding in self.bindings(host):
            if binding['thread_id'] == thread:
                return binding
        raise ValidationError('lane must be explicitly bound first')

    def claim(self, environment: str, thread: str, event: str, digest: str, *, messages: int = 1) -> tuple[bool, dict]:
        """An unknown receipt is durable before the first network write.

        It deliberately remains unknown after a process crash. A second process
        cannot take it over or resend, even when no acknowledgement was saved.
        """
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT digest,receipt FROM operations WHERE environment=? AND thread=? AND event=?',
                             (environment, thread, event)).fetchone()
            if row:
                if row['digest'] != digest:
                    raise IdempotencyConflict('operation ID has a different payload')
                return False, json.loads(row['receipt'])
            ids = [{'status': 'unknown' if messages == 1 else 'not_sent',
                    'command_id': str(uuid.uuid4()), 'message_id': str(uuid.uuid4())}
                   for _ in range(messages)]
            receipt = {'status': 'unknown', 'environment_id': environment, 'thread_id': thread}
            if messages == 1:
                receipt.update(ids[0])
            else:
                receipt.update(messages=ids, packet_prepared=False)
            db.execute('INSERT INTO operations VALUES (?,?,?,?,?)',
                       (environment, thread, event, digest, json.dumps(receipt)))
            return True, receipt

    def finish(self, environment: str, thread: str, event: str, receipt: dict) -> dict:
        with self._db() as db:
            db.execute('UPDATE operations SET receipt=? WHERE environment=? AND thread=? AND event=?',
                       (json.dumps(receipt), environment, thread, event))
        return receipt

    def unknown(self, environment: str, thread: str) -> list[dict]:
        with self._db() as db:
            # Filter in SQL before limiting so old accepted deliveries cannot
            # hide a more recent uncertain operation.
            rows = db.execute("""SELECT event,receipt FROM operations
                WHERE environment=? AND thread=?
                AND json_extract(receipt,'$.status') IN ('unknown','partial')
                ORDER BY rowid DESC LIMIT 32""", (environment, thread))
            return [{'operation_id': row['event'], **json.loads(row['receipt'])} for row in rows]
