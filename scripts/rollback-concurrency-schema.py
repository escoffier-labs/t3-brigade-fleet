#!/usr/bin/env python3
"""Reverse the per-repository concurrency schema so pre-concurrency code runs.

The concurrency release (PR #26) rebuilt two lease tables in place:

- controller `fleet.db` `leases`: primary key (host, repository) became
  (host, repository, request_id).
- target `target.db` `repository_leases`: (repository PRIMARY KEY, request_id)
  became (repository, request_id, expires_at) keyed by (repository, request_id).

It also added two host keys, `max_concurrent_per_repository` and
`repository_lease_ttl_seconds`, that older config loaders reject as unknown.

Older code opens these tables with `CREATE TABLE IF NOT EXISTS` and writes them
with `ON CONFLICT(host, repository)` / `ON CONFLICT(repository)`, which fails on
the new keys. This script puts the old shapes back. Expired lease rows are
dropped. If a repository holds more than one live lease, the old schema cannot
represent it: the script refuses unless --force, which keeps the lease with the
latest expiry and reports the others.

Every file it changes is first copied beside the original with a
`.before-rollback-<UTC timestamp>` suffix. Stop the fleet first: no agent or
controller process may be running, and no request should be in flight.

Uses only the standard library and never imports t3_fleet, so it runs under
either release.
"""
from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

_CONFIG_KEYS = re.compile(
    r"^\s*(max_concurrent_per_repository|repository_lease_ttl_seconds)\s*="
)


class RollbackRefused(Exception):
    pass


def _timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _backup_db(path: Path, stamp: str) -> Path:
    destination = path.with_name(f"{path.name}.before-rollback-{stamp}")
    source = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        target = sqlite3.connect(destination)
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()
    return destination


def _primary_key(connection: sqlite3.Connection, table: str) -> list[str]:
    rows = [row for row in connection.execute(f"PRAGMA table_info({table})") if row[5]]
    return [row[1] for row in sorted(rows, key=lambda row: row[5])]


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


def _choose(
    rows: list[tuple],
    key_width: int,
    *,
    force: bool,
    label: str,
) -> tuple[list[tuple], list[tuple]]:
    """Keep one row per old key: the latest expiry. Returns (kept, dropped)."""
    groups: dict[tuple, list[tuple]] = {}
    for row in rows:
        groups.setdefault(tuple(row[:key_width]), []).append(row)
    kept: list[tuple] = []
    dropped: list[tuple] = []
    conflicts = []
    for key, members in sorted(groups.items()):
        members.sort(key=lambda row: (row[-1], row[key_width]), reverse=True)
        kept.append(members[0])
        if len(members) > 1:
            conflicts.append((key, [row[key_width] for row in members]))
            dropped.extend(members[1:])
    if conflicts and not force:
        detail = "; ".join(f"{key}: {ids}" for key, ids in conflicts)
        raise RollbackRefused(
            f"{label}: more than one live lease per repository ({detail}). "
            "Drain those requests first, or pass --force to keep only the "
            "lease with the latest expiry."
        )
    return kept, dropped


def rollback_controller_db(path: Path, *, now: float, force: bool, dry_run: bool) -> dict:
    connection = sqlite3.connect(path)
    try:
        if "leases" not in {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }:
            return {"db": str(path), "action": "no leases table"}
        if _primary_key(connection, "leases") == ["host", "repository"]:
            return {"db": str(path), "action": "already pre-concurrency"}
        live = connection.execute(
            "SELECT host, repository, request_id, expires_at FROM leases "
            "WHERE expires_at > ? ORDER BY host, repository, request_id",
            (now,),
        ).fetchall()
        expired = connection.execute(
            "SELECT COUNT(*) FROM leases WHERE expires_at <= ?", (now,)
        ).fetchone()[0]
        kept, dropped = _choose(live, 2, force=force, label=f"controller {path}")
        if not dry_run:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                CREATE TABLE leases_single_slot (
                    host TEXT NOT NULL,
                    repository TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (host, repository)
                )
                """
            )
            connection.executemany(
                "INSERT INTO leases_single_slot (host, repository, request_id, expires_at) "
                "VALUES (?, ?, ?, ?)",
                kept,
            )
            connection.execute("DROP TABLE leases")
            connection.execute("ALTER TABLE leases_single_slot RENAME TO leases")
            connection.commit()
        return {
            "db": str(path),
            "action": "would rebuild leases" if dry_run else "rebuilt leases",
            "kept": [row[2] for row in kept],
            "dropped_conflicting": [row[2] for row in dropped],
            "dropped_expired": expired,
        }
    finally:
        connection.close()


def rollback_target_db(path: Path, *, now: float, force: bool, dry_run: bool) -> dict:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        if "repository_leases" not in {
            row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }:
            return {"db": str(path), "action": "no repository_leases table"}
        if "expires_at" not in _columns(connection, "repository_leases"):
            return {"db": str(path), "action": "already pre-concurrency"}
        live = connection.execute(
            "SELECT repository, request_id, expires_at FROM repository_leases "
            "WHERE expires_at > ? ORDER BY repository, request_id",
            (now,),
        ).fetchall()
        expired = connection.execute(
            "SELECT COUNT(*) FROM repository_leases WHERE expires_at <= ?", (now,)
        ).fetchone()[0]
        kept, dropped = _choose(live, 1, force=force, label=f"target {path}")
        if not dry_run:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                CREATE TABLE repository_leases_single_slot (
                    repository TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL REFERENCES target_requests(request_id)
                )
                """
            )
            connection.executemany(
                "INSERT INTO repository_leases_single_slot (repository, request_id) VALUES (?, ?)",
                [(row[0], row[1]) for row in kept],
            )
            connection.execute("DROP TABLE repository_leases")
            connection.execute(
                "ALTER TABLE repository_leases_single_slot RENAME TO repository_leases"
            )
            connection.commit()
            problems = connection.execute("PRAGMA foreign_key_check").fetchall()
            if problems:
                raise RollbackRefused(f"target {path}: foreign key check failed: {problems}")
        return {
            "db": str(path),
            "action": "would rebuild repository_leases" if dry_run else "rebuilt repository_leases",
            "kept": [row[1] for row in kept],
            "dropped_conflicting": [row[1] for row in dropped],
            "dropped_expired": expired,
        }
    finally:
        connection.close()


def strip_config(path: Path, *, stamp: str, dry_run: bool) -> dict:
    lines = path.read_bytes().decode("utf-8").splitlines(keepends=True)
    kept = [line for line in lines if not _CONFIG_KEYS.match(line)]
    removed = len(lines) - len(kept)
    if removed and not dry_run:
        shutil.copy2(path, path.with_name(f"{path.name}.before-rollback-{stamp}"))
        path.write_bytes("".join(kept).encode("utf-8"))
    return {
        "config": str(path),
        "action": ("would remove" if dry_run else "removed") if removed else "no concurrency keys",
        "lines": removed,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--controller-db", action="append", default=[], type=Path)
    parser.add_argument("--target-db", action="append", default=[], type=Path)
    parser.add_argument("--config", action="append", default=[], type=Path)
    parser.add_argument("--force", action="store_true",
                        help="keep only the latest lease when a repository holds several")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if not (args.controller_db or args.target_db or args.config):
        parser.error("name at least one --controller-db, --target-db, or --config")
    for path in [*args.controller_db, *args.target_db, *args.config]:
        if not path.is_file():
            parser.error(f"not a file: {path}")

    stamp = _timestamp()
    now = time.time()
    reports: list[dict] = []
    try:
        # Check everything before changing anything, so a refusal leaves every
        # file untouched.
        for path in args.controller_db:
            rollback_controller_db(path, now=now, force=args.force, dry_run=True)
        for path in args.target_db:
            rollback_target_db(path, now=now, force=args.force, dry_run=True)
        for path in args.controller_db:
            if not args.dry_run:
                reports.append({"backup": str(_backup_db(path, stamp))})
            reports.append(rollback_controller_db(path, now=now, force=args.force, dry_run=args.dry_run))
        for path in args.target_db:
            if not args.dry_run:
                reports.append({"backup": str(_backup_db(path, stamp))})
            reports.append(rollback_target_db(path, now=now, force=args.force, dry_run=args.dry_run))
        for path in args.config:
            reports.append(strip_config(path, stamp=stamp, dry_run=args.dry_run))
    except RollbackRefused as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2
    for report in reports:
        print(" ".join(f"{key}={value}" for key, value in report.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
