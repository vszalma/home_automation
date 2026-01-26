#!/usr/bin/env python3
"""
Lightweight SQLite migration runner for media_pipeline.

Applies ordered .sql migrations and records applied versions in schema_migrations.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Set

MIGRATION_PATTERN = re.compile(r"^(\d{3})_(.+)\.sql$", re.IGNORECASE)


@dataclass(frozen=True)
class Migration:
    version: int
    filename: str
    path: Path


def resolve_path(value: str) -> Path:
    """Resolve paths safely across platforms."""
    return Path(value).expanduser().resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply SQLite migrations for the media pipeline."
    )
    parser.add_argument("--db", required=True, help="Path to the sqlite db file.")
    parser.add_argument(
        "--migrations",
        required=True,
        help="Directory containing migration .sql files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show pending migrations without applying changes.",
    )
    parser.add_argument(
        "--target",
        type=int,
        default=None,
        help="Apply migrations up to this version (inclusive).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_migrations_table(conn: sqlite3.Connection, verbose: bool) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations(
            version INTEGER PRIMARY KEY,
            filename TEXT NOT NULL,
            applied_utc TEXT NOT NULL
        )
        """
    )
    if verbose:
        print("Ensured schema_migrations table exists.")


def discover_migrations(directory: Path, verbose: bool) -> List[Migration]:
    migrations: List[Migration] = []
    version_seen: Set[int] = set()

    for path in sorted(directory.glob("*.sql")):
        match = MIGRATION_PATTERN.match(path.name)
        if not match:
            if verbose:
                print(f"Ignoring file not matching pattern: {path.name}")
            continue
        version = int(match.group(1))
        if version in version_seen:
            raise SystemExit(f"Duplicate migration version detected: {version:03d}")
        version_seen.add(version)
        migrations.append(Migration(version=version, filename=path.name, path=path))

    migrations.sort(key=lambda m: m.version)
    return migrations


def get_applied_versions(conn: sqlite3.Connection) -> Set[int]:
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {row[0] for row in rows}


def apply_pragmas(conn: sqlite3.Connection, verbose: bool) -> None:
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        mode = conn.execute("PRAGMA journal_mode=WAL;").fetchone()
        if verbose:
            print(f"journal_mode set to: {mode[0] if mode else 'unknown'}")
    except sqlite3.DatabaseError as exc:
        if verbose:
            print(f"Warning: failed to set journal_mode=WAL ({exc})")
    conn.execute("PRAGMA synchronous=NORMAL;")


def apply_migration(
    conn: sqlite3.Connection,
    migration: Migration,
    verbose: bool,
) -> None:
    sql_text = migration.path.read_text(encoding="utf-8")
    if verbose:
        print(f"Applying migration {migration.version:03d}: {migration.filename}")
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN;")
        cursor.executescript(sql_text)
        cursor.execute(
            "INSERT INTO schema_migrations(version, filename, applied_utc) VALUES (?, ?, ?);",
            (migration.version, migration.filename, utc_now_iso()),
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        raise RuntimeError(
            f"Failed to apply migration {migration.version:03d} ({migration.filename}): {exc}"
        ) from exc


def summarize_applied(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    return row[0] if row and row[0] is not None else None


def main() -> None:
    args = parse_args()
    db_path = resolve_path(args.db)
    migrations_dir = resolve_path(args.migrations)

    if not migrations_dir.is_dir():
        raise SystemExit(f"Migrations directory not found: {migrations_dir}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        apply_pragmas(conn, args.verbose)
        if not args.dry_run:
            ensure_migrations_table(conn, args.verbose)
        else:
            # In dry-run, avoid writing; check existence best-effort.
            try:
                conn.execute("SELECT 1 FROM schema_migrations LIMIT 1;").fetchone()
            except sqlite3.DatabaseError:
                if args.verbose:
                    print("schema_migrations table not found; assuming no migrations applied (dry-run).")

        migrations = discover_migrations(migrations_dir, args.verbose)
        applied_versions = get_applied_versions(conn) if not args.dry_run else set()

        if args.verbose:
            print(f"Discovered {len(migrations)} migration(s); applied: {sorted(applied_versions)}")

        target_version = args.target if args.target is not None else (migrations[-1].version if migrations else None)

        pending: List[Migration] = [
            m for m in migrations
            if m.version not in applied_versions and (target_version is None or m.version <= target_version)
        ]

        applied_count = 0
        skipped_count = len(migrations) - len(pending)
        if args.dry_run:
            print("Dry-run mode: pending migrations to apply:")
            for m in pending:
                print(f"  {m.version:03d} - {m.filename}")
            print(f"Target version: {target_version if target_version is not None else 'latest'}")
            print(f"Applied: {len(applied_versions)}, Pending: {len(pending)}, Skipped: {skipped_count}")
            sys.exit(0)

        for migration in pending:
            apply_migration(conn, migration, args.verbose)
            applied_count += 1

        current_version = summarize_applied(conn)
        print(
            json.dumps(
                {
                    "applied_count": applied_count,
                    "skipped_count": skipped_count,
                    "current_db_version": current_version,
                }
            )
        )
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"Migration failed: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
