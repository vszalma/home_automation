from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        conn.execute("PRAGMA journal_mode=WAL;").fetchone()
    except sqlite3.DatabaseError:
        pass
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def begin_run(conn: sqlite3.Connection, command: str, args_json: Optional[str]) -> int:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        INSERT INTO runs(command, args_json, status, started_utc)
        VALUES (?, ?, 'running', ?)
        """,
        (command, args_json, now),
    )
    conn.commit()
    return int(cursor.lastrowid)


def end_run_ok(conn: sqlite3.Connection, run_id: int, notes: Optional[str]) -> None:
    conn.execute(
        """
        UPDATE runs
        SET status='ok', notes=?, ended_utc=?
        WHERE run_id=?
        """,
        (notes, utc_now_iso(), run_id),
    )
    conn.commit()


def end_run_failed(conn: sqlite3.Connection, run_id: int, error: str) -> None:
    conn.execute(
        """
        UPDATE runs
        SET status='failed', notes=?, ended_utc=?
        WHERE run_id=?
        """,
        (error, utc_now_iso(), run_id),
    )
    conn.commit()
