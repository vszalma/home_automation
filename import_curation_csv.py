# import_curation_csv.py
# Step 4c: Import reviewed CSV from Step 4b and persist semantic curation at SHA level.
#
# Behavior:
# - Default: APPLY changes to DB
# - Use --dry-run to preview without writing
#
# Inputs:
# - CSV produced by build_review_csv.py (4b) and edited by a human.
#   Expected columns (minimum):
#     sha256, run_id, review_action
#   Optional columns used if present:
#     review_notes
#     curated_caption
#     curated_add_tags
#     curated_remove_tags
#     curated_suppress_ai_tags
#
# DB tables written (must exist; create via 004_curation_tables.sql):
# - curated_tags(sha256, tag, source, run_id, confidence, note, created_at, updated_at) UNIQUE(sha256, tag)
# - ai_tag_overrides(sha256, tag, action, run_id, note, created_at, updated_at) UNIQUE(sha256, tag, action)
# - curated_captions(sha256 PK, caption, source, run_id, note, updated_at)
# - curation_import_runs(import_id PK, csv_path, accept_threshold, started_at, finished_at, rows_total, rows_applied, rows_skipped, rows_errors)
#
# Reads AI suggestions from:
# - ai_tags(sha256, run_id, tag, score, evidence, created_at)
# - ai_captions(sha256, run_id, caption, ...)
#
# Design notes:
# - Curated tags are SHA-level authoritative semantics.
# - AI tags/captions remain raw evidence; this script does NOT modify ai_* tables.
# - Idempotent: uses UPSERT semantics; safe to rerun on the same CSV.

from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

try:
    import home_automation_common  # type: ignore
    import logging
    import structlog
    _HAS_STRUCTLOG = True
except Exception:
    _HAS_STRUCTLOG = False


# ----------------------------
# Logging
# ----------------------------
def _init_logger(module_name: str, verbose: bool = False):
    if _HAS_STRUCTLOG and hasattr(home_automation_common, "create_logger"):
        # In this repo, create_logger configures logging sinks (side effects)
        home_automation_common.create_logger(module_name)
        logging.getLogger().setLevel(logging.DEBUG if verbose else logging.INFO)
        return structlog.get_logger().bind(module=f"{module_name}.main")

    import logging as _logging
    _logging.basicConfig(level=_logging.DEBUG if verbose else _logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    return _logging.getLogger(module_name)


# ----------------------------
# CLI
# ----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Step 4c: Import reviewed curation CSV (semantic, SHA-level).")
    p.add_argument("--db", required=True, help="Path to SQLite DB")
    p.add_argument("--csv", required=True, dest="csv_path", help="Path to reviewed CSV output from step 4b")
    p.add_argument("--accept-threshold", type=float, default=0.80,
                   help="AI score threshold for promoting tags when review_action=accept (default 0.80)")
    p.add_argument("--only-action", default=None, choices=["accept", "review", "reject", "unaccept"],
                   help="Optional: process only rows with this review_action")
    p.add_argument("--sync-curated-tags", action="store_true",
                   help="When accepting, delete prior ai_promoted curated_tags that are no longer promoted")
    p.add_argument("--limit", type=int, default=0, help="Optional: process only first N rows")
    p.add_argument("--dry-run", action="store_true", help="Preview changes; do not write to DB")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()


# ----------------------------
# DB helpers
# ----------------------------
def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def require_table(conn: sqlite3.Connection, name: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    if not row:
        raise RuntimeError(f"Required table missing: {name}. Did you apply 004_curation_tables.sql?")


def begin_import_run(conn: sqlite3.Connection, csv_path: str, accept_threshold: float) -> int:
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO curation_import_runs(csv_path, accept_threshold, started_at, rows_total, rows_applied, rows_skipped, rows_errors)
        VALUES (?, ?, ?, 0, 0, 0, 0)
        """,
        (csv_path, accept_threshold, now),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def finalize_import_run(conn: sqlite3.Connection, import_id: int, totals: Dict[str, int]) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        UPDATE curation_import_runs
        SET finished_at = ?,
            rows_total = ?,
            rows_applied = ?,
            rows_skipped = ?,
            rows_errors = ?
        WHERE import_id = ?
        """,
        (now, totals["rows_total"], totals["rows_applied"], totals["rows_skipped"], totals["rows_errors"], import_id),
    )


# ----------------------------
# CSV parsing helpers
# ----------------------------
def _split_pipe(value: str) -> List[str]:
    if not value:
        return []
    parts = [p.strip() for p in value.split("|")]
    return [p for p in parts if p]


@dataclass
class ReviewRow:
    sha256: str
    run_id: int
    review_action: str
    review_notes: str

    curated_caption: str
    curated_add_tags: List[str]
    curated_remove_tags: List[str]
    curated_suppress_ai_tags: List[str]


def read_review_rows(csv_path: str, only_action: Optional[str], limit: int) -> List[ReviewRow]:
    rows: List[ReviewRow] = []
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    with p.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required_cols = {"sha256", "run_id", "review_action"}
        missing = required_cols - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"CSV missing required columns: {sorted(missing)}")

        for i, r in enumerate(reader, start=1):
            if limit and len(rows) >= limit:
                break

            sha = (r.get("sha256") or "").strip()
            run_id_s = (r.get("run_id") or "").strip()
            action = (r.get("review_action") or "").strip().lower()

            if not sha or not run_id_s or not action:
                # skip malformed rows
                continue

            if only_action and action != only_action:
                continue

            try:
                run_id = int(run_id_s)
            except ValueError:
                continue

            rows.append(
                ReviewRow(
                    sha256=sha,
                    run_id=run_id,
                    review_action=action,
                    review_notes=(r.get("review_notes") or "").strip(),
                    curated_caption=(r.get("curated_caption") or "").strip(),
                    curated_add_tags=_split_pipe((r.get("curated_add_tags") or "").strip()),
                    curated_remove_tags=_split_pipe((r.get("curated_remove_tags") or "").strip()),
                    curated_suppress_ai_tags=_split_pipe((r.get("curated_suppress_ai_tags") or "").strip()),
                )
            )

    return rows


# ----------------------------
# AI fetch helpers
# ----------------------------
def fetch_ai_tags(conn: sqlite3.Connection, sha256: str, run_id: int, threshold: float) -> List[Tuple[str, float]]:
    rows = conn.execute(
        """
        SELECT tag, score
        FROM ai_tags
        WHERE sha256 = ? AND run_id = ? AND score >= ?
        ORDER BY score DESC, tag ASC
        """,
        (sha256, run_id, threshold),
    ).fetchall()
    return [(r["tag"], float(r["score"])) for r in rows]


def fetch_ai_caption(conn: sqlite3.Connection, sha256: str, run_id: int) -> Optional[str]:
    row = conn.execute(
        """
        SELECT caption
        FROM ai_captions
        WHERE sha256 = ? AND run_id = ?
        """,
        (sha256, run_id),
    ).fetchone()
    return row["caption"] if row else None


# ----------------------------
# Write operations (UPSERT)
# ----------------------------
def upsert_curated_tags(
    conn: sqlite3.Connection,
    sha256: str,
    tags: List[Tuple[str, Optional[float], str]],
    run_id: int,
    note: str,
) -> int:
    """
    tags: list of (tag, confidence, source)
    Returns number of rows attempted (not necessarily changed).
    """
    now = datetime.now().isoformat(timespec="seconds")
    conn.executemany(
        """
        INSERT INTO curated_tags(sha256, tag, source, run_id, confidence, note, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sha256, tag) DO UPDATE SET
            source=excluded.source,
            run_id=excluded.run_id,
            confidence=excluded.confidence,
            note=excluded.note,
            updated_at=excluded.updated_at
        """,
        [
            (sha256, tag, source, run_id, conf, note, now, now)
            for tag, conf, source in tags
        ],
    )
    return len(tags)


def delete_curated_tags(conn: sqlite3.Connection, sha256: str, tags_to_remove: Sequence[str]) -> int:
    if not tags_to_remove:
        return 0
    cur = conn.executemany(
        "DELETE FROM curated_tags WHERE sha256 = ? AND tag = ?",
        [(sha256, t) for t in tags_to_remove],
    )
    # sqlite3 cursor rowcount is unreliable for executemany, return intended count
    return len(tags_to_remove)


def delete_ai_promoted_curated_tags(conn: sqlite3.Connection, sha256: str) -> int:
    cur = conn.execute(
        "DELETE FROM curated_tags WHERE sha256 = ? AND source = 'ai_promoted'",
        (sha256,),
    )
    return cur.rowcount


def delete_ai_accepted_curated_caption(conn: sqlite3.Connection, sha256: str) -> int:
    cur = conn.execute(
        "DELETE FROM curated_captions WHERE sha256 = ? AND source = 'ai_accepted'",
        (sha256,),
    )
    return cur.rowcount


def sync_ai_promoted_tags(conn: sqlite3.Connection, sha256: str, desired_tags: Set[str]) -> int:
    """
    Remove ai_promoted curated tags that are no longer desired for this sha256.
    """
    if desired_tags:
        placeholders = ",".join(["?"] * len(desired_tags))
        sql = f"""
            DELETE FROM curated_tags
            WHERE sha256 = ?
              AND source = 'ai_promoted'
              AND tag NOT IN ({placeholders})
        """
        params = (sha256, *sorted(desired_tags))
    else:
        sql = """
            DELETE FROM curated_tags
            WHERE sha256 = ?
              AND source = 'ai_promoted'
        """
        params = (sha256,)
    cur = conn.execute(sql, params)
    return cur.rowcount


def upsert_ai_overrides(
    conn: sqlite3.Connection,
    sha256: str,
    tags_to_suppress: Sequence[str],
    run_id: int,
    note: str,
) -> int:
    if not tags_to_suppress:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    conn.executemany(
        """
        INSERT INTO ai_tag_overrides(sha256, tag, action, run_id, note, created_at, updated_at)
        VALUES (?, ?, 'suppress', ?, ?, ?, ?)
        ON CONFLICT(sha256, tag, action) DO UPDATE SET
            run_id=excluded.run_id,
            note=excluded.note,
            updated_at=excluded.updated_at
        """,
        [(sha256, t, run_id, note, now, now) for t in tags_to_suppress],
    )
    return len(tags_to_suppress)


def upsert_curated_caption(
    conn: sqlite3.Connection,
    sha256: str,
    caption: str,
    source: str,
    run_id: int,
    note: str,
) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO curated_captions(sha256, caption, source, run_id, note, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(sha256) DO UPDATE SET
            caption=excluded.caption,
            source=excluded.source,
            run_id=excluded.run_id,
            note=excluded.note,
            updated_at=excluded.updated_at
        """,
        (sha256, caption, source, run_id, note, now),
    )


# ----------------------------
# Core logic
# ----------------------------
def compute_curated_tag_set(
    ai_tags: List[Tuple[str, float]],
    add_tags: List[str],
    remove_tags: List[str],
) -> List[Tuple[str, Optional[float], str]]:
    """
    Produce final curated tags as tuples: (tag, confidence, source).
    - ai_tags already filtered by threshold.
    - add_tags are human-added, confidence None (or 1.0 if you prefer).
    - remove_tags removes from final set.
    """
    remove_set = {t.strip() for t in remove_tags if t.strip()}
    add_set = {t.strip() for t in add_tags if t.strip()}

    curated: Dict[str, Tuple[Optional[float], str]] = {}

    for tag, score in ai_tags:
        t = tag.strip()
        if not t or t in remove_set:
            continue
        curated[t] = (float(score), "ai_promoted")

    for t in add_set:
        if t and t not in remove_set:
            curated[t] = (None, "human_added")

    # Return in stable order: ai_promoted by confidence desc, then human_added alpha
    ai_part = [(t, conf, src) for t, (conf, src) in curated.items() if src == "ai_promoted"]
    human_part = [(t, conf, src) for t, (conf, src) in curated.items() if src == "human_added"]

    ai_part.sort(key=lambda x: (-(x[1] or 0.0), x[0]))
    human_part.sort(key=lambda x: x[0])

    return ai_part + human_part


def main() -> None:
    args = parse_args()
    logger = _init_logger("import_curation_csv", verbose=args.verbose)

    conn = connect(args.db)

    # Ensure required tables exist (created via 004_curation_tables.sql)
    for t in ("curated_tags", "ai_tag_overrides", "curated_captions", "curation_import_runs"):
        require_table(conn, t)

    # AI evidence tables required for promotion
    for t in ("ai_tags", "ai_captions"):
        require_table(conn, t)

    rows = read_review_rows(args.csv_path, args.only_action, args.limit)

    totals = {"rows_total": 0, "rows_applied": 0, "rows_skipped": 0, "rows_errors": 0}

    if args.dry_run:
        logger.info("Dry-run enabled: no DB writes will occur",
                    csv=args.csv_path, accept_threshold=args.accept_threshold)
    else:
        logger.info("Applying curation CSV to DB",
                    csv=args.csv_path, accept_threshold=args.accept_threshold)

    import_id: Optional[int] = None
    if not args.dry_run:
        import_id = begin_import_run(conn, args.csv_path, args.accept_threshold)
        conn.commit()

    for rr in rows:
        totals["rows_total"] += 1

        try:
            if rr.review_action not in ("accept", "review", "reject", "unaccept"):
                totals["rows_skipped"] += 1
                continue

            if rr.review_action == "unaccept":
                if args.dry_run:
                    existing_ai_promoted = conn.execute(
                        "SELECT COUNT(*) AS c FROM curated_tags WHERE sha256 = ? AND source = 'ai_promoted'",
                        (rr.sha256,),
                    ).fetchone()["c"]
                    existing_ai_accepted_cap = conn.execute(
                        "SELECT COUNT(*) AS c FROM curated_captions WHERE sha256 = ? AND source = 'ai_accepted'",
                        (rr.sha256,),
                    ).fetchone()["c"]
                    logger.info(
                        "Would unaccept curation",
                        sha256=rr.sha256,
                        run_id=rr.run_id,
                        delete_ai_promoted_tags=existing_ai_promoted,
                        delete_ai_accepted_caption=existing_ai_accepted_cap,
                    )
                    totals["rows_applied"] += 1
                    continue

                deleted_tags = delete_ai_promoted_curated_tags(conn, rr.sha256)
                deleted_caption = delete_ai_accepted_curated_caption(conn, rr.sha256)
                logger.info(
                    "Unaccepted curation",
                    sha256=rr.sha256,
                    run_id=rr.run_id,
                    deleted_ai_promoted_tags=deleted_tags,
                    deleted_ai_accepted_caption=deleted_caption,
                )
                totals["rows_applied"] += 1
                continue

            if rr.review_action != "accept":
                # v1: only apply curation outputs for accept
                totals["rows_skipped"] += 1
                continue

            # Pull AI tags (filtered by threshold)
            ai_tags = fetch_ai_tags(conn, rr.sha256, rr.run_id, args.accept_threshold)

            # Apply manual add/remove
            final_tags = compute_curated_tag_set(ai_tags, rr.curated_add_tags, rr.curated_remove_tags)
            desired_ai_promoted = {t for t, _, src in final_tags if src == "ai_promoted"}

            # Suppress tags (sticky)
            suppress_tags = rr.curated_suppress_ai_tags

            # Caption: prefer curated_caption if provided, else keep AI caption uncurated for now
            caption_to_set: Optional[str] = rr.curated_caption.strip() if rr.curated_caption else None
            caption_source = "human" if caption_to_set else None

            # If user did not provide curated caption, we can optionally "accept" AI caption:
            # Uncomment to store accepted AI caption:
            # if caption_to_set is None:
            #     ai_cap = fetch_ai_caption(conn, rr.sha256, rr.run_id)
            #     if ai_cap:
            #         caption_to_set = ai_cap
            #         caption_source = "ai_accepted"

            note = rr.review_notes.strip()

            if args.dry_run:
                sync_delete_count = 0
                if args.sync_curated_tags:
                    if desired_ai_promoted:
                        placeholders = ",".join(["?"] * len(desired_ai_promoted))
                        row = conn.execute(
                            f"""
                            SELECT COUNT(*) AS c
                            FROM curated_tags
                            WHERE sha256 = ?
                              AND source = 'ai_promoted'
                              AND tag NOT IN ({placeholders})
                            """,
                            (rr.sha256, *sorted(desired_ai_promoted)),
                        ).fetchone()
                    else:
                        row = conn.execute(
                            """
                            SELECT COUNT(*) AS c
                            FROM curated_tags
                            WHERE sha256 = ?
                              AND source = 'ai_promoted'
                            """,
                            (rr.sha256,),
                        ).fetchone()
                    sync_delete_count = row["c"] if row else 0
                # Log what would happen
                logger.info(
                    "Would apply curation",
                    sha256=rr.sha256,
                    run_id=rr.run_id,
                    curated_tags=len(final_tags),
                    suppressed_tags=len(suppress_tags),
                    set_caption=bool(caption_to_set),
                    sync_curated_tags=args.sync_curated_tags,
                    sync_delete_ai_promoted=sync_delete_count,
                )
                totals["rows_applied"] += 1
                continue

            # APPLY DB writes
            wrote = 0

            # Upsert curated tags
            wrote += upsert_curated_tags(
                conn,
                rr.sha256,
                final_tags,
                rr.run_id,
                note=note,
            )

            # Optional sync: remove ai_promoted curated tags no longer desired
            if args.sync_curated_tags:
                sync_ai_promoted_tags(conn, rr.sha256, desired_ai_promoted)

            # Remove tags explicitly requested (ensure removal even if previously curated)
            delete_curated_tags(conn, rr.sha256, rr.curated_remove_tags)

            # Upsert suppressions
            upsert_ai_overrides(conn, rr.sha256, suppress_tags, rr.run_id, note=note)

            # Upsert curated caption if provided
            if caption_to_set and caption_source:
                upsert_curated_caption(conn, rr.sha256, caption_to_set, caption_source, rr.run_id, note=note)

            totals["rows_applied"] += 1

        except Exception as e:
            totals["rows_errors"] += 1
            logger.error("Row processing failed", sha256=rr.sha256, run_id=rr.run_id, error=repr(e))

    if not args.dry_run and import_id is not None:
        finalize_import_run(conn, import_id, totals)
        conn.commit()

    logger.info("Import complete", **totals)


if __name__ == "__main__":
    main()
