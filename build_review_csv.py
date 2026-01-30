# build_review_csv.py
# Step 4b: Build a single review CSV from rule tags + ingest tags + AI enrichment.
#
# Goals:
# - One CSV output, easy to filter/sort
# - Deterministic, re-runnable
# - Default review_action = "accept" only when "safe"
# - accept-threshold is configurable (default 0.80)
# - Optional stub: write review rows into a table later (--write-review-table)
#
# Usage examples:
#   python build_review_csv.py --db C:\media_pipeline\db\media_pipeline.sqlite --out C:\media_pipeline\reports\review_queue.csv
#   python build_review_csv.py --db ... --out review.csv --run-id latest --accept-threshold 0.85
#   python build_review_csv.py --db ... --out review.csv --where-tag needs_review --limit 500
#
# Notes:
# - Assumes:
#     ai_captions(sha256, run_id, caption, source_file_id, created_at)
#     ai_tags(sha256, run_id, tag, score, evidence, created_at)
#     ai_queue(sha256, status, last_error, last_run_id, updated_at)
#     files(file_id, root_id, path, filename, sha256, ...)
#     roots(root_id, base_path)
#     hash_group_rule_tags + rule_tags (tag text)
# - Tag interpretation:
#     State tags: has_library_canonical, originals_only, library_only, multi_library_candidates, provenance_missing, needs_review
#     Ingest tags: folder_year, event, person (you may have prefixes or separate vocab; this script is tolerant)
#
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ----------------------------
# DB helpers
# ----------------------------
def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def pick_run_id(conn: sqlite3.Connection, run_id_arg: str) -> int:
    if run_id_arg.isdigit():
        return int(run_id_arg)

    if run_id_arg.lower() != "latest":
        raise ValueError("--run-id must be an integer or 'latest'")

    row = conn.execute(
        "SELECT run_id FROM ai_caption_runs ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise RuntimeError("No ai_caption_runs found. Run step 4a first.")
    return int(row["run_id"])


def list_target_sha256(conn: sqlite3.Connection, where_tag: Optional[str], limit: int) -> List[str]:
    if where_tag:
        sql = """
        SELECT DISTINCT hgrt.sha256
        FROM hash_group_rule_tags hgrt
        JOIN rule_tags rt ON rt.id = hgrt.tag_id
        WHERE rt.tag = ?
        """
        params: List = [where_tag]
    else:
        sql = "SELECT sha256 FROM hash_groups"
        params = []

    if limit and limit > 0:
        sql += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [r["sha256"] for r in rows]


# ----------------------------
# Path resolution (matches your stored pattern: path may already include filename)
# ----------------------------
def resolve_file_abs_path(conn: sqlite3.Connection, file_id: int) -> Optional[Path]:
    row = conn.execute(
        """
        SELECT f.path, f.filename, r.base_path
        FROM files f
        JOIN roots r ON r.root_id = f.root_id
        WHERE f.file_id = ?
        """,
        (file_id,),
    ).fetchone()

    if not row:
        return None

    base = row["base_path"]
    if not base:
        return None

    path_str = (row["path"] or "").lstrip("\\/")
    filename = (row["filename"] or "")

    p = Path(path_str)
    # If p already ends with the filename, don't append again
    if filename and p.name.lower() == filename.lower():
        rel = p
    else:
        rel = p / filename if filename else p

    return Path(base) / rel


# ----------------------------
# Data accessors
# ----------------------------
def get_ai_caption(conn: sqlite3.Connection, sha256: str, run_id: int) -> Tuple[Optional[str], Optional[int]]:
    row = conn.execute(
        """
        SELECT caption, source_file_id
        FROM ai_captions
        WHERE sha256 = ? AND run_id = ?
        """,
        (sha256, run_id),
    ).fetchone()
    if not row:
        return None, None
    return (row["caption"], row["source_file_id"])


def get_ai_tags(conn: sqlite3.Connection, sha256: str, run_id: int, top_n: int = 8) -> List[Tuple[str, float]]:
    rows = conn.execute(
        """
        SELECT tag, score
        FROM ai_tags
        WHERE sha256 = ? AND run_id = ?
        ORDER BY score DESC, tag ASC
        LIMIT ?
        """,
        (sha256, run_id, top_n),
    ).fetchall()
    return [(r["tag"], float(r["score"])) for r in rows]


def get_ai_queue_status(conn: sqlite3.Connection, sha256: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    # Your 4a script typically upserts ai_queue with last_run_id.
    row = conn.execute(
        """
        SELECT status, last_error, last_run_id
        FROM ai_queue
        WHERE sha256 = ?
        """,
        (sha256,),
    ).fetchone()
    if not row:
        return None, None, None
    return row["status"], row["last_error"], row["last_run_id"]


def get_rule_tags(conn: sqlite3.Connection, sha256: str) -> List[str]:
    rows = conn.execute(
        """
        SELECT rt.tag AS tag, hgrt.value AS value
        FROM hash_group_rule_tags hgrt
        JOIN rule_tags rt ON rt.id = hgrt.tag_id
        WHERE hgrt.sha256 = ?
        ORDER BY rt.tag
        """,
        (sha256,),
    ).fetchall()

    out: List[str] = []
    for r in rows:
        tag = r["tag"]
        val = r["value"]
        if val is None or str(val).strip() == "":
            out.append(tag)
        else:
            out.append(f"{tag}:{val}")
    return out


def split_ingest_tags(tags: List[str]) -> Tuple[Optional[str], List[str], List[str]]:
    """
    Best-effort extraction. Your tag vocab may be:
      folder_year:2012 or folder_year_2012 or just folder_year with value elsewhere.
      event:Christmas or event_christmas
      person:Grace or person_grace

    We support common patterns; unknown tags are left in state_tags.
    """
    folder_year: Optional[str] = None
    events: List[str] = []
    persons: List[str] = []

    for t in tags:
        tl = t.lower()

        # folder year
        if tl.startswith("folder_year:"):
            folder_year = t.split(":", 1)[1].strip()
            continue
        if tl.startswith("folder_year_"):
            folder_year = t.split("_", 2)[2] if tl.count("_") >= 2 else t.split("_", 1)[1]
            continue
        if tl == "folder_year" and folder_year is None:
            folder_year = ""  # unknown value; still indicates presence
            continue

        # event
        if tl.startswith("event:"):
            events.append(t.split(":", 1)[1].strip())
            continue
        if tl.startswith("event_"):
            events.append(t.split("_", 1)[1].strip())
            continue
        if tl == "event":
            events.append("")
            continue

        # person
        if tl.startswith("person:"):
            persons.append(t.split(":", 1)[1].strip())
            continue
        if tl.startswith("person_"):
            persons.append(t.split("_", 1)[1].strip())
            continue
        if tl == "person":
            persons.append("")
            continue

    # de-dupe while preserving order
    def _dedupe(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return folder_year, _dedupe(events), _dedupe(persons)


# ----------------------------
# Review logic
# ----------------------------
STATE_RISK_TAGS = {
    "needs_review",
    "provenance_missing",
    "multi_library_candidates",
}

def compute_priority(state_tags: List[str], ai_error: Optional[str], ai_has_caption: bool, ai_top_score: float) -> int:
    """
    Deterministic priority scoring. Tune later.
    """
    score = 0
    tl = {t.lower() for t in state_tags}

    if "needs_review" in tl:
        score += 100
    if "provenance_missing" in tl:
        score += 80
    if "multi_library_candidates" in tl:
        score += 60
    if "originals_only" in tl:
        score += 30
    if "library_only" in tl:
        score += 10
    if "has_library_canonical" in tl:
        score -= 5  # slightly less urgent

    if ai_error:
        score += 40
    if ai_has_caption:
        score += 5
    if ai_top_score >= 0.90:
        score += 10
    elif ai_top_score >= 0.80:
        score += 5

    return max(score, 0)


def default_action(
    state_tags: List[str],
    ai_status: Optional[str],
    ai_error: Optional[str],
    ai_caption: Optional[str],
    ai_tags: List[Tuple[str, float]],
    accept_threshold: float,
) -> str:
    """
    Default to 'accept' only when safe:
      - no risk state tags
      - no ai errors
      - caption exists
      - top ai tag >= threshold (or no tags -> review)
    Else: 'review'
    """
    tl = {t.lower() for t in state_tags}

    if any(t in tl for t in STATE_RISK_TAGS):
        return "review"

    if ai_status and ai_status.lower() == "error":
        return "review"
    if ai_error:
        return "review"

    if not ai_caption or not ai_caption.strip():
        return "review"

    if not ai_tags:
        return "review"

    top_score = max(s for _, s in ai_tags)
    if top_score < accept_threshold:
        return "review"

    return "accept"


def format_top_ai_tags(ai_tags: List[Tuple[str, float]]) -> str:
    return "|".join([f"{t}:{s:.2f}" for t, s in ai_tags])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Step 4b: Build review CSV from DB state + AI outputs.")
    p.add_argument("--db", required=True, help="Path to SQLite DB")
    p.add_argument("--out", required=True, help="Output CSV path")
    p.add_argument("--run-id", default="latest", help="AI run_id to use (int) or 'latest' (default)")
    p.add_argument("--where-tag", default=None, help="Optional: limit to sha256 having this rule tag (e.g., needs_review)")
    p.add_argument("--limit", type=int, default=0, help="Max sha256 (0 = no limit)")
    p.add_argument("--accept-threshold", type=float, default=0.80, help="AI confidence threshold for default action=accept (default 0.80)")
    p.add_argument("--top-tags", type=int, default=8, help="How many AI tags to include (default 8)")
    p.add_argument("--write-review-table", action="store_true",
                  help="Stub: also write basic rows to review_queue table (optional).")
    return p.parse_args()


def ensure_review_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS review_queue (
            sha256 TEXT PRIMARY KEY,
            run_id INTEGER NOT NULL,
            priority INTEGER NOT NULL,
            review_action TEXT NOT NULL,
            review_reasons TEXT,
            representative_file_id INTEGER,
            representative_abs_path TEXT,
            ai_caption TEXT,
            ai_top_tags TEXT,
            state_tags TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )


def upsert_review_table(conn: sqlite3.Connection, rows: List[Dict]) -> None:
    conn.executemany(
        """
        INSERT INTO review_queue(
            sha256, run_id, priority, review_action, review_reasons,
            representative_file_id, representative_abs_path,
            ai_caption, ai_top_tags, state_tags, updated_at
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(sha256) DO UPDATE SET
            run_id=excluded.run_id,
            priority=excluded.priority,
            review_action=excluded.review_action,
            review_reasons=excluded.review_reasons,
            representative_file_id=excluded.representative_file_id,
            representative_abs_path=excluded.representative_abs_path,
            ai_caption=excluded.ai_caption,
            ai_top_tags=excluded.ai_top_tags,
            state_tags=excluded.state_tags,
            updated_at=excluded.updated_at
        """,
        [
            (
                r["sha256"],
                r["run_id"],
                r["priority"],
                r["review_action"],
                r["review_reasons"],
                r["representative_file_id"],
                r["representative_abs_path"],
                r["ai_caption"],
                r["ai_top_tags"],
                r["state_tags"],
                r["generated_at"],
            )
            for r in rows
        ],
    )


def main() -> None:
    args = parse_args()
    conn = connect(args.db)

    run_id = pick_run_id(conn, args.run_id)
    sha_list = list_target_sha256(conn, args.where_tag, args.limit)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows_out: List[Dict] = []

    for sha in sha_list:
        # Rule/state tags
        tags = get_rule_tags(conn, sha)
        tl = [t.lower() for t in tags]

        # Derive booleans
        has_library_canonical = "has_library_canonical" in tl
        originals_only = "originals_only" in tl
        library_only = "library_only" in tl
        multi_library_candidates = "multi_library_candidates" in tl
        provenance_missing = "provenance_missing" in tl
        needs_review = "needs_review" in tl

        # Ingest tag parsing
        folder_year, event_tags, person_tags = split_ingest_tags(tags)

        # AI enrichment
        ai_caption, source_file_id = get_ai_caption(conn, sha, run_id)
        ai_tags = get_ai_tags(conn, sha, run_id, top_n=args.top_tags)

        ai_status, ai_error, last_run_id = get_ai_queue_status(conn, sha)

        rep_abs_path = None
        if source_file_id is not None:
            rp = resolve_file_abs_path(conn, int(source_file_id))
            rep_abs_path = str(rp) if rp else None

        top_score = max((s for _, s in ai_tags), default=0.0)

        # Reasons
        reasons = []
        if needs_review:
            reasons.append("STATE:needs_review")
        if provenance_missing:
            reasons.append("STATE:provenance_missing")
        if multi_library_candidates:
            reasons.append("STATE:multi_library_candidates")
        if ai_status and ai_status.lower() == "error":
            reasons.append("AI:error")
        if ai_error:
            reasons.append("AI:error_detail")
        if not ai_caption:
            reasons.append("AI:caption_missing")
        if top_score < args.accept_threshold:
            reasons.append("AI:low_confidence")

        review_reasons = "|".join(reasons)

        action = default_action(
            state_tags=tags,
            ai_status=ai_status,
            ai_error=ai_error,
            ai_caption=ai_caption,
            ai_tags=ai_tags,
            accept_threshold=args.accept_threshold,
        )

        priority = compute_priority(
            state_tags=tags,
            ai_error=ai_error,
            ai_has_caption=bool(ai_caption and ai_caption.strip()),
            ai_top_score=top_score,
        )

        rows_out.append({
            "sha256": sha,
            "run_id": run_id,
            "priority": priority,
            "review_action": action,
            "review_notes": "",
            "review_reasons": review_reasons,

            "representative_file_id": source_file_id,
            "representative_abs_path": rep_abs_path,

            "state_tags": "|".join(tags),

            "has_library_canonical": int(has_library_canonical),
            "originals_only": int(originals_only),
            "library_only": int(library_only),
            "multi_library_candidates": int(multi_library_candidates),
            "provenance_missing": int(provenance_missing),
            "needs_review": int(needs_review),

            "folder_year": folder_year if folder_year is not None else "",
            "event_tags": "|".join(event_tags),
            "person_tags": "|".join(person_tags),

            "ai_caption": ai_caption or "",
            "ai_top_tags": format_top_ai_tags(ai_tags),
            "ai_top_score": f"{top_score:.2f}",

            "ai_status": ai_status or "",
            "ai_error": ai_error or "",

            # placeholders for 4c import
            "curated_caption": "",
            "curated_add_tags": "",
            "curated_remove_tags": "",
            "curated_suppress_ai_tags": "",

            "generated_at": datetime.now().isoformat(timespec="seconds"),
        })

    # Write CSV
    fieldnames = [
        "priority",
        "review_action",
        "review_notes",
        "review_reasons",

        "sha256",
        "run_id",

        "representative_file_id",
        "representative_abs_path",

        "state_tags",
        "has_library_canonical",
        "originals_only",
        "library_only",
        "multi_library_candidates",
        "provenance_missing",
        "needs_review",

        "folder_year",
        "event_tags",
        "person_tags",

        "ai_caption",
        "ai_top_tags",
        "ai_top_score",
        "ai_status",
        "ai_error",

        "curated_caption",
        "curated_add_tags",
        "curated_remove_tags",
        "curated_suppress_ai_tags",

        "generated_at",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        # sort by priority desc by default to make the CSV immediately useful
        for r in sorted(rows_out, key=lambda x: int(x["priority"]), reverse=True):
            w.writerow({k: r.get(k, "") for k in fieldnames})

    # Optional stub: write to a review_queue table
    if args.write_review_table:
        ensure_review_table(conn)
        upsert_review_table(conn, rows_out)
        conn.commit()

    print(f"Wrote {len(rows_out)} rows to {out_path}")


if __name__ == "__main__":
    main()
