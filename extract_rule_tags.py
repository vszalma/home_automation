#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import string
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import structlog
from tqdm import tqdm

import home_automation_common


# -----------------------------
# Defaults (safe + minimal)
# -----------------------------
DEFAULT_EVENT_KEYWORDS = {
    "christmas", "xmas", "thanksgiving", "halloween", "easter",
    "birthday", "wedding", "vacation", "trip", "graduation",
}
DEFAULT_STOPWORDS = {
    # "junk" folders / generic buckets (tune via config/tagging_stopwords.txt)
    "dcim", "camera", "screenshots", "download", "downloads", "temp", "tmp",
    "edited", "export", "exports", "misc", "unsorted", "new folder",
    "photos", "pictures", "videos", "img", "images",
}
DEFAULT_PEOPLE_WHITELIST: Set[str] = set()

STATE_TAGS: Dict[str, str] = {
    "has_library_canonical": "Hash group has a canonical library file selected.",
    "originals_only": "Hash group has originals but no library/staging members.",
    "library_only": "Hash group has library/staging members but no originals.",
    "multi_library_candidates": "Hash group has multiple library/staging candidates.",
    "provenance_missing": "Hash group has originals but no canonical_provenance rows.",
    "needs_review": "Rollup tag for anomalies (originals_only/multi_library_candidates/provenance_missing).",
}

INGEST_TAGS: Dict[str, str] = {
    "event": "Event tag derived from staging/original folder segments.",
    "person": "Person tag derived from staging/original folder segments (whitelist-based).",
    "folder_year": "Year derived from folder segments (or year embedded in segment).",
}


@dataclass(frozen=True)
class ConfigLists:
    event_keywords: Set[str]
    stopwords: Set[str]
    people_whitelist: Set[str]


# -----------------------------
# CLI
# -----------------------------
def _get_arguments() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Step 3: derive rule/content tags from DB state and ingest paths.")
    p.add_argument("--db", required=True, help="Path to sqlite database.")
    p.add_argument("--scope", choices=["state", "ingest", "both"], default="both")
    p.add_argument("--roles", default="staging", help="Comma-separated ingest roles (default: staging).")
    p.add_argument("--only-new", action="store_true", help="Ingest: only files from latest run (files.last_seen_run_id == max).")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--config-dir", default="config", help="Directory containing tagging config files (default: config).")
    p.add_argument("--rebuild-state", action="store_true", default=True, help="Rebuild state tags (default true).")
    p.add_argument("--rebuild-ingest", action="store_true", default=False, help="Rebuild ingest tags (default false; sticky).")
    p.add_argument("--batch-size", type=int, default=20000)
    return p.parse_args()


# -----------------------------
# Utilities
# -----------------------------
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _tqdm_enabled() -> bool:
    # tqdm is always installed in your environment, but keep it polite in CI/non-tty
    if os.environ.get("CI", "").lower() in {"1", "true", "yes"}:
        return False
    return sys.stdout.isatty()


def _load_optional_wordlist(path: Path, fallback: Set[str]) -> Set[str]:
    if not path.exists():
        return set(fallback)
    vals: Set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            vals.add(s.lower())
    return vals if vals else set(fallback)


def _load_config_lists(config_dir: Path) -> ConfigLists:
    return ConfigLists(
        event_keywords=_load_optional_wordlist(config_dir / "tagging_event_keywords.txt", DEFAULT_EVENT_KEYWORDS),
        stopwords=_load_optional_wordlist(config_dir / "tagging_stopwords.txt", DEFAULT_STOPWORDS),
        people_whitelist=_load_optional_wordlist(config_dir / "tagging_people_whitelist.txt", DEFAULT_PEOPLE_WHITELIST),
    )


def _open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    # These pragmas are safe for derivation workloads
    try:
        conn.execute("PRAGMA journal_mode=WAL;").fetchone()
    except sqlite3.DatabaseError:
        pass
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def _detect_files_pk(conn: sqlite3.Connection) -> str:
    rows = conn.execute("PRAGMA table_info(files);").fetchall()
    pk_cols = [r["name"] for r in rows if r["pk"] == 1]
    if not pk_cols:
        raise RuntimeError("Could not detect PK column for files table via PRAGMA table_info(files).")
    if len(pk_cols) > 1:
        return "file_id" if "file_id" in pk_cols else pk_cols[0]
    return pk_cols[0]


def _ensure_tag_tables(conn: sqlite3.Connection) -> None:
    # Minimal normalized schema (non-destructive)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rule_tags(
            id INTEGER PRIMARY KEY,
            tag TEXT NOT NULL UNIQUE,
            description TEXT
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hash_group_rule_tags(
            sha256 TEXT NOT NULL,
            tag_id INTEGER NOT NULL,
            value TEXT,
            created_at TEXT,
            updated_at TEXT,
            UNIQUE(sha256, tag_id, value)
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS file_rule_tags(
            file_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            value TEXT,
            created_at TEXT,
            updated_at TEXT,
            UNIQUE(file_id, tag_id, value)
        );
    """)


def _upsert_tags(conn: sqlite3.Connection, tags: Dict[str, str]) -> Dict[str, int]:
    # Keep descriptions updated
    for tag, desc in tags.items():
        conn.execute(
            """
            INSERT INTO rule_tags(tag, description)
            VALUES (?, ?)
            ON CONFLICT(tag) DO UPDATE SET description = excluded.description;
            """,
            (tag, desc),
        )
    placeholders = ",".join(["?"] * len(tags))
    rows = conn.execute(
        f"SELECT id, tag FROM rule_tags WHERE tag IN ({placeholders});",
        list(tags.keys()),
    ).fetchall()
    return {r["tag"]: int(r["id"]) for r in rows}


def _chunked(seq: Sequence[Tuple], n: int) -> Iterable[Sequence[Tuple]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _normalize_segment(seg: str) -> str:
    s = seg.lower().replace("_", " ").replace("-", " ")
    s = s.translate(str.maketrans("", "", string.punctuation))
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _derive_ingest_values_from_path(rel_path: str, cfg: ConfigLists) -> Dict[str, Set[str]]:
    """
    Return dict(tag -> set(values)) derived from folder segments only.
    - event: keyword match anywhere in segment
    - folder_year: segment is YYYY or contains YYYY
    - person: whitelist-only token match
    """
    # Ensure Windows-like splitting even if rel_path contains "/" or we're on a non-Windows host.
    p = PureWindowsPath(rel_path.replace("/", "\\"))
    parts = list(p.parent.parts)  # folder segments only (no filename)

    out: Dict[str, Set[str]] = {"event": set(), "person": set(), "folder_year": set()}

    year_exact = re.compile(r"^(19|20)\d{2}$")
    year_in_text = re.compile(r"(19|20)\d{2}")

    for raw in parts:
        norm = _normalize_segment(raw)
        if not norm:
            continue
        if norm in cfg.stopwords:
            continue

        # Year
        if year_exact.fullmatch(norm):
            out["folder_year"].add(norm)
        else:
            m = year_in_text.search(norm)
            if m:
                out["folder_year"].add(m.group(0))

        # Event keyword match
        for kw in cfg.event_keywords:
            if kw in norm:
                out["event"].add(kw)

        # Person whitelist tokens
        if cfg.people_whitelist:
            for tok in norm.split():
                if tok in cfg.people_whitelist and tok not in cfg.stopwords:
                    out["person"].add(tok)

    # Remove empties
    return {k: v for k, v in out.items() if v}


# -----------------------------
# State tagging
# -----------------------------
def _query_state_facts(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    sql = """
    WITH members AS (
      SELECT
        sha256,
        SUM(CASE WHEN role='original' THEN 1 ELSE 0 END) AS original_count,
        SUM(CASE WHEN role='library' THEN 1 ELSE 0 END) AS library_count,
        SUM(CASE WHEN role='staging' THEN 1 ELSE 0 END) AS staging_count,
        COUNT(*) AS total_members
      FROM file_group_members
      GROUP BY sha256
    ),
    prov AS (
      SELECT sha256, COUNT(*) AS prov_originals
      FROM canonical_provenance
      GROUP BY sha256
    )
    SELECT
      hg.sha256,
      hg.canonical_library_file_id,
      COALESCE(m.original_count,0) AS original_count,
      COALESCE(m.library_count,0) AS library_count,
      COALESCE(m.staging_count,0) AS staging_count,
      COALESCE(m.total_members,0) AS total_members,
      COALESCE(p.prov_originals,0) AS prov_originals
    FROM hash_groups hg
    LEFT JOIN members m ON m.sha256 = hg.sha256
    LEFT JOIN prov p ON p.sha256 = hg.sha256;
    """
    return conn.execute(sql).fetchall()


def _derive_state_tag_rows(
    rows: List[sqlite3.Row],
    state_tag_ids: Dict[str, int],
    now: str,
) -> List[Tuple[str, int, str, str, str]]:
    out_rows: List[Tuple[str, int, str, str, str]] = []
    for r in rows:
        sha = r["sha256"]
        orig = int(r["original_count"])
        lib = int(r["library_count"])
        stg = int(r["staging_count"])
        prov = int(r["prov_originals"])
        canonical = r["canonical_library_file_id"]

        needs_review = False

        if canonical is not None:
            out_rows.append((sha, state_tag_ids["has_library_canonical"], "1", now, now))

        if orig > 0 and (lib + stg) == 0:
            out_rows.append((sha, state_tag_ids["originals_only"], "1", now, now))
            needs_review = True

        if orig == 0 and (lib + stg) > 0:
            out_rows.append((sha, state_tag_ids["library_only"], "1", now, now))

        if (lib + stg) > 1:
            out_rows.append((sha, state_tag_ids["multi_library_candidates"], "1", now, now))
            needs_review = True

        if orig > 0 and prov == 0:
            out_rows.append((sha, state_tag_ids["provenance_missing"], "1", now, now))
            needs_review = True

        if needs_review:
            out_rows.append((sha, state_tag_ids["needs_review"], "1", now, now))

    return out_rows


# -----------------------------
# Ingest tagging
# -----------------------------
def _get_latest_run_id(conn: sqlite3.Connection) -> Optional[int]:
    row = conn.execute("SELECT MAX(last_seen_run_id) AS m FROM files;").fetchone()
    return int(row["m"]) if row and row["m"] is not None else None


def _query_ingest_sources(
    conn: sqlite3.Connection,
    files_pk: str,
    roles: List[str],
    only_new: bool,
    latest_run_id: Optional[int],
) -> List[sqlite3.Row]:
    role_placeholders = ",".join(["?"] * len(roles))
    params: List[object] = list(roles)

    only_new_clause = ""
    if only_new and latest_run_id is not None:
        only_new_clause = " AND f.last_seen_run_id = ? "
        params.append(latest_run_id)

    # IMPORTANT: your schema uses files.path (relative to root)
    sql = f"""
    SELECT
      fgm.sha256,
      fgm.role,
      f.{files_pk} AS file_id,
      f.path AS path
    FROM file_group_members fgm
    JOIN files f ON fgm.file_id = f.{files_pk}
    WHERE fgm.role IN ({role_placeholders})
    {only_new_clause}
    ;
    """
    return conn.execute(sql, params).fetchall()


def _role_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    rows = conn.execute("""
        SELECT role, COUNT(*) AS n
        FROM file_group_members
        GROUP BY role
        ORDER BY n DESC;
    """).fetchall()
    return {r["role"]: int(r["n"]) for r in rows}


def _derive_ingest_tag_rows(
    source_rows: List[sqlite3.Row],
    cfg: ConfigLists,
    ingest_tag_ids: Dict[str, int],
    now: str,
) -> Tuple[List[Tuple[str, int, str, str, str]], int, int]:
    """
    Returns:
      - tag rows: (sha256, tag_id, value, created_at, updated_at)
      - sha_tagged count
      - conflict_shas count (sha with >1 event or >1 person values)
    """
    sha_to_values: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))

    it = tqdm(source_rows, desc="Parsing ingest paths", disable=not _tqdm_enabled())
    for r in it:
        sha = r["sha256"]
        rel_path = r["path"]
        derived = _derive_ingest_values_from_path(rel_path, cfg)
        for tag, values in derived.items():
            for v in values:
                sha_to_values[sha][tag].add(v)

    conflict_shas: Set[str] = set()
    for sha, d in sha_to_values.items():
        if len(d.get("event", set())) > 1 or len(d.get("person", set())) > 1:
            conflict_shas.add(sha)

    out_rows: List[Tuple[str, int, str, str, str]] = []
    for sha, d in sha_to_values.items():
        for tag, values in d.items():
            tid = ingest_tag_ids[tag]
            for v in values:
                out_rows.append((sha, tid, v, now, now))

    return out_rows, len(sha_to_values), len(conflict_shas)


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    args = _get_arguments()

    home_automation_common.create_logger("extract_rule_tags")
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.INFO))
    logger = structlog.get_logger().bind(module="extract_rule_tags")

    if not os.path.exists(args.db):
        raise FileNotFoundError(args.db)

    conn = _open_db(args.db)
    try:
        _ensure_tag_tables(conn)
        files_pk = _detect_files_pk(conn)  # will be "file_id" in your schema
        cfg = _load_config_lists(Path(args.config_dir))

        now = _now_iso()

        # Ensure tag definitions exist
        tags_to_define: Dict[str, str] = {}
        if args.scope in ("state", "both"):
            tags_to_define.update(STATE_TAGS)
        if args.scope in ("ingest", "both"):
            tags_to_define.update(INGEST_TAGS)

        with conn:
            tag_ids = _upsert_tags(conn, tags_to_define)

        # -------------------------
        # STATE TAGS
        # -------------------------
        if args.scope in ("state", "both"):
            state_tag_ids = {k: tag_ids[k] for k in STATE_TAGS.keys()}
            if args.rebuild_state and not args.dry_run:
                with conn:
                    conn.execute(
                        f"DELETE FROM hash_group_rule_tags WHERE tag_id IN ({','.join(['?'] * len(state_tag_ids))});",
                        list(state_tag_ids.values()),
                    )

            state_rows = _query_state_facts(conn)
            state_tag_rows = _derive_state_tag_rows(state_rows, state_tag_ids, now)
            logger.info("State tags derived", sha_count=len(state_rows), tag_rows=len(state_tag_rows))

            if not args.dry_run and state_tag_rows:
                with conn:
                    # Since we deleted by tag_id, INSERT OR IGNORE is fine
                    conn.executemany(
                        """
                        INSERT OR IGNORE INTO hash_group_rule_tags(sha256, tag_id, value, created_at, updated_at)
                        VALUES (?,?,?,?,?);
                        """,
                        state_tag_rows,
                    )

        # -------------------------
        # INGEST TAGS (sticky by default)
        # -------------------------
        if args.scope in ("ingest", "both"):
            ingest_tag_ids = {k: tag_ids[k] for k in INGEST_TAGS.keys()}

            roles = [r.strip().lower() for r in args.roles.split(",") if r.strip()]
            if not roles:
                roles = ["staging"]

            latest_run_id = _get_latest_run_id(conn) if args.only_new else None
            if args.only_new:
                logger.info("Only-new enabled", latest_run_id=latest_run_id)

            if args.rebuild_ingest and not args.dry_run:
                with conn:
                    conn.execute(
                        f"DELETE FROM hash_group_rule_tags WHERE tag_id IN ({','.join(['?'] * len(ingest_tag_ids))});",
                        list(ingest_tag_ids.values()),
                    )

            ingest_sources = _query_ingest_sources(conn, files_pk, roles, args.only_new, latest_run_id)

            if len(ingest_sources) == 0:
                counts = _role_counts(conn)
                suggestions: List[str] = []
                if "staging" in roles and counts.get("staging", 0) == 0:
                    suggestions.append("Staging appears empty. For initial harvest, try: --roles original")
                if args.only_new:
                    suggestions.append("Try removing --only-new (it may be filtering everything).")
                if not suggestions:
                    suggestions.append("Verify file_group_members contains rows for the requested roles.")

                logger.warning(
                    "No ingest-source rows found for requested roles/filters.",
                    requested_roles=roles,
                    only_new=args.only_new,
                    latest_run_id=latest_run_id,
                    available_role_counts=counts,
                    suggestions=suggestions,
                )
            else:
                ingest_tag_rows, sha_tagged, conflict_shas = _derive_ingest_tag_rows(
                    ingest_sources, cfg, ingest_tag_ids, now
                )

                logger.info(
                    "Ingest tags derived",
                    requested_roles=roles,
                    only_new=args.only_new,
                    source_rows=len(ingest_sources),
                    sha_tagged=sha_tagged,
                    tag_rows=len(ingest_tag_rows),
                    conflicts=conflict_shas,
                    sticky=(not args.rebuild_ingest),
                )

                if not args.dry_run and ingest_tag_rows:
                    with conn:
                        # Sticky behavior: insert-only by default
                        conn.executemany(
                            """
                            INSERT OR IGNORE INTO hash_group_rule_tags(sha256, tag_id, value, created_at, updated_at)
                            VALUES (?,?,?,?,?);
                            """,
                            ingest_tag_rows,
                        )

        # -------------------------
        # Summary counts (by tag)
        # -------------------------
        report_tags: List[str] = []
        if args.scope in ("state", "both"):
            report_tags.extend(list(STATE_TAGS.keys()))
        if args.scope in ("ingest", "both"):
            report_tags.extend(list(INGEST_TAGS.keys()))

        if report_tags:
            placeholders = ",".join(["?"] * len(report_tags))
            rep = conn.execute(
                f"""
                SELECT rt.tag, COUNT(*) AS n
                FROM hash_group_rule_tags hgt
                JOIN rule_tags rt ON rt.id = hgt.tag_id
                WHERE rt.tag IN ({placeholders})
                GROUP BY rt.tag
                ORDER BY n DESC;
                """,
                report_tags,
            ).fetchall()

            for r in rep:
                logger.info("Tag count", tag=r["tag"], count=int(r["n"]))

        logger.info("extract_rule_tags complete", scope=args.scope, dry_run=args.dry_run)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
