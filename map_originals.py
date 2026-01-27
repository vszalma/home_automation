#!/usr/bin/env python3
"""
map_originals.py

Step 2:
- Choose/confirm hash_groups.canonical_library_file_id (library-first, staging-second).
- Populate canonical_provenance(sha256, original_file_id, weight, note).

IMPORTANT DESIGN INVARIANT (default behavior)
---------------------------------------------
By default, this script will NOT use role='original' files as the canonical target.

That is intentional: canonical_library_file_id is meant to represent the canonical
*library-of-record* file for a sha256 hash group. If no library (or staging) file
exists, canonical_library_file_id is left NULL, and that NULL is a useful signal:
"This content exists in Original but has not been represented/ingested into the Library."

OPTIONAL / DIAGNOSTIC MODE: --allow-original-fallback
-----------------------------------------------------
If you pass --allow-original-fallback, the script is allowed to select an 'original'
file as canonical *only when no library/staging candidate exists*.

Why this flag exists:
- During messy migrations / incomplete backfills, you may want *every* hash_group to
  have a deterministic "anchor" file_id for reporting or auditing.
- You may want to temporarily anchor groups prior to copying into staging/library.

Why the flag is OFF by default:
- If you accidentally allow originals to be canonical, later logic that assumes
  canonicals live under the library root can cause incorrect cleanup decisions or
  confusing provenance interpretation.

In short:
- Default (recommended): library → staging → (no canonical; leave NULL)
- With fallback flag:      library → staging → original (as a temporary/diagnostic anchor)

Idempotency:
- Canonical: updated only when NULL, unless --force-canonical is used.
- Provenance: by default deletes/rebuilds per sha256; use --append-provenance to avoid deletes.

Windows notes:
- DB stores paths relative to roots; we normalize separators for similarity scoring.
"""

from __future__ import annotations

import argparse
import difflib
import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from tqdm import tqdm


# ---- Adjust if your files table PK differs ----
FILES_ID_COL = "id"  # Change to "file_id" if needed.


def get_logger():
    """
    Try to use your repo's shared logging helper.
    Falls back gracefully if import fails.
    """
    try:
        import home_automation_common  # type: ignore

        if hasattr(home_automation_common, "get_logger"):
            return home_automation_common.get_logger(__name__)
    except Exception:
        pass

    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return logging.getLogger("map_originals")


logger = get_logger()


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def normalize_rel_path(p: str) -> str:
    p = p.replace("\\", "/").strip("/")
    return p


def split_path_parts(p: str) -> List[str]:
    p = normalize_rel_path(p)
    return [part for part in p.split("/") if part]


def stem_and_ext(path: str) -> Tuple[str, str]:
    base = os.path.basename(path.replace("\\", "/"))
    stem, ext = os.path.splitext(base)
    return (stem.lower(), ext.lower().lstrip("."))


def seq_similarity(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(a=a, b=b).ratio()


def jaccard(a: Sequence[str], b: Sequence[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


@dataclass(frozen=True)
class FileRow:
    file_id: int
    path: str
    ext: Optional[str]
    status: Optional[str]
    role: str


def fetch_group_members(conn: sqlite3.Connection, sha256: str) -> List[FileRow]:
    cols_files = table_columns(conn, "files")
    has_ext = "ext" in cols_files
    has_status = "status" in cols_files

    select_cols = [
        f"f.{FILES_ID_COL} AS file_id",
        "f.path AS path",
        ("f.ext AS ext" if has_ext else "NULL AS ext"),
        ("f.status AS status" if has_status else "NULL AS status"),
        "m.role AS role",
    ]

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM file_group_members m
    JOIN files f ON f.{FILES_ID_COL} = m.file_id
    WHERE m.sha256 = ?
    """
    rows = conn.execute(sql, (sha256,)).fetchall()
    return [
        FileRow(
            file_id=int(r[0]),
            path=str(r[1]),
            ext=(str(r[2]) if r[2] is not None else None),
            status=(str(r[3]) if r[3] is not None else None),
            role=str(r[4]),
        )
        for r in rows
    ]


def status_is_good(status: Optional[str]) -> bool:
    if not status:
        return True
    s = status.strip().lower()
    return s in {"ok", "good", "active", "present", "verified", "ready"}


def choose_canonical_candidate(
    members: List[FileRow],
    allow_original_fallback: bool,
) -> Optional[FileRow]:
    """
    Choose a canonical candidate for the group.

    DEFAULT (recommended):
      - Prefer role='library'
      - Else role='staging'
      - Else return None  (leave canonical NULL; indicates "not in library yet")

    OPTIONAL (--allow-original-fallback):
      - If no library/staging exists, allow role='original' as a *temporary/diagnostic anchor*.

    Notes:
      - We DO NOT automatically pick 'original' unless the flag is set.
      - Within a role bucket, we pick deterministically:
          good status first, then shorter path, then lowest file_id.
    """
    if not members:
        return None

    role_order = ["library", "staging"]
    if allow_original_fallback:
        # WARNING: This changes the meaning of canonical_library_file_id to be
        # "canonical member file_id" (not necessarily within the library root).
        # Use only for analysis/migration and ideally plan to upgrade canonicals later.
        role_order.append("original")

    for role in role_order:
        bucket = [m for m in members if m.role.lower() == role]
        if not bucket:
            continue

        scored: List[Tuple[Tuple[int, int, int], FileRow]] = []
        for m in bucket:
            good = 1 if status_is_good(m.status) else 0
            path_len = len(normalize_rel_path(m.path))
            key = (-good, path_len, m.file_id)
            scored.append((key, m))
        scored.sort(key=lambda x: x[0])
        return scored[0][1]

    return None


def compute_provenance_weight(
    original_path: str,
    canonical_path: str,
    original_ext: Optional[str],
    canonical_ext: Optional[str],
) -> Tuple[float, str]:
    op = normalize_rel_path(original_path)
    cp = normalize_rel_path(canonical_path)

    o_parts = split_path_parts(op)
    c_parts = split_path_parts(cp)

    o_stem, o_ext2 = stem_and_ext(op)
    c_stem, c_ext2 = stem_and_ext(cp)

    ox = (original_ext or o_ext2 or "").lower().lstrip(".")
    cx = (canonical_ext or c_ext2 or "").lower().lstrip(".")

    stem_sim = seq_similarity(o_stem, c_stem)
    dir_sim = jaccard(o_parts[:-1], c_parts[:-1])
    ext_match = 1.0 if (ox and cx and ox == cx) else 0.0

    weight = 0.60 * stem_sim + 0.30 * dir_sim + 0.10 * ext_match
    weight = max(0.0, min(1.0, weight))

    note = f"stem_sim={stem_sim:.3f}; dir_sim={dir_sim:.3f}; ext_match={int(ext_match)}"
    return weight, note


def ensure_indexes(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fgm_sha ON file_group_members(sha256)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fgm_file ON file_group_members(file_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hash_groups_sha ON hash_groups(sha256)")
    conn.commit()


def iter_hash_groups(conn: sqlite3.Connection, only_missing_canonical: bool) -> Iterable[Tuple[str, Optional[int]]]:
    if only_missing_canonical:
        sql = "SELECT sha256, canonical_library_file_id FROM hash_groups WHERE canonical_library_file_id IS NULL"
    else:
        sql = "SELECT sha256, canonical_library_file_id FROM hash_groups"
    for sha, cid in conn.execute(sql):
        yield str(sha), (int(cid) if cid is not None else None)


def upsert_canonical(conn: sqlite3.Connection, sha256: str, canonical_file_id: int, force: bool) -> bool:
    if force:
        cur = conn.execute(
            "UPDATE hash_groups SET canonical_library_file_id = ? WHERE sha256 = ?",
            (canonical_file_id, sha256),
        )
        return cur.rowcount > 0

    cur = conn.execute(
        """
        UPDATE hash_groups
        SET canonical_library_file_id = ?
        WHERE sha256 = ?
          AND canonical_library_file_id IS NULL
        """,
        (canonical_file_id, sha256),
    )
    return cur.rowcount > 0


def delete_provenance_for_sha(conn: sqlite3.Connection, sha256: str) -> None:
    conn.execute("DELETE FROM canonical_provenance WHERE sha256 = ?", (sha256,))


def insert_provenance(conn: sqlite3.Connection, sha256: str, original_file_id: int, weight: float, note: str) -> None:
    conn.execute(
        """
        INSERT INTO canonical_provenance (sha256, original_file_id, weight, note)
        VALUES (?, ?, ?, ?)
        """,
        (sha256, original_file_id, float(weight), note),
    )


def fetch_file_by_id(conn: sqlite3.Connection, file_id: int) -> Tuple[str, Optional[str], Optional[str]]:
    cols = table_columns(conn, "files")
    has_ext = "ext" in cols
    has_status = "status" in cols

    sql = f"""
    SELECT path,
           {('ext' if has_ext else 'NULL')},
           {('status' if has_status else 'NULL')}
    FROM files
    WHERE {FILES_ID_COL} = ?
    """
    row = conn.execute(sql, (file_id,)).fetchone()
    if not row:
        raise RuntimeError(f"files row not found for {FILES_ID_COL}={file_id}")
    return str(row[0]), (str(row[1]) if row[1] is not None else None), (str(row[2]) if row[2] is not None else None)


# -------------------------
# Summary-report helpers
# -------------------------

@dataclass
class RoleCounts:
    library: int = 0
    staging: int = 0
    original: int = 0

    def any(self) -> bool:
        return (self.library + self.staging + self.original) > 0


def summarize_roles(members: List[FileRow]) -> RoleCounts:
    rc = RoleCounts()
    for m in members:
        r = m.role.lower()
        if r == "library":
            rc.library += 1
        elif r == "staging":
            rc.staging += 1
        elif r == "original":
            rc.original += 1
    return rc


def build_summary_report(
    total_groups: int,
    updated_canonical: int,
    provenance_inserted: int,
    skipped_no_candidate: int,
    stats: Dict[str, int],
) -> str:
    """
    Build a human-readable summary that is useful 6-12 months later.

    stats keys used below are maintained in main() as we process each group.
    """
    lines: List[str] = []
    lines.append("Summary report")
    lines.append("--------------")
    lines.append(f"Groups processed: {total_groups}")
    lines.append(f"Canonical updated (db writes): {updated_canonical}")
    lines.append(f"Provenance rows inserted: {provenance_inserted}")
    lines.append(f"Groups with no canonical candidate (left NULL): {skipped_no_candidate}")
    lines.append("")

    # Canonical outcomes
    lines.append("Canonical selection outcomes")
    lines.append(f"- Selected library canonical: {stats.get('canonical_selected_library', 0)}")
    lines.append(f"- Selected staging canonical: {stats.get('canonical_selected_staging', 0)}")
    lines.append(f"- Selected original canonical (fallback): {stats.get('canonical_selected_original', 0)}")
    lines.append("")

    # Useful “health” signals
    lines.append("Library ingestion signals (group composition)")
    lines.append(f"- Groups with originals AND library/staging: {stats.get('groups_with_original_and_lib_or_stage', 0)}")
    lines.append(f"- Groups with originals ONLY (no library/staging): {stats.get('groups_original_only', 0)}")
    lines.append(f"- Groups with library/staging ONLY (no originals): {stats.get('groups_lib_or_stage_only', 0)}")
    lines.append(f"- Groups with no members (should be rare): {stats.get('groups_empty', 0)}")
    lines.append("")

    # Provenance signals
    lines.append("Provenance signals")
    lines.append(f"- Groups where provenance was written (had canonical + originals): {stats.get('groups_with_provenance_written', 0)}")
    lines.append(f"- Groups with originals but NO canonical => no provenance written: {stats.get('groups_originals_but_no_canonical', 0)}")
    lines.append("")

    lines.append("Notes")
    lines.append("- 'Groups with originals ONLY' is typically your backlog to ingest/copy into staging/library.")
    lines.append("- If 'Selected original canonical' is non-zero, you likely ran with --allow-original-fallback.")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description="Step 2: Map originals to canonical library files per sha256 group.")
    ap.add_argument("--db", required=True, help="Path to sqlite database file.")
    ap.add_argument("--only-missing-canonical", action="store_true",
                    help="Process only hash_groups missing canonical_library_file_id.")
    ap.add_argument("--force-canonical", action="store_true",
                    help="Overwrite canonical_library_file_id even if already set.")
    ap.add_argument(
        "--allow-original-fallback",
        action="store_true",
        help=(
            "ALLOW role='original' to be chosen as canonical ONLY if no library/staging candidate exists. "
            "This is OFF by default to preserve the invariant that canonical_library_file_id points to a "
            "library-of-record file. Use this only for migration diagnostics or if you explicitly want every "
            "hash_group to have some canonical anchor even when the content has not been ingested into the Library."
        ),
    )
    ap.add_argument("--append-provenance", action="store_true",
                    help="Do not delete existing provenance rows for sha before inserting.")
    ap.add_argument("--min-weight", type=float, default=0.0,
                    help="Drop provenance rows below this weight (0..1).")
    ap.add_argument("--commit-every", type=int, default=500,
                    help="Commit every N groups (default 500).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process only first N groups (debug).")
    ap.add_argument("--summary-sample", type=int, default=0,
                    help="Include up to N example sha256 values for key categories in the summary (0=off).")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    ensure_indexes(conn)

    # Basic schema sanity checks (fail fast if migrations didn't run)
    for t in ["files", "hash_groups", "file_group_members", "canonical_provenance"]:
        conn.execute(f"SELECT 1 FROM {t} LIMIT 1")

    updated_canonical = 0
    skipped_no_candidate = 0
    provenance_inserted = 0
    groups_processed = 0

    # Summary stats
    stats: Dict[str, int] = {
        "canonical_selected_library": 0,
        "canonical_selected_staging": 0,
        "canonical_selected_original": 0,
        "groups_with_original_and_lib_or_stage": 0,
        "groups_original_only": 0,
        "groups_lib_or_stage_only": 0,
        "groups_empty": 0,
        "groups_with_provenance_written": 0,
        "groups_originals_but_no_canonical": 0,
    }

    # Optional: collect a few example sha256s to make the summary more actionable
    samples: Dict[str, List[str]] = {
        "groups_original_only": [],
        "groups_with_original_and_lib_or_stage": [],
        "groups_originals_but_no_canonical": [],
    }

    groups_iter = list(iter_hash_groups(conn, only_missing_canonical=args.only_missing_canonical))
    if args.limit and args.limit > 0:
        groups_iter = groups_iter[: args.limit]

    logger.info(
        "Starting map_originals",
        db=args.db,
        groups=len(groups_iter),
        only_missing_canonical=bool(args.only_missing_canonical),
        force_canonical=bool(args.force_canonical),
        allow_original_fallback=bool(args.allow_original_fallback),
        append_provenance=bool(args.append_provenance),
        min_weight=args.min_weight,
    )

    for i, (sha, existing_canonical_id) in enumerate(tqdm(groups_iter, desc="hash_groups", unit="group")):
        groups_processed += 1

        members = fetch_group_members(conn, sha)
        rc = summarize_roles(members)

        if not rc.any():
            stats["groups_empty"] += 1

        has_lib_or_stage = (rc.library + rc.staging) > 0
        has_orig = rc.original > 0

        if has_orig and has_lib_or_stage:
            stats["groups_with_original_and_lib_or_stage"] += 1
            if args.summary_sample and len(samples["groups_with_original_and_lib_or_stage"]) < args.summary_sample:
                samples["groups_with_original_and_lib_or_stage"].append(sha)
        elif has_orig and not has_lib_or_stage:
            stats["groups_original_only"] += 1
            if args.summary_sample and len(samples["groups_original_only"]) < args.summary_sample:
                samples["groups_original_only"].append(sha)
        elif has_lib_or_stage and not has_orig:
            stats["groups_lib_or_stage_only"] += 1

        # Choose candidate following the invariant described above.
        candidate = choose_canonical_candidate(
            members,
            allow_original_fallback=args.allow_original_fallback,
        )

        # If candidate is None, we intentionally leave canonical NULL (default behavior).
        if candidate is None:
            skipped_no_candidate += 1
            if has_orig:
                stats["groups_originals_but_no_canonical"] += 1
                if args.summary_sample and len(samples["groups_originals_but_no_canonical"]) < args.summary_sample:
                    samples["groups_originals_but_no_canonical"].append(sha)
            continue

        # Track what we selected (even if we don't end up writing due to canonical already set and not forced)
        sel_role = candidate.role.lower()
        if sel_role == "library":
            stats["canonical_selected_library"] += 1
        elif sel_role == "staging":
            stats["canonical_selected_staging"] += 1
        elif sel_role == "original":
            stats["canonical_selected_original"] += 1

        did_update = upsert_canonical(conn, sha, candidate.file_id, force=args.force_canonical)
        if did_update:
            updated_canonical += 1

        canonical_id = candidate.file_id if (args.force_canonical or existing_canonical_id is None) else existing_canonical_id
        if canonical_id is None:
            continue

        canonical_path, canonical_ext, _canonical_status = fetch_file_by_id(conn, canonical_id)

        originals = [m for m in members if m.role.lower() == "original"]
        if not originals:
            continue

        if not args.append_provenance:
            delete_provenance_for_sha(conn, sha)

        wrote_any = False
        for o in originals:
            weight, note = compute_provenance_weight(
                original_path=o.path,
                canonical_path=canonical_path,
                original_ext=o.ext,
                canonical_ext=canonical_ext,
            )
            if weight < args.min_weight:
                continue
            insert_provenance(conn, sha, o.file_id, weight, note)
            provenance_inserted += 1
            wrote_any = True

        if wrote_any:
            stats["groups_with_provenance_written"] += 1

        if args.commit_every and (i + 1) % args.commit_every == 0:
            conn.commit()

    conn.commit()
    conn.close()

    # Main completion log
    logger.info(
        "Completed map_originals",
        groups_processed=groups_processed,
        updated_canonical=updated_canonical,
        skipped_no_candidate=skipped_no_candidate,
        provenance_inserted=provenance_inserted,
    )

    # Summary report (human-readable)
    report = build_summary_report(
        total_groups=groups_processed,
        updated_canonical=updated_canonical,
        provenance_inserted=provenance_inserted,
        skipped_no_candidate=skipped_no_candidate,
        stats=stats,
    )
    print("")
    print(report)

    # Optional: show example sha256s in key categories
    if args.summary_sample and args.summary_sample > 0:
        print("")
        print("Summary samples (sha256)")
        print("-----------------------")
        for k, vals in samples.items():
            if not vals:
                continue
            print(f"{k}:")
            for v in vals:
                print(f"  - {v}")


if __name__ == "__main__":
    main()
