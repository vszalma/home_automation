from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import sqlite3
import structlog
try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - ensure graceful behavior without tqdm
    tqdm = None

import home_automation_common
from db import open_db, utc_now_iso, begin_run, end_run_failed, end_run_ok

IMAGE_EXTS: Set[str] = {
    ".jpg",
    ".jpeg",
    ".png",
    ".tif",
    ".tiff",
    ".heic",
    ".heif",
    ".webp",
    ".bmp",
    ".gif",
    ".nef",
    ".cr2",
    ".cr3",
    ".arw",
    ".dng",
    ".raf",
    ".orf",
    ".rw2",
}
VIDEO_EXTS: Set[str] = {
    ".mp4",
    ".mov",
    ".m4v",
    ".avi",
    ".mkv",
    ".mts",
    ".m2ts",
    ".3gp",
}
SKIP_DIRS = {"$RECYCLE.BIN", "System Volume Information", ".git", ".svn"}
HASH_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB
TQDM_AVAILABLE = tqdm is not None


def _tqdm_enabled() -> bool:
    if not TQDM_AVAILABLE:
        return False
    if os.environ.get("CI", "").lower() in {"1", "true", "yes"}:
        return False
    return sys.stdout.isatty()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan media roots and inventory into SQLite.")
    parser.add_argument("--db", required=True, help="Path to SQLite db file.")
    parser.add_argument("--root", required=False, help="Name of root to scan (else all active).")
    parser.add_argument("--include-ext", required=False, help="Comma-separated list of extensions to include.")
    parser.add_argument(
        "--media-type",
        choices=["image", "video", "other", "all"],
        default="all",
        help="Filter by media type. Default all.",
    )
    parser.add_argument(
        "--hash-mode",
        choices=["none", "missing", "missing_or_changed"],
        default="missing_or_changed",
        help="Hashing strategy. Default missing_or_changed.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Worker threads for hashing (0 = single-thread).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB or hash; just report counts.")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging.")
    parser.add_argument(
        "--max-errors",
        type=int,
        default=0,
        help="Maximum per-file errors before aborting (0 = unlimited; for debugging).",
    )
    return parser.parse_args()


def resolve_media_type(ext: str) -> str:
    if ext in IMAGE_EXTS:
        return "image"
    if ext in VIDEO_EXTS:
        return "video"
    return "other"


def should_include(ext: str, include_filter: Optional[Set[str]], media_type_filter: str) -> bool:
    if include_filter is not None and ext not in include_filter:
        return False
    media_type = resolve_media_type(ext)
    if media_type_filter == "all":
        return True
    return media_type == media_type_filter


def hash_file(path: Path) -> Tuple[Optional[str], Optional[str]]:
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            while True:
                chunk = f.read(HASH_CHUNK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest(), None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def fetch_roots(conn: sqlite3.Connection, root_name: Optional[str]) -> List[sqlite3.Row]:
    if root_name:
        rows = conn.execute(
            "SELECT root_id, name, base_path, type FROM roots WHERE is_active=1 AND name=?",
            (root_name,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT root_id, name, base_path, type FROM roots WHERE is_active=1",
        ).fetchall()
    return rows


def load_existing_file(conn: sqlite3.Connection, root_id: int, rel_path: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT file_id, size_bytes, mtime_utc, sha256 FROM files WHERE root_id=? AND path=?",
        (root_id, rel_path),
    ).fetchone()


def ensure_hash_group(conn: sqlite3.Connection, sha256: str, run_id: int) -> None:
    conn.execute(
        """
        INSERT INTO hash_groups(sha256, first_seen_run_id, last_seen_run_id)
        VALUES (?, ?, ?)
        ON CONFLICT(sha256) DO UPDATE SET last_seen_run_id=excluded.last_seen_run_id
        """,
        (sha256, run_id, run_id),
    )


def ensure_membership(conn: sqlite3.Connection, sha256: str, file_id: int, role: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO file_group_members(sha256, file_id, role)
        VALUES (?, ?, ?)
        """,
        (sha256, file_id, role),
    )


def sanitize_ext_list(raw: Optional[str]) -> Optional[Set[str]]:
    if not raw:
        return None
    exts = set()
    for part in raw.split(","):
        p = part.strip().lower()
        if not p:
            continue
        if not p.startswith("."):
            p = "." + p
        exts.add(p)
    return exts if exts else None


def iso_from_timestamp(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def mark_missing(conn: sqlite3.Connection, root_id: int, run_id: int) -> int:
    result = conn.execute(
        """
        UPDATE files
        SET status='missing', updated_utc=?, last_seen_run_id=?
        WHERE root_id=? AND status='active' AND last_seen_run_id IS NOT ? AND last_seen_run_id!=?
        """,
        (utc_now_iso(), run_id, root_id, run_id, run_id),
    )
    return result.rowcount if result is not None else 0


def scan_root(
    conn: sqlite3.Connection,
    root: sqlite3.Row,
    args: argparse.Namespace,
    run_id: Optional[int],
    counts: Dict[str, int],
    logger,
) -> None:
    base_path = Path(root["base_path"])
    role = {"original": "original", "library": "library", "staging": "staging"}.get(root["type"], "library")
    include_filter = sanitize_ext_list(args.include_ext)
    pending_hashes: List[Dict[str, object]] = []
    seen_paths: Set[str] = set()
    error_count = 0
    sample_logged = 0
    progress = tqdm(
        total=None,
        desc=f"{root['name']} {root['base_path']}",
        unit="files",
        leave=False,
        disable=not _tqdm_enabled(),
    ) if _tqdm_enabled() else None

    if not base_path.exists():
        msg = "Base path missing; skipping root"
        if progress is not None:
            tqdm.write(msg)
            progress.close()
        logger.warning(msg, module="scan.scan_root", base_path=str(base_path), run_id=run_id)
        return

    if not args.dry_run:
        conn.execute("BEGIN;")

    try:
        for dirpath, dirnames, filenames in os.walk(base_path):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for filename in filenames:
                rel_path = ""
                full_path = Path(dirpath) / filename
                try:
                    try:
                        rel_path = str(full_path.relative_to(base_path))
                    except ValueError:
                        rel_path = os.path.relpath(full_path, base_path)
                    ext = full_path.suffix.lower()
                    if not should_include(ext, include_filter, args.media_type):
                        continue
                    if args.verbose and sample_logged < 5:
                        msg = "Example file discovered"
                        if progress is not None:
                            tqdm.write(msg)  # keep progress bar clean while logging
                        logger.info(
                            msg,
                            module="scan.scan_root",
                            file=str(full_path),
                            root=root["name"],
                            base_path=str(base_path),
                            run_id=run_id,
                        )
                        sample_logged += 1

                    seen_paths.add(rel_path)
                    try:
                        st = full_path.stat()
                    except Exception as exc:  # noqa: BLE001
                        msg = "Stat failed; skipping file"
                        if progress is not None:
                            tqdm.write(msg)
                        logger.warning(
                            msg,
                            module="scan.scan_root",
                            file=str(full_path),
                            error=str(exc),
                            run_id=run_id,
                        )
                        continue

                    mtime_iso = iso_from_timestamp(st.st_mtime)
                    size_bytes = int(st.st_size)
                    media_type = resolve_media_type(ext)
                    if media_type not in {"image", "video", "other"}:
                        media_type = "other"

                    prev = load_existing_file(conn, root["root_id"], rel_path)
                    file_id = None
                    prev_size = prev["size_bytes"] if prev else None
                    prev_mtime = prev["mtime_utc"] if prev else None
                    prev_sha = prev["sha256"] if prev else None

                    hash_needed = False
                    if args.hash_mode != "none" and not args.dry_run:
                        if args.hash_mode == "missing":
                            hash_needed = prev_sha is None
                        else:  # missing_or_changed
                            hash_needed = prev_sha is None or prev_size != size_bytes or prev_mtime != mtime_iso

                    now = utc_now_iso()

                    if args.dry_run:
                        if prev:
                            counts["updated"] += 1
                        else:
                            counts["inserted"] += 1
                        if hash_needed:
                            counts["hash_needed"] += 1
                        counts["files_seen"] += 1
                        continue

                    if prev:
                        conn.execute(
                            """
                            UPDATE files
                            SET size_bytes=?, mtime_utc=?, status='active', last_seen_run_id=?, updated_utc=?, media_type=?
                            WHERE file_id=?
                            """,
                            (size_bytes, mtime_iso, run_id, now, media_type, prev["file_id"]),
                        )
                        file_id = prev["file_id"]
                        counts["updated"] += 1
                    else:
                        cur = conn.execute(
                            """
                            INSERT INTO files(root_id, path, filename, ext, size_bytes, mtime_utc,
                                              status, created_utc, updated_utc, last_seen_run_id, media_type)
                            VALUES(?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)
                            """,
                            (
                                root["root_id"],
                                rel_path,
                                filename,
                                ext,
                                size_bytes,
                                mtime_iso,
                                now,
                                now,
                                run_id,
                                media_type,
                            ),
                        )
                        file_id = int(cur.lastrowid)
                        counts["inserted"] += 1

                    counts["files_seen"] += 1

                    if hash_needed and file_id is not None:
                        pending_hashes.append(
                            {
                                "file_id": file_id,
                                "path": full_path,
                                "size": size_bytes,
                                "prev_sha": prev_sha,
                                "media_type": media_type,
                                "role": role,
                            }
                        )
                except Exception as exc:  # noqa: BLE001
                    error_count += 1
                    # Log full path and error details to diagnose UNC vs mapped drive issues, relpath errors, or DB constraints.
                    logger.exception(
                        "Unexpected error processing file",
                        module="scan.scan_root",
                        file=filename,
                        full_path=str(full_path) if "full_path" in locals() else "",
                        root=root["name"],
                        base_path=str(base_path),
                        rel_path=rel_path if "rel_path" in locals() else "",
                        error_type=type(exc).__name__,
                        error=str(exc),
                        run_id=run_id,
                    )
                    if args.max_errors and error_count >= args.max_errors:
                        logger.error(
                            "Max errors reached; aborting scan",
                            module="scan.scan_root",
                            run_id=run_id,
                            max_errors=args.max_errors,
                            error_count=error_count,
                        )
                        raise
                finally:
                    if progress is not None:
                        progress.update(1)
    finally:
        if progress is not None:
            progress.close()

    if not args.dry_run:
        missing = mark_missing(conn, root["root_id"], run_id)
        counts["missing_marked"] += missing

        if args.hash_mode != "none":
            hash_progress = tqdm(
                total=len(pending_hashes),
                desc="Hashing files",
                unit="files",
                leave=False,
                disable=not _tqdm_enabled(),
            ) if _tqdm_enabled() else None
            try:
                if args.workers and args.workers > 0:
                    with ThreadPoolExecutor(max_workers=args.workers) as executor:
                        future_to_item = {executor.submit(hash_file, item["path"]): item for item in pending_hashes}
                        for future in as_completed(future_to_item):
                            item = future_to_item[future]
                            digest, err = future.result()
                            if err or digest is None:
                                counts["hash_errors"] += 1
                            else:
                                apply_hash_updates(conn, item, digest, run_id, counts)
                            if hash_progress is not None:
                                hash_progress.update(1)
                else:
                    for item in pending_hashes:
                        digest, err = hash_file(item["path"])
                        if err or digest is None:
                            counts["hash_errors"] += 1
                        else:
                            apply_hash_updates(conn, item, digest, run_id, counts)
                        if hash_progress is not None:
                            hash_progress.update(1)
            finally:
                if hash_progress is not None:
                    hash_progress.close()

        conn.commit()


def apply_hash_updates(
    conn: sqlite3.Connection,
    item: Dict[str, object],
    digest: str,
    run_id: int,
    counts: Dict[str, int],
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE files
        SET sha256=?, updated_utc=?, last_seen_run_id=?
        WHERE file_id=?
        """,
        (digest, now, run_id, item["file_id"]),
    )
    ensure_hash_group(conn, digest, run_id)
    ensure_membership(conn, digest, item["file_id"], item["role"])
    counts["hashed"] += 1


def main() -> None:
    args = parse_args()
    home_automation_common.create_logger("scan")
    logging.getLogger().setLevel(logging.DEBUG if args.verbose else logging.INFO)
    logger = structlog.get_logger().bind(module="scan.main")
    if not TQDM_AVAILABLE:
        logger.info("tqdm not installed; progress bars disabled")

    counts = {
        "roots_scanned": 0,
        "files_seen": 0,
        "inserted": 0,
        "updated": 0,
        "missing_marked": 0,
        "hashed": 0,
        "hash_errors": 0,
        "hash_needed": 0,
    }

    try:
        conn = open_db(Path(args.db))
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to open DB", error=str(exc))
        raise SystemExit(1)

    run_id: Optional[int] = None
    try:
        if args.dry_run:
            logger.info("Dry-run mode: no DB writes or hashing will occur.")
        else:
            cmdline = "scan.py"
            args_json = json.dumps(vars(args), default=str)
            run_id = begin_run(conn, cmdline, args_json)
            logger = logger.bind(run_id=run_id)
            logger.info("Run started")

        roots = fetch_roots(conn, args.root)
        if not roots:
            logger.info("No active roots to scan.")
            if run_id is not None and not args.dry_run:
                end_run_ok(conn, run_id, "no roots")
            return

        for root in roots:
            logger.info(
                "Scanning root",
                module="scan.main",
                root=root["name"],
                base_path=root["base_path"],
                run_id=run_id,
            )
            try:
                scan_root(conn, root, args, run_id, counts, logger)
                counts["roots_scanned"] += 1
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Error scanning root",
                    module="scan.main",
                    root=root["name"],
                    message=str(exc),
                    run_id=run_id,
                )
                if not args.dry_run:
                    conn.rollback()
                raise

        summary = {
            "roots_scanned": counts["roots_scanned"],
            "files_seen": counts["files_seen"],
            "inserted": counts["inserted"],
            "updated": counts["updated"],
            "missing_marked": counts["missing_marked"],
            "hashed": counts["hashed"],
            "hash_errors": counts["hash_errors"],
        }
        logger.info("Scan complete", **summary)

        if run_id is not None and not args.dry_run:
            end_run_ok(conn, run_id, json.dumps(summary))
    except Exception as exc:  # noqa: BLE001
        if run_id is not None and not args.dry_run:
            end_run_failed(conn, run_id, str(exc))
        logger.exception("Scan failed", module="scan.main", run_id=run_id)
        raise SystemExit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
