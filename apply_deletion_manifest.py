import argparse
import csv
import json
import os
import shutil
from pathlib import Path
from datetime import datetime

import structlog
from tqdm import tqdm

import home_automation_common

READ_CHUNK_SIZE = 1024 * 1024


def _get_arguments():
    parser = argparse.ArgumentParser(
        description="Apply deletions or quarantines based on a verified manifest CSV."
    )
    parser.add_argument("--manifest", required=True, help="Path to verified manifest CSV.")
    parser.add_argument("--quarantine-root", required=True, help="Root directory for quarantined files.")
    parser.add_argument("--limit", type=int, default=500, help="Maximum rows to process in this run.")
    parser.add_argument("--offset", type=int, default=0, help="Rows to skip before processing.")
    parser.add_argument("--state-file", required=False, help="Path to cursor state file for resumable runs.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate actions without moving or deleting files.")
    parser.add_argument(
        "--delete-permanently",
        action="store_true",
        help="If set, files will be permanently deleted instead of quarantined.",
    )
    parser.add_argument(
        "--yes-really-delete",
        action="store_true",
        help="Required when using --delete-permanently to confirm destructive action.",
    )
    parser.add_argument(
        "--expected-run-id",
        required=True,
        help="Expected run_id value. Use 'auto' to infer from the first processed row.",
    )
    parser.add_argument(
        "--results-out",
        required=True,
        help="Path to deletion results CSV (appended if state is used).",
    )
    return parser.parse_args()


def _load_state(state_path):
    if not state_path:
        return None
    path = Path(state_path)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return int(data.get("cursor", 0))
    except Exception:
        return None


def _save_state(state_path, cursor):
    if not state_path:
        return
    try:
        with Path(state_path).open("w", encoding="utf-8") as f:
            json.dump({"cursor": cursor}, f)
    except Exception:
        pass


def _ensure_writer(path, fieldnames, append):
    file_exists = Path(path).exists()
    mode = "a" if append else "w"
    f = open(path, mode, newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if not append or not file_exists or Path(path).stat().st_size == 0:
        writer.writeheader()
    return f, writer


def _relative_to_root(path: Path):
    if path.drive:
        try:
            return path.relative_to(path.anchor)
        except ValueError:
            return path.name
    return path


def _prevalidate_run_ids(manifest_path, expected_run_id, start_offset, limit):
    """Scan applicable rows to validate run_id before performing operations."""
    first_run_id = None
    mismatches = []
    with open(manifest_path, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for idx, row in enumerate(reader):
            if idx < start_offset:
                continue
            if idx >= start_offset + limit:
                break
            run_id = row.get("run_id")
            if not run_id:
                mismatches.append((idx, "missing run_id"))
                continue
            if first_run_id is None:
                first_run_id = run_id
            if expected_run_id != "auto" and run_id != expected_run_id:
                mismatches.append((idx, f"expected {expected_run_id}, found {run_id}"))
    inferred = first_run_id if expected_run_id == "auto" else expected_run_id
    return inferred, mismatches


def _process_row(row, args, logger, quarantine_root):
    action = "skipped"
    notes = []
    destination_quarantine_path = ""

    if row.get("verification_status") != "verified":
        notes.append("not verified")
        return action, destination_quarantine_path, notes

    source_path = Path(row.get("source_path", ""))

    if not source_path.exists():
        notes.append("source missing")
        action = "error"
        return action, destination_quarantine_path, notes

    if args.delete_permanently:
        action = "delete"
        if args.dry_run:
            notes.append("dry run: delete")
            return action, destination_quarantine_path, notes
        try:
            source_path.unlink()
        except Exception as exc:
            action = "error"
            notes.append(f"delete failed: {exc}")
        return action, destination_quarantine_path, notes

    # Quarantine move
    action = "quarantine"
    rel_path = _relative_to_root(source_path)
    destination_quarantine_path = Path(quarantine_root) / rel_path
    destination_quarantine_path = home_automation_common.get_unique_destination_path(destination_quarantine_path)

    if args.dry_run:
        notes.append("dry run: quarantine")
        return action, str(destination_quarantine_path), notes

    try:
        destination_quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_path), str(destination_quarantine_path))
    except Exception as exc:
        action = "error"
        notes.append(f"quarantine failed: {exc}")
    return action, str(destination_quarantine_path), notes


def main():
    args = _get_arguments()

    if args.delete_permanently and not args.yes_really_delete:
        raise SystemExit("Refusing to delete permanently without --yes-really-delete.")

    home_automation_common.create_logger("apply_deletion_manifest")
    logger = structlog.get_logger()

    state_cursor = _load_state(args.state_file)
    start_offset = max(args.offset, state_cursor or 0)
    append_mode = bool(args.state_file)

    expected_run_id, mismatches = _prevalidate_run_ids(
        args.manifest, args.expected_run_id, start_offset, args.limit
    )
    if mismatches:
        for idx, reason in mismatches:
            logger.error(
                "Run ID validation failed.",
                module="apply_deletion_manifest.prevalidate",
                row_index=idx,
                reason=reason,
            )
        raise SystemExit("Run ID validation failed; aborting without changes.")

    logger.info(
        "Starting deletion/quarantine run.",
        module="apply_deletion_manifest.main",
        expected_run_id=expected_run_id or "",
        manifest=args.manifest,
        quarantine_root=args.quarantine_root,
        limit=args.limit,
        offset=start_offset,
        delete_permanently=args.delete_permanently,
        dry_run=args.dry_run,
    )

    fieldnames = [
        "run_id",
        "source_path",
        "action_taken",
        "destination_quarantine_path",
        "notes",
    ]

    results_file, results_writer = _ensure_writer(args.results_out, fieldnames, append_mode)

    processed = 0
    quarantined = 0
    deleted = 0
    skipped = 0
    errors = 0
    next_cursor = start_offset
    start_time = datetime.now()

    try:
        with open(args.manifest, "r", encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile)
            progress = tqdm(total=args.limit, desc="Applying deletion manifest", unit="file")
            for idx, row in enumerate(reader):
                if idx < start_offset:
                    continue
                if processed >= args.limit:
                    break

                action, destination_quarantine_path, notes = _process_row(
                    row, args, logger, args.quarantine_root
                )

                if action == "quarantine":
                    quarantined += 1
                elif action == "delete":
                    deleted += 1
                elif action == "error":
                    errors += 1
                else:
                    skipped += 1

                results_writer.writerow({
                    "run_id": row.get("run_id", ""),
                    "source_path": row.get("source_path", ""),
                    "action_taken": action,
                    "destination_quarantine_path": destination_quarantine_path,
                    "notes": "; ".join(notes),
                })

                processed += 1
                next_cursor = idx + 1
                progress.update(1)
            progress.close()
    finally:
        results_file.close()

    _save_state(args.state_file, next_cursor)

    duration = datetime.now() - start_time
    logger.info(
        "Deletion/quarantine run completed.",
        module="apply_deletion_manifest.main",
        processed=processed,
        quarantined=quarantined,
        deleted=deleted,
        skipped=skipped,
        errors=errors,
        duration=str(duration),
        next_cursor=next_cursor,
        state_file=args.state_file or "",
        expected_run_id=expected_run_id or "",
    )


if __name__ == "__main__":
    main()
