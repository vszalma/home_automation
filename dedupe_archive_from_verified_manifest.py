import argparse
import csv
import json
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import structlog
from tqdm import tqdm

import home_automation_common

HASH_PENALTY_DUP_SUFFIX = 20
HASH_PENALTY_CAMERA = 10
HASH_BONUS_SPACES = 5
HASH_BONUS_LETTERS_CAP = 30
HASH_BONUS_LENGTH_CAP = 30


def _get_arguments():
    parser = argparse.ArgumentParser(
        description="Dedupe archive files based on a verified manifest (hash-based, scope by year or global)."
    )
    parser.add_argument("--manifest", required=True, help="Path to verification CSV.")
    parser.add_argument("--archive-root", required=True, help="Archive root used to parse year segments.")
    parser.add_argument("--quarantine-root", required=True, help="Root folder where duplicates are quarantined.")
    parser.add_argument("--keep-out", required=True, help="CSV to record canonical keep decisions.")
    parser.add_argument("--dupes-out", required=True, help="CSV to record duplicate actions.")
    parser.add_argument("--expected-run-id", required=True, help="Expected run_id or 'auto' to infer.")
    parser.add_argument(
        "--scope",
        choices=["year", "global"],
        default="year",
        help="Dedupe within each year folder (default) or across the entire archive.",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Number of duplicate files to act on per run.")
    parser.add_argument("--state-file", required=False, help="Path to state file for resumable runs.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate actions without moving files.")
    return parser.parse_args()


def _load_state(state_path):
    if not state_path:
        return 0
    path = Path(state_path)
    if not path.exists():
        return 0
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return int(data.get("cursor", 0))
    except Exception:
        return 0


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


def _parse_year_from_path(destination_path: Path, archive_root: Path):
    """
    Extract the year folder immediately under the archive root.
    Example: archive_root=D:\\MediaArchive, destination=D:\\MediaArchive\\2016\\file.jpg -> year=2016.
    """
    try:
        rel = destination_path.resolve(strict=False).relative_to(archive_root.resolve(strict=False))
    except Exception:
        return None, "destination not under archive_root"

    if len(rel.parts) == 0:
        return None, "destination has no relative parts"

    year_candidate = rel.parts[0]
    if re.fullmatch(r"\d{4}", year_candidate):
        return year_candidate, ""
    return None, "year segment missing or invalid"


def _has_duplicate_suffix(name: str):
    return bool(re.search(r"\s\(\d+\)$", name))


def _is_camera_style(name: str):
    patterns = [
        r"IMG_\d+",
        r"DSC_\d+",
        r"PXL_\d+",
        r"VID_\d+",
        r"MOV_\d+",
        r"DCIM\d*",
        r"DSCN\d+",
    ]
    return any(re.fullmatch(pat, name, flags=re.IGNORECASE) for pat in patterns)


def _score_destination(path: Path):
    """
    Heuristic scoring to choose a canonical keep:
      - Penalize Windows-style duplicate suffixes like " (2)".
      - Penalize common camera-style base names.
      - Reward names with letters/spaces and longer descriptive text.
      - Tie-break lexicographically later.
    """
    base = path.stem
    score = 0

    if _has_duplicate_suffix(base):
        score -= HASH_PENALTY_DUP_SUFFIX

    if _is_camera_style(base):
        score -= HASH_PENALTY_CAMERA

    letters = sum(1 for c in base if c.isalpha())
    score += min(letters, HASH_BONUS_LETTERS_CAP)

    if " " in base:
        score += HASH_BONUS_SPACES

    score += min(len(base), HASH_BONUS_LENGTH_CAP)

    return score


def _choose_canonical(entries):
    best = None
    best_score = None
    for entry in entries:
        score = _score_destination(entry["dest_path"])
        if best is None or score > best_score:
            best = entry
            best_score = score
            continue
        if score == best_score:
            if str(entry["dest_path"]) < str(best["dest_path"]):
                best = entry
                best_score = score
    keep_reason = f"score={best_score}"
    if _has_duplicate_suffix(best["dest_path"].stem):
        keep_reason += "; no-dup-suffix preferred"
    if _is_camera_style(best["dest_path"].stem):
        keep_reason += "; camera-style penalty applied"
    return best, keep_reason


def _build_groups(args, logger):
    archive_root = Path(args.archive_root)
    groups = {}
    mismatches = []
    inferred_run_id = None
    verified_rows_read = 0
    destination_files_considered = 0
    bytes_considered_total = 0
    with open(args.manifest, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for idx, row in enumerate(reader):
            run_id = row.get("run_id")
            if not run_id:
                mismatches.append((idx, "missing run_id"))
                continue

            if args.expected_run_id == "auto":
                if inferred_run_id is None:
                    inferred_run_id = run_id
                expected = inferred_run_id
            else:
                expected = args.expected_run_id
            if run_id != expected:
                mismatches.append((idx, f"expected {expected}, found {run_id}"))
                continue

            if row.get("verification_status") != "verified":
                continue

            hash_value = row.get("destination_hash") or row.get("source_hash")
            if not hash_value:
                logger.warning(
                    "Skipping row with empty hash.",
                    module="dedupe_archive_from_verified_manifest",
                    row_index=idx,
                )
                continue

            dest_path = Path(row.get("destination_path", ""))
            year_val = ""
            if args.scope == "year":
                year_val, year_note = _parse_year_from_path(dest_path, archive_root)
                if year_val is None:
                    logger.warning(
                        "Skipping row outside archive root or with invalid year.",
                        module="dedupe_archive_from_verified_manifest",
                        row_index=idx,
                        note=year_note,
                    )
                    continue
            else:
                # Global scope: still attempt to parse year for logging/quarantine placement; fallback to empty if invalid.
                yr, _ = _parse_year_from_path(dest_path, archive_root)
                year_val = yr or ""

            try:
                size_int = int(row.get("file_size_bytes", 0))
            except Exception:
                size_int = 0

            destination_files_considered += 1
            bytes_considered_total += size_int

            key = (year_val if args.scope == "year" else "global", hash_value)
            groups.setdefault(key, []).append(
                {
                    "run_id": run_id,
                    "dest_path": dest_path,
                    "hash": hash_value,
                    "year": year_val,
                    "size": size_int,
                }
            )
            verified_rows_read += 1
    return (
        groups,
        mismatches,
        (inferred_run_id if args.expected_run_id == "auto" else args.expected_run_id),
        verified_rows_read,
        destination_files_considered,
        bytes_considered_total,
    )


def _compute_actions(groups, scope):
    actions = []
    keep_records = {}
    duplicate_groups_found = 0
    duplicate_files_identified = 0

    for key in sorted(groups.keys()):
        entries = groups[key]
        if len(entries) <= 1:
            continue
        duplicate_groups_found += 1
        best, keep_reason = _choose_canonical(entries)
        keep_records[key] = {
            "run_id": best["run_id"],
            "scope": scope,
            "year": best["year"],
            "hash": best["hash"],
            "kept_destination_path": str(best["dest_path"]),
            "keep_reason": keep_reason,
        }
        for entry in sorted(entries, key=lambda e: str(e["dest_path"])):
            if entry is best:
                continue
            duplicate_files_identified += 1
            actions.append(
                {
                    "entry": entry,
                    "key": key,
                    "scope": scope,
                    "year": entry["year"],
                    "hash": entry["hash"],
                    "keep_path": str(best["dest_path"]),
                    "keep_reason": keep_reason,
                }
            )
    return actions, keep_records, duplicate_groups_found, duplicate_files_identified


def _quarantine_destination(quarantine_root: Path, entry):
    year_folder = entry["year"] or "unknown"
    target = quarantine_root / year_folder / entry["dest_path"].name
    return home_automation_common.get_unique_destination_path(target)


def main():
    args = _get_arguments()

    home_automation_common.create_logger("dedupe_archive_from_verified_manifest")
    logger = structlog.get_logger()

    state_cursor = _load_state(args.state_file)
    append_mode = bool(args.state_file)

    (
        groups,
        mismatches,
        expected_run_id,
        verified_rows_read,
        destination_files_considered,
        bytes_considered_total,
    ) = _build_groups(args, logger)

    if mismatches:
        for idx, reason in mismatches:
            logger.error(
                "Run ID validation failed.",
                module="dedupe_archive_from_verified_manifest",
                row_index=idx,
                reason=reason,
            )
        raise SystemExit("Run ID validation failed; aborting without changes.")

    logger.info(
        "Run ID validated.",
        module="dedupe_archive_from_verified_manifest",
        expected_run_id=expected_run_id or "",
        verified_rows=verified_rows_read,
    )

    actions, keep_records, duplicate_groups_found, duplicate_files_identified = _compute_actions(groups, args.scope)

    keep_fields = ["run_id", "scope", "year", "hash", "kept_destination_path", "keep_reason"]
    dupes_fields = ["run_id", "scope", "year", "hash", "duplicate_destination_path", "quarantine_path", "action_taken", "notes"]

    keep_file, keep_writer = _ensure_writer(args.keep_out, keep_fields, append_mode)
    dupes_file, dupes_writer = _ensure_writer(args.dupes_out, dupes_fields, append_mode)

    # Seed keep-written set with keys already processed in prior runs (if cursor > 0).
    keep_written_for_key = set()
    for idx in range(min(state_cursor, len(actions))):
        keep_written_for_key.add(actions[idx]["key"])

    processed_duplicates = 0
    duplicates_quarantined = 0
    bytes_quarantined = 0
    errors = 0
    next_cursor = state_cursor
    start_time = datetime.now()

    quarantine_root = Path(args.quarantine_root)

    try:
        progress = tqdm(total=args.limit, desc="Quarantining duplicates", unit="file")
        for idx, action in enumerate(actions):
            if idx < state_cursor:
                continue
            if processed_duplicates >= args.limit:
                break

            key = action["key"]
            entry = action["entry"]

            if key not in keep_written_for_key:
                keep_row = keep_records[key]
                keep_writer.writerow(keep_row)
                keep_written_for_key.add(key)

            duplicate_path = Path(entry["dest_path"])
            quarantine_path = str(_quarantine_destination(quarantine_root, entry))
            notes = []
            action_taken = "quarantine"

            if not duplicate_path.exists():
                action_taken = "missing"
                notes.append("duplicate missing on disk")
                errors += 1
            else:
                if args.dry_run:
                    action_taken = "dry-run-quarantine"
                else:
                    try:
                        Path(quarantine_path).parent.mkdir(parents=True, exist_ok=True)
                        quarantine_final = home_automation_common.get_unique_destination_path(Path(quarantine_path))
                        shutil.move(str(duplicate_path), str(quarantine_final))
                        quarantine_path = str(quarantine_final)
                    except Exception as exc:
                        action_taken = "error"
                        notes.append(f"move failed: {exc}")
                        errors += 1
            if action_taken in {"quarantine", "dry-run-quarantine"}:
                duplicates_quarantined += 1
                if action_taken == "quarantine":
                    try:
                        bytes_quarantined += int(entry.get("size", 0))
                    except Exception:
                        pass

            dupes_writer.writerow(
                {
                    "run_id": entry["run_id"],
                    "scope": action["scope"],
                    "year": action["year"],
                    "hash": action["hash"],
                    "duplicate_destination_path": str(duplicate_path),
                    "quarantine_path": quarantine_path,
                    "action_taken": action_taken,
                    "notes": "; ".join(notes),
                }
            )

            processed_duplicates += 1
            next_cursor = idx + 1
            progress.update(1)
        progress.close()
    finally:
        keep_file.close()
        dupes_file.close()

    _save_state(args.state_file, next_cursor)

    duration = datetime.now() - start_time
    logger.info(
        "Deduplication completed.",
        module="dedupe_archive_from_verified_manifest",
        verified_rows_read=verified_rows_read,
        duplicate_groups_found=duplicate_groups_found,
        duplicate_files_identified=duplicate_files_identified,
        duplicates_quarantined=duplicates_quarantined,
        bytes_quarantined=bytes_quarantined,
        errors=errors,
        duration=str(duration),
        next_cursor=next_cursor,
        state_file=args.state_file or "",
        expected_run_id=expected_run_id or "",
        destination_files_considered=destination_files_considered,
        bytes_considered_total=bytes_considered_total,
    )


if __name__ == "__main__":
    main()
