import argparse
import csv
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

import structlog
from tqdm import tqdm

import home_automation_common

DEFAULT_HASH = "sha256"
HASH_CHOICES = ["sha256"]  # Reserved for future expansion (e.g., blake3)
READ_CHUNK_SIZE = 1024 * 1024  # 1 MiB


def _get_arguments():
    parser = argparse.ArgumentParser(
        description="Verify that media files were copied correctly from source to destination."
    )
    parser.add_argument("--input-csv", required=True, help="Path to the organize_media_by_date output CSV.")
    parser.add_argument("--verified-out", required=True, help="CSV file to append verified rows.")
    parser.add_argument("--unverified-out", required=True, help="CSV file to append unverified rows.")
    parser.add_argument(
        "--hash",
        choices=HASH_CHOICES,
        default=DEFAULT_HASH,
        help="Hash algorithm to use for verification. Defaults to sha256.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of rows to process in this run. Defaults to 1000.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Number of rows to skip before processing. Defaults to 0.",
    )
    parser.add_argument(
        "--state-file",
        type=str,
        required=False,
        help="Path to state file for resumable processing. When provided, outputs are appended and only new rows are processed.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, min(4, os.cpu_count() or 1)),
        help="Number of workers (currently processed sequentially). Defaults to min(4, cpu_count).",
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
    path = Path(state_path)
    try:
        with path.open("w", encoding="utf-8") as f:
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


def _hash_file(path: Path, hash_name: str):
    try:
        h = hashlib.new(hash_name)
    except Exception:
        return None, f"unsupported hash: {hash_name}"
    try:
        with path.open("rb") as f:
            while True:
                chunk = f.read(READ_CHUNK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest(), ""
    except Exception as exc:
        return None, str(exc)


def _verify_row(row, hash_name, logger):
    source_path = Path(row.get("source_path", ""))
    destination_path = Path(row.get("destination_path", ""))
    file_size_bytes = row.get("file_size_bytes", "")
    run_id = row.get("run_id", "")

    notes = []
    status = "unverified"
    source_hash = ""
    destination_hash = ""

    if not source_path.exists():
        notes.append("source missing")
        return status, notes, source_hash, destination_hash, run_id, file_size_bytes

    if not destination_path.exists():
        notes.append("destination missing")
        return status, notes, source_hash, destination_hash, run_id, file_size_bytes

    try:
        source_size = source_path.stat().st_size
        dest_size = destination_path.stat().st_size
    except Exception as exc:
        notes.append(f"stat error: {exc}")
        return status, notes, source_hash, destination_hash, run_id, file_size_bytes

    if str(file_size_bytes).isdigit():
        expected_size = int(file_size_bytes)
        if expected_size != source_size:
            notes.append("source size mismatch vs csv")
        if expected_size != dest_size:
            notes.append("destination size mismatch vs csv")

    if source_size != dest_size:
        notes.append("size mismatch")
        return status, notes, source_hash, destination_hash, run_id, file_size_bytes

    source_hash, hash_note = _hash_file(source_path, hash_name)
    if hash_note:
        notes.append(f"source hash error: {hash_note}")
        return status, notes, source_hash or "", destination_hash, run_id, file_size_bytes

    destination_hash, hash_note = _hash_file(destination_path, hash_name)
    if hash_note:
        notes.append(f"destination hash error: {hash_note}")
        return status, notes, source_hash, destination_hash or "", run_id, file_size_bytes

    if source_hash != destination_hash:
        notes.append("hash mismatch")
        return status, notes, source_hash, destination_hash, run_id, file_size_bytes

    status = "verified"
    return status, notes, source_hash, destination_hash, run_id, file_size_bytes


def main():
    args = _get_arguments()

    home_automation_common.create_logger("verify_media_archive")
    logger = structlog.get_logger()

    start_time = datetime.now()

    state_cursor = _load_state(args.state_file)
    start_offset = max(args.offset, state_cursor or 0)
    append_mode = bool(args.state_file)

    fieldnames = [
        "run_id",
        "source_path",
        "destination_path",
        "file_size_bytes",
        "source_hash",
        "destination_hash",
        "verification_status",
        "notes",
    ]

    verified_file, verified_writer = _ensure_writer(args.verified_out, fieldnames, append_mode)
    unverified_file, unverified_writer = _ensure_writer(args.unverified_out, fieldnames, append_mode)

    processed = 0
    verified_count = 0
    unverified_count = 0
    reason_counts = {}
    next_cursor = start_offset
    rows_missing_size = 0
    bytes_processed_total = 0
    bytes_verified_total = 0
    bytes_unverified_total = 0
    bytes_hashed_total = 0

    try:
        with open(args.input_csv, "r", encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile)
            progress = tqdm(total=args.limit, desc="Verifying media files", unit="file")
            for idx, row in enumerate(reader):
                if idx < start_offset:
                    continue
                if processed >= args.limit:
                    break

                file_size_raw = row.get("file_size_bytes", "")
                try:
                    size_int = int(file_size_raw)
                except Exception:
                    size_int = 0
                    rows_missing_size += 1

                bytes_processed_total += size_int

                status, notes, source_hash, destination_hash, run_id, file_size_bytes = _verify_row(
                    row, args.hash, logger
                )

                output_row = {
                    "run_id": run_id,
                    "source_path": row.get("source_path", ""),
                    "destination_path": row.get("destination_path", ""),
                    "file_size_bytes": file_size_bytes,
                    "source_hash": source_hash,
                    "destination_hash": destination_hash,
                    "verification_status": status,
                    "notes": "; ".join(notes),
                }

                if status == "verified":
                    verified_writer.writerow(output_row)
                    verified_count += 1
                    bytes_verified_total += size_int
                    bytes_hashed_total += size_int * 2  # hashed source and destination
                else:
                    unverified_writer.writerow(output_row)
                    unverified_count += 1
                    bytes_unverified_total += size_int
                    for note in notes:
                        reason_counts[note] = reason_counts.get(note, 0) + 1

                processed += 1
                next_cursor = idx + 1
                progress.update(1)
            progress.close()
    finally:
        verified_file.close()
        unverified_file.close()

    _save_state(args.state_file, next_cursor)

    duration = datetime.now() - start_time
    logger.info(
        "Verification completed.",
        module="verify_media_archive.main",
        processed=processed,
        verified=verified_count,
        unverified=unverified_count,
        unverified_reasons=reason_counts,
        duration=str(duration),
        next_cursor=next_cursor,
        state_file=args.state_file or "",
        rows_missing_size=rows_missing_size,
        bytes_processed_total=bytes_processed_total,
        bytes_verified_total=bytes_verified_total,
        bytes_unverified_total=bytes_unverified_total,
        bytes_hashed_total=bytes_hashed_total,
    )


if __name__ == "__main__":
    main()
