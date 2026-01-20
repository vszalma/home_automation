import argparse
import csv
import os
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path

import structlog
from PIL import Image, ExifTags
from tqdm import tqdm

import home_automation_common

DEFAULT_IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".heic", ".webp", ".bmp",
}
DEFAULT_VIDEO_EXTENSIONS = {
    ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".m4v", ".mpeg", ".mpg",
}


def _get_arguments():
    parser = argparse.ArgumentParser(
        description="Organize media files into year-based folders."
    )
    parser.add_argument("--source", "-s", type=str, required=True, help="Source directory or drive.")
    parser.add_argument("--destination-root", "-d", type=str, required=True, help="Destination root directory.")
    parser.add_argument(
        "--media-kind",
        choices=["images", "videos", "both"],
        default="both",
        help="Media kind to include. Defaults to both.",
    )
    parser.add_argument(
        "--types",
        type=str,
        required=False,
        help="Comma-separated list of extensions to override defaults (e.g. .jpg,.png,.mp4).",
    )
    parser.add_argument("--date-from", type=str, required=False, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--date-to", type=str, required=False, help="End date (YYYY-MM-DD).")
    parser.add_argument(
        "--mode",
        choices=["copy", "move", "report"],
        default="report",
        help="copy: copy files, move: move files, report: write CSV only.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the selected mode without copying or moving files; writes report CSV.",
    )
    parser.add_argument(
        "--report-csv",
        type=str,
        required=False,
        help="Path to report CSV. Defaults to destination-root/<YYYY-MM-DD>-organize-media-report.csv.",
    )
    parser.add_argument(
        "--video-date-source",
        choices=["filesystem", "ffprobe"],
        default="filesystem",
        help="Choose how to derive video date metadata. Defaults to filesystem.",
    )
    parser.add_argument(
        "--ffprobe-path",
        type=str,
        required=False,
        help="Path to ffprobe executable. If not provided, will attempt to locate via PATH.",
    )
    parser.add_argument(
        "--ffprobe-timeout",
        type=int,
        default=10,
        help="Timeout in seconds for ffprobe calls. Defaults to 10.",
    )
    return parser.parse_args()


def _parse_date(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


def _build_extension_sets(types_arg):
    if types_arg:
        extensions = {ext.strip().lower() for ext in types_arg.split(",") if ext.strip()}
        # Ensure extensions start with a dot.
        normalized = {ext if ext.startswith(".") else f".{ext}" for ext in extensions}
        return normalized, DEFAULT_IMAGE_EXTENSIONS, DEFAULT_VIDEO_EXTENSIONS
    return DEFAULT_IMAGE_EXTENSIONS | DEFAULT_VIDEO_EXTENSIONS, DEFAULT_IMAGE_EXTENSIONS, DEFAULT_VIDEO_EXTENSIONS


def _get_media_kind(extension, image_exts, video_exts):
    if extension in image_exts:
        return "image"
    if extension in video_exts:
        return "video"
    return "unknown"


def _date_in_range(value, date_from, date_to):
    if date_from and value < date_from:
        return False
    if date_to and value > date_to:
        return False
    return True


def _extract_exif_date(image_path):
    try:
        with Image.open(image_path) as img:
            exif = img._getexif()
            if not exif:
                return None, "exif missing"
            exif_data = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
            date_str = exif_data.get("DateTimeOriginal") or exif_data.get("DateTime")
            if not date_str:
                return None, "exif missing"
            # EXIF date format: "YYYY:MM:DD HH:MM:SS"
            return datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S"), ""
    except Exception:
        return None, "exif parse error"


def _get_filesystem_date(stat):
    if os.name == "nt":
        return datetime.fromtimestamp(stat.st_ctime)
    return datetime.fromtimestamp(stat.st_mtime)


def _get_effective_date(path, media_kind, stat):
    notes = []
    if media_kind == "image":
        exif_date, exif_note = _extract_exif_date(path)
        if exif_note:
            notes.append(exif_note)
        if exif_date:
            return exif_date, "exif", notes
    # For videos, or when EXIF is missing/invalid, fallback to filesystem timestamps.
    return _get_filesystem_date(stat), "filesystem", notes


def _sanitize_filename(name):
    # Reuse common sanitization for Windows-safe filenames.
    return home_automation_common.sanitize_filename(name)


def _build_destination_path(destination_root, effective_date, source_path):
    year_folder = str(effective_date.year)
    sanitized_name = _sanitize_filename(source_path.name)
    return Path(destination_root) / year_folder / sanitized_name


def _pre_scan_files(source, allowed_extensions, exclusions):
    source = Path(source)
    files = []
    for dirpath, dirnames, filenames in os.walk(source):
        dirnames[:] = [d for d in dirnames if d not in exclusions]
        for filename in filenames:
            extension = Path(filename).suffix.lower()
            if extension in allowed_extensions:
                files.append(Path(dirpath) / filename)
    return files


def _resolve_collision(base_path):
    resolved = home_automation_common.get_unique_destination_path(base_path)
    if resolved == base_path:
        return resolved, False, ""
    match = re.match(r"^.*\s\((\d+)\)$", resolved.stem)
    suffix = f"({match.group(1)})" if match else ""
    return resolved, True, suffix


def _resolve_ffprobe_path(explicit_path):
    if explicit_path:
        candidate = Path(explicit_path)
        if candidate.exists():
            return str(candidate)
        return None
    return shutil.which("ffprobe")


def main():
    args = _get_arguments()

    home_automation_common.create_logger("organize_media_by_date")
    logger = structlog.get_logger()

    date_from = _parse_date(args.date_from)
    date_to = _parse_date(args.date_to)

    allowed_extensions, image_exts, video_exts = _build_extension_sets(args.types)

    exclusions = home_automation_common.get_exclusion_list("collector")

    source = Path(args.source)
    destination_root = Path(args.destination_root)

    report_csv = Path(args.report_csv) if args.report_csv else None
    if not report_csv:
        today = datetime.now().date()
        report_csv = destination_root / f"{today}-organize-media-report.csv"

    start_time = datetime.now()

    ffprobe_path = _resolve_ffprobe_path(args.ffprobe_path)
    ffprobe_available = bool(ffprobe_path and Path(ffprobe_path).exists())

    logger.info(
        "ffprobe resolution",
        module="organize_media_by_date.main",
        requested_path=args.ffprobe_path or "",
        resolved_path=ffprobe_path or "",
        available=ffprobe_available,
        video_date_source=args.video_date_source,
        ffprobe_timeout=args.ffprobe_timeout,
    )

    logger.info(
        "Starting media organization.",
        module="organize_media_by_date.main",
        message=f"Source: {source} Destination: {destination_root} Mode: {args.mode}",
    )

    candidates = _pre_scan_files(source, allowed_extensions, exclusions)
    total_candidates = len(candidates)

    seen = 0
    processed = 0
    copied = 0
    moved = 0
    skipped_by_date = 0
    errors = 0
    warned_extensions = set()
    rows_since_flush = 0
    report_writer = None
    report_file = None
    report_fieldnames = [
        "schema_version",
        "run_id",
        "action",
        "media_kind",
        "source_path",
        "file_name",
        "file_extension",
        "file_size_bytes",
        "effective_datetime",
        "effective_year",
        "destination_root",
        "destination_path",
        "collision_resolved",
        "collision_suffix",
        "metadata_source",
        "date_filter_result",
        "status",
        "notes",
    ]
    report_file = open(report_csv, mode="w", newline="", encoding="utf-8")
    report_writer = csv.DictWriter(report_file, fieldnames=report_fieldnames)
    report_writer.writeheader()

    run_id = str(uuid.uuid4())
    action = args.mode

    try:
        for path in tqdm(candidates, total=total_candidates, desc="Processing media files"):
            seen += 1
            extension = path.suffix.lower()
            media_kind = _get_media_kind(extension, image_exts, video_exts)

            if args.types and media_kind == "unknown" and extension not in warned_extensions:
                warned_extensions.add(extension)
                logger.warning(
                    "Unknown extension in --types list.",
                    module="organize_media_by_date.main",
                    message=f"Extension {extension} is not in default image/video sets; treating as unknown.",
                )

            if args.media_kind == "images" and media_kind != "image":
                continue
            if args.media_kind == "videos" and media_kind != "video":
                continue
            if args.media_kind == "both" and media_kind == "unknown" and args.types:
                pass

            processed += 1

            try:
                stat = path.stat()
                effective_date, metadata_source, notes = _get_effective_date(path, media_kind, stat)
            except Exception as exc:
                errors += 1
                logger.error(
                    "Failed to determine effective date.",
                    module="organize_media_by_date.main",
                    message=str(exc),
                    file=str(path),
                )
                report_writer.writerow({
                    "schema_version": "1.0",
                    "run_id": run_id,
                    "action": action,
                    "media_kind": media_kind,
                    "source_path": str(path),
                    "file_name": path.name,
                    "file_extension": extension,
                    "file_size_bytes": "",
                    "effective_datetime": "",
                    "effective_year": "",
                    "destination_root": str(destination_root),
                    "destination_path": "",
                    "collision_resolved": "false",
                    "collision_suffix": "",
                    "metadata_source": "error",
                    "date_filter_result": "error",
                    "status": "error",
                    "notes": "effective date error",
                })
                rows_since_flush += 1
                if rows_since_flush >= 1000:
                    report_file.flush()
                    rows_since_flush = 0
                continue

            if date_from and effective_date < date_from:
                date_filter_result = "excluded_before"
            elif date_to and effective_date > date_to:
                date_filter_result = "excluded_after"
            else:
                date_filter_result = "included"

            if date_filter_result != "included":
                skipped_by_date += 1

            destination_path = _build_destination_path(destination_root, effective_date, path)
            destination_path, collision_resolved, collision_suffix = _resolve_collision(destination_path)

            if collision_resolved:
                notes.append("collision applied")

            file_size = stat.st_size
            status = "skipped" if date_filter_result != "included" else "ok"
            if args.dry_run and status == "ok":
                notes.append("dry run")

            report_writer.writerow({
                "schema_version": "1.0",
                "run_id": run_id,
                "action": action,
                "media_kind": media_kind,
                "source_path": str(path),
                "file_name": path.name,
                "file_extension": extension,
                "file_size_bytes": file_size,
                "effective_datetime": effective_date.isoformat(),
                "effective_year": str(effective_date.year),
                "destination_root": str(destination_root),
                "destination_path": str(destination_path),
                "collision_resolved": str(collision_resolved).lower(),
                "collision_suffix": collision_suffix,
                "metadata_source": metadata_source,
                "date_filter_result": date_filter_result,
                "status": status,
                "notes": "; ".join(notes),
            })
            rows_since_flush += 1
            if rows_since_flush >= 1000:
                report_file.flush()
                rows_since_flush = 0

            if date_filter_result != "included":
                continue

            if args.dry_run:
                logger.info(
                    "Dry run action.",
                    module="organize_media_by_date.main",
                    message=f"Would {action} {path} to {destination_path}.",
                )
                continue

            if args.mode == "copy":
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, destination_path)
                copied += 1
            elif args.mode == "move":
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(path, destination_path)
                moved += 1
    finally:
        if report_file:
            report_file.close()

    end_time = datetime.now()
    duration = end_time - start_time

    logger.info(
        "Media organization completed.",
        module="organize_media_by_date.main",
        message="Process completed.",
        found_candidates=total_candidates,
        processed=processed,
        copied=copied,
        moved=moved,
        skipped_by_date=skipped_by_date,
        errors=errors,
        report_path=str(report_csv) if report_csv else "",
        duration=str(duration),
    )


if __name__ == "__main__":
    main()
