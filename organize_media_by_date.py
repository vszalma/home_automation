import argparse
import csv
import json
import os
import re
import shutil
import subprocess
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
    parser.add_argument(
        "--set-destination-created-time",
        action="store_true",
        help="On Windows, set destination file creation time to the effective date after copy/move.",
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
    # Prefer modified time (mtime) on all platforms for consistency.
    return datetime.fromtimestamp(stat.st_mtime)


def _get_effective_date(
    path,
    media_kind,
    extension,
    stat,
    args,
    ffprobe_info,
    warn_state,
    logger,
):
    notes = []
    if media_kind == "image":
        exif_date, exif_note = _extract_exif_date(path)
        if exif_note:
            notes.append(exif_note)
        if exif_date:
            return exif_date, "exif", notes

    if media_kind == "video":
        if args.video_date_source == "ffprobe":
            ffprobe_allowed_exts = {".mp4", ".mov", ".m4v", ".avi"}
            if extension.lower() not in ffprobe_allowed_exts:
                logger.debug(
                    "Skipping ffprobe for unsupported video extension.",
                    module="organize_media_by_date._get_effective_date",
                    extension=extension,
                )
                return _get_filesystem_date(stat), "filesystem_mtime", notes
            if ffprobe_info["available"]:
                ffprobe_date, ffprobe_note = _extract_video_creation_time_ffprobe(
                    path, ffprobe_info["path"], ffprobe_info["timeout"]
                )
                if ffprobe_date:
                    return ffprobe_date, "ffprobe", notes
                if ffprobe_note:
                    notes.append(ffprobe_note)
            else:
                if not warn_state["ffprobe_warned"]:
                    logger.warning(
                        "ffprobe not available; falling back to filesystem dates for videos.",
                        module="organize_media_by_date._get_effective_date",
                    )
                    warn_state["ffprobe_warned"] = True
        return _get_filesystem_date(stat), "filesystem_mtime", notes

    # Default fallback for other media kinds or when EXIF is missing/invalid.
    return _get_filesystem_date(stat), "filesystem_mtime", notes


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


def _parse_ffprobe_datetime(value):
    try:
        if not value:
            return None
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo:
            return dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _extract_video_creation_time_ffprobe(path: Path, ffprobe_path: str, timeout: int):
    try:
        result = subprocess.run(
            [
                ffprobe_path,
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_entries",
                "format_tags=creation_time:stream_tags=creation_time",
                "-i",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except Exception as exc:
        return None, f"ffprobe error: {exc}"

    if result.returncode != 0:
        return None, f"ffprobe exited with {result.returncode}"

    try:
        data = json.loads(result.stdout or "{}")
    except Exception as exc:
        return None, f"ffprobe json parse error: {exc}"

    # Prefer format-level creation_time.
    try:
        format_tags = data.get("format", {}).get("tags", {})
        creation_time = format_tags.get("creation_time")
        if creation_time:
            parsed = _parse_ffprobe_datetime(creation_time)
            if parsed:
                return parsed, ""
    except Exception:
        pass

    # Fall back to first stream with creation_time.
    try:
        for stream in data.get("streams", []):
            creation_time = stream.get("tags", {}).get("creation_time")
            if creation_time:
                parsed = _parse_ffprobe_datetime(creation_time)
                if parsed:
                    return parsed, ""
    except Exception:
        pass

    return None, "creation_time not found"


def _datetime_to_filetime(dt: datetime):
    # FILETIME is 100-ns intervals since January 1, 1601 (UTC).
    # Convert naive local time to UTC before computing FILETIME.
    if dt.tzinfo is None:
        dt = dt.astimezone().replace(tzinfo=None)
    epoch_as_filetime = 116444736000000000  # difference between 1601 and 1970 in 100ns units
    hundreds_of_ns = int(dt.timestamp() * 10_000_000)
    return epoch_as_filetime + hundreds_of_ns


def set_windows_creation_time(path: Path, dt: datetime):
    if os.name != "nt":
        return
    try:
        import ctypes
        import ctypes.wintypes as wt
    except Exception:
        return

    GENERIC_WRITE = 0x40000000
    OPEN_EXISTING = 3
    FILE_FLAG_BACKUP_SEMANTICS = 0x02000000

    CreateFileW = ctypes.windll.kernel32.CreateFileW
    SetFileTime = ctypes.windll.kernel32.SetFileTime
    CloseHandle = ctypes.windll.kernel32.CloseHandle

    CreateFileW.argtypes = [
        wt.LPCWSTR,
        wt.DWORD,
        wt.DWORD,
        wt.LPVOID,
        wt.DWORD,
        wt.DWORD,
        wt.HANDLE,
    ]
    CreateFileW.restype = wt.HANDLE

    SetFileTime.argtypes = [wt.HANDLE, wt.LPFILETIME, wt.LPFILETIME, wt.LPFILETIME]
    SetFileTime.restype = wt.BOOL

    handle = CreateFileW(
        str(path),
        GENERIC_WRITE,
        0,
        None,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        None,
    )
    if handle == wt.HANDLE(-1).value:
        return

    try:
        ft = _datetime_to_filetime(dt)
        c_time = wt.FILETIME(ft & 0xFFFFFFFF, ft >> 32)
        if not SetFileTime(handle, ctypes.byref(c_time), None, None):
            return
    finally:
        CloseHandle(handle)


def _maybe_set_dest_created_time(destination_path, effective_date, args, notes, logger):
    if not args.set_destination_created_time:
        return
    try:
        set_windows_creation_time(destination_path, effective_date)
        notes.append("dest_created_time_set")
    except Exception as exc:
        notes.append(f"dest_created_time_failed:{exc.__class__.__name__}")
        logger.warning(
            "Failed to set destination creation time.",
            module="organize_media_by_date.main",
            file=str(destination_path),
            error=str(exc),
        )


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
    ffprobe_info = {
        "path": ffprobe_path,
        "available": ffprobe_available,
        "timeout": args.ffprobe_timeout,
    }
    warn_state = {"ffprobe_warned": False}

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
    bytes_candidates_total = 0
    bytes_processed_total = 0
    bytes_copied_total = 0
    bytes_moved_total = 0
    bytes_skipped_by_date_total = 0
    bytes_error_total = 0
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
                bytes_candidates_total += stat.st_size
                bytes_processed_total += stat.st_size
                effective_date, metadata_source, notes = _get_effective_date(
                    path,
                    media_kind,
                    extension,
                    stat,
                    args,
                    ffprobe_info,
                    warn_state,
                    logger,
                )
            except Exception as exc:
                errors += 1
                try:
                    if "stat" in locals():
                        bytes_error_total += stat.st_size
                except Exception:
                    pass
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
                try:
                    bytes_skipped_by_date_total += stat.st_size
                except Exception:
                    pass

            destination_path = _build_destination_path(destination_root, effective_date, path)
            destination_path, collision_resolved, collision_suffix = _resolve_collision(destination_path)

            if collision_resolved:
                notes.append("collision applied")

            file_size = stat.st_size
            status = "skipped" if date_filter_result != "included" else "ok"
            if args.dry_run and status == "ok":
                notes.append("dry run")

            row_data = {
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
                "notes": "",  # filled later
            }

            if date_filter_result != "included":
                row_data["notes"] = "; ".join(notes)
                report_writer.writerow(row_data)
                rows_since_flush += 1
                if rows_since_flush >= 1000:
                    report_file.flush()
                    rows_since_flush = 0
                continue

            if args.dry_run:
                logger.info(
                    "Dry run action.",
                    module="organize_media_by_date.main",
                    message=f"Would {action} {path} to {destination_path}.",
                )
                row_data["notes"] = "; ".join(notes)
                report_writer.writerow(row_data)
                rows_since_flush += 1
                if rows_since_flush >= 1000:
                    report_file.flush()
                    rows_since_flush = 0
                continue

            if args.mode == "copy":
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, destination_path)
                copied += 1
                bytes_copied_total += file_size
                _maybe_set_dest_created_time(destination_path, effective_date, args, notes, logger)
            elif args.mode == "move":
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(path, destination_path)
                moved += 1
                bytes_moved_total += file_size
                _maybe_set_dest_created_time(destination_path, effective_date, args, notes, logger)

            row_data["notes"] = "; ".join(notes)
            report_writer.writerow(row_data)
            rows_since_flush += 1
            if rows_since_flush >= 1000:
                report_file.flush()
                rows_since_flush = 0
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
        bytes_candidates_total=bytes_candidates_total,
        bytes_processed_total=bytes_processed_total,
        bytes_copied_total=bytes_copied_total,
        bytes_moved_total=bytes_moved_total,
        bytes_skipped_by_date_total=bytes_skipped_by_date_total,
        bytes_error_total=bytes_error_total,
        report_path=str(report_csv) if report_csv else "",
        duration=str(duration),
    )


if __name__ == "__main__":
    main()
