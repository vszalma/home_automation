import pandas as pd
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import structlog
import argparse
from datetime import datetime
from tqdm import tqdm
import home_automation_common

def _get_arguments():
    parser = argparse.ArgumentParser(description="Move duplicate files and archive them safely.")
    parser.add_argument(
        "--csv",
        "-c",
        type=str,
        required=True,
        help="CSV file containing duplicate analysis results"
    )
    parser.add_argument(
        "--archive-root",
        "-a",
        type=str,
        required=True,
        help="Destination folder where duplicates will be archived"
    )
    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        required=True,
        help="Original source directory where files are located (used for cleanup purposes)"
    )
    return parser.parse_args()

def compute_archive_path(original_path: str, archive_root: Path) -> Path:
    path = Path(original_path)
    drive = path.drive.replace(":", "")
    parts = path.parts[1:]
    if len(parts) > 0:
        root_folder = parts[0]
        sub_path = Path(*parts[1:])
        archive_path = archive_root / f"{drive}_{root_folder}" / sub_path
    else:
        archive_path = archive_root / f"{drive}_root"
    return archive_path

def cleanup_empty_dirs(start_path: Path, stop_path: Path, logger):
    current = start_path
    while current != stop_path and stop_path in current.parents:
        try:
            if not any(current.iterdir()):
                current.rmdir()
                logger.info("Deleted empty folder", folder=str(current))
            else:
                break
        except Exception as e:
            logger.warning("Failed to remove folder", folder=str(current), error=str(e))
            break
        current = current.parent

def move_file(row, archive_root, source_root, logger):
    original_path = Path(row["path"])
    if not original_path.exists():
        logger.warning("File not found", file=str(original_path))
        row["archive_location"] = "NOT_FOUND"
        return row

    archive_path = compute_archive_path(str(original_path), archive_root)
    try:
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(original_path), str(archive_path))
        row["archive_location"] = str(archive_path)
        logger.info("Moved file", source=str(original_path), destination=str(archive_path))

        cleanup_empty_dirs(original_path.parent, source_root, logger)

    except Exception as e:
        logger.error("Failed to move file", file=str(original_path), error=str(e))
        row["archive_location"] = "ERROR"
    return row

def post_cleanup_empty_folders(source_root: Path, logger):
    logger.info("Starting post-move empty folder cleanup", root=str(source_root))
    all_folders = sorted([Path(dirpath) for dirpath, _, _ in os.walk(source_root)], reverse=True)
    for folder in all_folders:
        try:
            if not any(Path(folder).iterdir()):
                Path(folder).rmdir()
                logger.info("Deleted empty folder", folder=str(folder))
        except Exception as e:
            logger.warning("Failed to remove folder", folder=str(folder), error=str(e))

def main():
    args = _get_arguments()
    csv_file = args.csv
    archive_root = Path(args.archive_root)
    source_root = Path(args.directory) # Used for cleanup purposes

    today = datetime.now().date()
    log_file = home_automation_common.get_full_filename("log", f"{today}_move_duplicates.log")
    home_automation_common.configure_logging(log_file)
    logger = structlog.get_logger()

    df = pd.read_csv(csv_file)
    df["archive_location"] = ""

    output_file = f"{today}_duplicates_moved.csv"
    output_file = home_automation_common.get_full_filename("output", output_file)

    duplicates_to_move = df[df["duplicate_status"] == "duplicate delete"]
    results = []

    max_workers = os.cpu_count() or 2
    logger.info("Starting move operation", workers=max_workers, files=len(duplicates_to_move))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(move_file, row, archive_root, source_root, logger) for _, row in duplicates_to_move.iterrows()]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Moving files"):
            result_row = future.result()
            results.append(result_row)

    updated_df = pd.concat([df[df["duplicate_status"] != "duplicate delete"], pd.DataFrame(results)], ignore_index=True)
    updated_df.to_csv(f"{output_file}", index=False)
    logger.info("Move complete", output_csv=f"{output_file}")

    # post_cleanup_empty_folders(source_root, logger)

if __name__ == "__main__":
    main()
