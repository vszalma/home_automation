
import pandas as pd
import shutil
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import home_automation_common
import argparse
from datetime import datetime
import structlog


# Setup logging
logging.basicConfig(
    filename="move_duplicates.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
def _get_arguments():
    """
    Parses command-line arguments for .
    Returns:
        argparse.Namespace: A namespace object containing the parsed arguments.
    Arguments:
        --input, -i (str, required): Path to the input file (output from gather_inventory.py).
        --output, -o (str, required): Output file to write deduplicated results to..
    """
    parser = argparse.ArgumentParser(
        description="Process a directory and file type for file operations."
    )

    # Add named arguments
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        required=True,
        help="Path to the input file (output from gather_inventory.py).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        required=True,
        help="Output file to write deduplicated results to.",
    )
    parser.add_argument(
        "--threads",
        "-t",
        type=str,
        required=False,
        default=True,
        help="Max number of worker threads to use for moving files. Defaults to 4",
    )
    parser.add_argument(
        "--archive",
        "-a",
        type=str,
        required=True,
        help="The location where archived files will be moved.",
    )
    parser.add_argument(
        "--source_root",
        "-s",
        type=str,
        required=True,
        help="The root directory where the source files are located. Used to prevent unsafe folder deletions.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a namespace object
    return args

def compute_archive_path(original_path: str, archive_root: Path) -> Path:
    path = Path(original_path)
    drive = path.drive.replace(":", "")
    parts = path.parts[1:]  # Skip drive
    if len(parts) > 0:
        root_folder = parts[0]
        sub_path = Path(*parts[1:])
        archive_path = archive_root / f"{drive}_{root_folder}" / sub_path
    else:
        archive_path = archive_root / f"{drive}_root"
    return archive_path

def cleanup_empty_dirs(start_path: Path, stop_path: Path):
    current = start_path
    while current != stop_path and stop_path in current.parents:
        try:
            if not any(current.iterdir()):
                current.rmdir()
                logger = structlog.get_logger()
                logger.info(
                "Deleted empty folder",
                module="move_duplicates.cleanup_empty_dirs",
                message=f"Empty folder {current} has been deleted.",
                )
            else:
                break
        except Exception as e:
            logger = structlog.get_logger()
            logger.warning(
            "Failed to delete empty folder",
            module="move_duplicates.cleanup_empty_dirs",
            message=f"Unable to delete mpty folder {current}: {e}.",
            )
            break
        current = current.parent

def move_file(row, source_root: Path, archive_root: Path):
    original_path = Path(row["path"])
    if not original_path.exists():
        logger = structlog.get_logger()
        logger.warning(
        "File not found while moving duplicate.",
        module="move_duplicates.move_file",
        message=f"File {original_path} was not found and so has not been deleted.",
        )
        row["archive_location"] = "NOT_FOUND"
        return row

    archive_path = compute_archive_path(str(original_path), archive_root)
    try:
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(original_path), str(archive_path))
        row["archive_location"] = str(archive_path)
        logger = structlog.get_logger()
        logger.info(
        "Duplicate file moved",
        module="move_duplicates.move_file",
        message=f"Duplicate file moved {original_path} -> {archive_path}",
        )

        # Attempt to remove now-empty folders
        cleanup_empty_dirs(original_path.parent, source_root)

    except Exception as e:
        logger = structlog.get_logger()
        logger.error(
        "Failed to move duplicate file",
        module="move_duplicates.move_file",
        message=f"Failed to move {original_path}: {e}",
        )
        row["archive_location"] = "ERROR"
    return row

def main():

    today = datetime.now().date()

    log_file = f"{today}_move_duplicates_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    args = _get_arguments()

    output_file = home_automation_common.get_full_filename("output", args.output)
    if not output_file.endswith('.csv'):
        logger.error(
        "Invalid output file name.",
        module="detect_duplicates.main",
        message=f"Output file {args.output} must have a .csv extension.",
        )
        return

    MAX_WORKERS = 4  # Default number of threads

    if args.threads.isdigit():
        MAX_WORKERS = int(args.threads)
    else:
        logging.warning(f"Invalid thread count '{args.threads}', using default {MAX_WORKERS}.")

    ARCHIVE_ROOT = Path(args.archive)
    if not ARCHIVE_ROOT.is_dir():
        logger.warning(
            "Invalid archive root directory.",
            module="move_duplicates.main",
            message=f"Archive root {ARCHIVE_ROOT} does not exist. Creating it.",
        )
        ARCHIVE_ROOT.mkdir(parents=True, exist_ok=True)

    SOURCE_ROOT = Path(args.source_root)
    if not SOURCE_ROOT.is_dir():
        logger.error(
            "Invalid source root directory.",
            module="move_duplicates.main",
            message=f"Source root {SOURCE_ROOT} does not exist.",
        )
        return

    if not Path(args.input).is_file():
        logger.error(
            "Invalid input file.",
            module="move_duplicates.main",
            message=f"Input file {args.input} does not exist.",
        )
        return

    start_time = datetime.now().time()

    logger.info(
        "Starting to move duplicates.",
        module="move_duplicates.main",
        message=f"Moving duplicates in file {args.input}, archiving duplicates to {args.archive} and writing to {args.output}.",
    )


    logging.info("Loading CSV and filtering duplicates to delete...")
    df = pd.read_csv(output_file)
    df["archive_location"] = ""

    duplicates_to_move = df[df["duplicate_status"] == "duplicate delete"]
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_file, row) for _, row in duplicates_to_move.iterrows()]
        for future in as_completed(futures):
            result_row = future.result()
            results.append(result_row)

    updated_df = pd.concat([df[df["duplicate_status"] != "duplicate delete"], pd.DataFrame(results)], ignore_index=True)
    updated_df.to_csv("duplicates_moved.csv", index=False)

    end_time = datetime.now().time()
    duration = home_automation_common.duration_from_times(end_time, start_time)

    logger.info(
        "Duplicate move process completed.",
        module="move_duplicates.main",
        message=f"Duplicates archived to {args.archive} and output written to {args.output}.",
        start_time=start_time,
        end_time=end_time,
        duration=duration,
    )

if __name__ == "__main__":
    main()
