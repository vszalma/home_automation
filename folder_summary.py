
import argparse
from datetime import datetime
import os
import csv
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import structlog
from tqdm import tqdm

import home_automation_common

def _get_arguments():
    """
    Parses command-line arguments for file operations.
    This function sets up an argument parser to handle command-line arguments
    for processing a directory and file type for file operations. It supports
    the following arguments:
    - --source (-s): Path to the source directory to process (required).
    - --threads (-t): Maximum number of worker threads to use to process files.
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    Raises:
        ArgumentError: If the provided action is not 'backup' or 'restore'.
    """
    parser = argparse.ArgumentParser(
        description="Process a directory and file type for file operations."
    )

    # Add named arguments
    parser.add_argument(
        "--source",
        "-s",
        type=str,
        required=True,
        help="Path to the source directory to process.",
    )
    parser.add_argument(
        "--threads",
        "-t",
        required=False,
        default=4,
        type=str,
        help="Maximum number of worker threads to use to process files.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a dictionary (or list if preferred)
    return args

def analyze_folder(folder_path: Path, root: Path):
    try:
        total_size = 0
        file_count = 0
        folder_count = 0
        file_types = set()

        for dirpath, dirnames, filenames in os.walk(folder_path):

            folder_count += len(dirnames)
            for file in filenames:
                file_path = Path(dirpath) / file
                try:
                    file_path = Path(f"\\\\?\\{file_path}")
                    stat = file_path.stat()
                    total_size += stat.st_size
                    file_count += 1
                    file_types.add(file_path.suffix.lower())
                except Exception as e:
                    logger = structlog.get_logger()
                    logger.warning(
                        "Unable to access file.",
                        module="folder_summary.analyze_folder",
                        message=f"Unable to access file: {file_path}: {e}",
                        )
                    continue

        depth = len(folder_path.relative_to(root).parts)
        return {
            "folder": str(folder_path),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "file_count": file_count,
            "folder_count": folder_count,
            "depth": depth,
            "file_types": ", ".join(sorted(file_types))
        }

    except Exception as e:
        logger = structlog.get_logger()
        logger.error(
            f"Error analyzing folder.",
            module="folder_summary.analyze_folder",
            message=f"Error analyzing folder {folder_path}: {e}",
            )
        return None

def main():
    
    args = _get_arguments()

    source = args.source
    max_workers = int(args.threads) if args.threads.isdigit() else 4

    # Trim the file name to a maximum of 12 characters
    limited_source = source[:12]

    output_file = f"{datetime.now().date()}_{limited_source}_folder_summary_output.csv"

    output_file = home_automation_common.get_full_filename("output", output_file)

    start_time = datetime.now().time()

    today = datetime.now().date()

    log_file = f"{today}_folder_summary_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()


    logger.info(
        f"Process started.",
        module="folder_summary.main",
        message="Folder summary started.",
        start_time=start_time,
        threads=max_workers,
        source=source,
        output_file=output_file,
        )

    exclusions = home_automation_common.get_exclusion_list("collector")
    # Convert exclusions to lowercase for case-insensitive comparison
    # exclusions = {exclusion.lower() for exclusion in exclusions}


    all_folders = [Path(dirpath) for dirpath, _, _ in os.walk(source)]

    all_folders = [
        folder for folder in all_folders
        if not any(exclusion in folder.parts for exclusion in exclusions)
        ]

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(analyze_folder, folder, source): folder for folder in all_folders}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Analyzing folders"):
            result = future.result()
            if result:
                results.append(result)

    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["folder", "total_size_mb", "file_count", "folder_count", "depth", "file_types"])
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    end_time = datetime.now().time()
    duration = home_automation_common.duration_from_times(end_time, start_time)

    logger.info(
        f"Process completed.",
        module="folder_summary.main",
        message="Folder summary written to CSV file.",
        start_time=start_time,
        end_time=end_time,
        duration=duration,
        source=source,
        output_file=output_file,
        )


if __name__ == "__main__":
    main()
