
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import csv
import logging
from pathlib import Path
from datetime import datetime
import hashlib
from tqdm import tqdm
import structlog
import home_automation_common
import argparse

"""
    This script gathers metadata from files in a specified directory and saves it to a CSV file.
    It uses multithreading to speed up the process.
"""


def _get_arguments():
    """
    Parses command-line arguments for .
    Returns:
        argparse.Namespace: A namespace object containing the parsed arguments.
    Arguments:
        --directory, -d (str, required): Path to the directory to gather inventory for.
        --threads, -t (str, required): Max number of worker threads to use.
    """
    parser = argparse.ArgumentParser(
        description="Process a directory and file type for file operations."
    )

    # Add named arguments
    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        required=True,
        help="Path to the directory to gather inventory from.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a namespace object
    return args

def is_excluded(path: Path) -> bool:
    """
    Check if the given path contains any excluded folder names at any level.
    Args:
        path (Path): The path to check.
    Returns:
        bool: True if the path contains an excluded folder name, False otherwise.
    """
    return any(part in EXCLUDED_DIRS for part in path.parts)

def get_file_metadata(file_path: Path):
    try:
        stat = file_path.stat()
        size = stat.st_size
        modified = datetime.fromtimestamp(stat.st_mtime).isoformat()
        ext = file_path.suffix.lower()
        partial_hash = ""

        try:
            with file_path.open("rb") as f:
                data = f.read(HASH_SAMPLE_SIZE)
                partial_hash = hashlib.md5(data).hexdigest()
        except Exception as e:
            logging.warning(f"Hashing failed for {file_path}: {e}")
            partial_hash = "ERROR_HASH"

        return {
            "path": str(file_path),
            "size": size,
            "modified": modified,
            "extension": ext,
            "partial_hash": partial_hash
        }
    except Exception as e:
        logging.error(f"Metadata error for {file_path}: {e}")
        return None

def gather_inventory_multithreaded(root_dir, output_file):
    root_path = Path(root_dir)
    all_files = [p for p in root_path.rglob("*") if p.is_file() and not is_excluded(p)]

    with open(output_file, mode="w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=["path", "size", "modified", "extension", "partial_hash"])
        writer.writeheader()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_path = {executor.submit(get_file_metadata, path): path for path in all_files}
            for future in tqdm(as_completed(future_to_path), total=len(future_to_path), desc="Scanning files"):
                result = future.result()
                if result:
                    writer.writerow(result)

if __name__ == "__main__":

    today = datetime.now().date()

    log_file = f"{today}_gather_inventory_log.log"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    args = _get_arguments()

    output_file = f"{today}_gather_inventory_output.csv"
    OUTPUT_FILE = home_automation_common.get_full_filename("output", output_file)

    ROOT_DIR = args.directory

        
    core_count = os.cpu_count()
    if core_count is None:
        core_count = 2  # fallback
    core_count = max(1, core_count - 1)
    MAX_WORKERS = core_count

    HASH_SAMPLE_SIZE = 1024


    global EXCLUDED_DIRS
    EXCLUDED_DIRS = home_automation_common.get_exclusion_list("collector")
    # EXCLUDED_DIRS = {f"{args.directory}\\System Volume Information", f"{args.directory}\\$RECYCLE.BIN"}

    start_time = datetime.now().time()

    logger = structlog.get_logger()
    logger.info(
        "Starting inventory gathering.",
        module="gather_inventory.__main__",
        message=f"Gathering inventory for directory {ROOT_DIR} using {MAX_WORKERS} worker threads."
    )

    gather_inventory_multithreaded(ROOT_DIR, OUTPUT_FILE)

    end_time = datetime.now().time()
    duration = home_automation_common.duration_from_times(end_time, start_time)

    logger.info(
        "Inventory gathering completed.",
        module="gather_inventory.__main__",
        message=f"Output saved to {OUTPUT_FILE}.",
        start_time=start_time,
        end_time=end_time,
        duration=duration,
    )
