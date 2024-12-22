import os
import hashlib
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import structlog
import logging
from datetime import datetime, timedelta
from time import time
import home_automation_common
import sys
import csv


def _get_arguments(argv):
    arg_help = "{0} <directory> <filetype>".format(argv[0])

    try:
        arg_directory = (
            sys.argv[1]
            if len(sys.argv) > 1
            else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
        )
        arg_filetypes = sys.argv[2] if len(sys.argv) > 2 else "image"
    except:
        print(arg_help)
        sys.exit(2)

    return [arg_directory, arg_filetypes]


def _calculate_file_hash(file_path, chunk_size=8192):
    """Calculate the SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
    except Exception as e:
        logger.exception(
            "File error", module="find_duplicates._calculate_file_hash", message=e, file_path=file_path
        )
        return None


def _group_files_by_size(directory, file_extension):
    """Group files by size after filtering by extension."""
    from tqdm import tqdm
    exclusions = home_automation_common.get_exclusion_list("collector", None)
    size_map = defaultdict(list)

    # Accurately count only files to be processed
    total_files = 0
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in exclusions]
        total_files += sum(1 for file in files if file.lower().endswith(file_extension.lower()))

    print(f"Total files to process: {total_files}")

    if total_files == 0:
        print("No matching files found.")
        return size_map  # Exit early if no files to process

    # Initialize tqdm progress bar
    with tqdm(total=total_files, desc="Scanning files", unit="file", leave=True, mininterval=0.1) as pbar:
        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if d not in exclusions]
            for file in files:
                if file.lower().endswith(file_extension.lower()):
                    file_path = Path(root) / file
                    try:
                        file_size = os.path.getsize(file_path)
                        if file_size > 0:
                            size_map[file_size].append(file_path)
                    except Exception as e:
                        logger.exception(
                            "File error",
                            module="find_duplicates._group_files_by_size",
                            message=e,
                            file_path=file_path,
                        )
                    pbar.update(1)  # Update progress bar only for matching files

    return size_map


def _get_output_filename(file_extension):
    return home_automation_common.get_full_filename(
        "output", f"duplicate.{file_extension}s.output.csv"
    )


def _process_duplicates(duplicates, output_file):
    # open output file
    headers = [
        "duplicate_count",
        "hash",
        "duplicate_file_count",
        "file_path",
    ]

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)  # Write the headers
        hash_count = 0
        file_count_per_hash = 0
        for hash_val, files in duplicates.items():
            hash_count += 1
            file_count_per_hash = 0
            for file in files:
                file_count_per_hash += 1
                duplicate_data = [
                    hash_count,
                    hash_val,
                    file_count_per_hash,
                    file,
                ]
                writer.writerow(duplicate_data)

    return


def _find_duplicate_files(directory, file_extension):
    """
    Find duplicate files in a directory structure by filtering by extension,
    grouping by file size, and hashing files in parallel.

    Args:
        directory (str): The directory to scan.
        file_extension (str): The file extension to filter (e.g., '.txt').

    Returns:
        dict: Dictionary with hash as key and list of duplicate file paths as values.
    """
    # Step 1: Group files by size
    logger.info(
        "Grouping files by size", module="find_duplicates._find_duplicate_files"
    )

    size_map = _group_files_by_size(directory, file_extension)

    file_hashes = defaultdict(list)

    # Step 2: Hash files in parallel for each size group with more than one file
    logger.info("Calculating hashes", module="find_duplicates._find_duplicate_files")
    files_to_hash = [
        file for size, files in size_map.items() if len(files) > 1 for file in files
    ]

    with ThreadPoolExecutor() as executor:
        with tqdm(total=len(files_to_hash), desc="Hashing files", unit="file") as pbar:
            futures = {
                executor.submit(_calculate_file_hash, file): file
                for file in files_to_hash
            }
            for future in futures:
                file_path = futures[future]
                file_hash = future.result()
                if file_hash:
                    file_hashes[file_hash].append(str(file_path))
                pbar.update(1)  # Update progress bar

    # Step 3: Filter out unique files (hashes with only one file)
    duplicates = {
        hash_val: paths for hash_val, paths in file_hashes.items() if len(paths) > 1
    }

    return duplicates


def get_duplicates_by_type(directory, file_extension):

    if not os.path.isdir(directory):
        logger.error(
            "Invalid directory",
            module="find_duplicates.get_duplicates_by_type",
            message=f"{directory} was not found. Please provide a valid path.",
        )
        return

    logger.info(
        "Searching for duplicates.",
        module="find_duplicates.get_duplicates_by_type",
        message=f"Searching for duplicate '{file_extension}' files in: {directory}",
    )
    duplicates = _find_duplicate_files(directory, file_extension)

    if duplicates:
        output_file = _get_output_filename(file_extension)
        logger.warning(
            "Duplicate files found",
            module="find_duplicates.get_duplicates_by_type",
            message=f"Duplicates written to file {output_file}.",
        )
        _process_duplicates(duplicates, output_file)
    else:
        logger.info("No duplicate found.", module="find_duplicates.get_duplicates_by_type")


if __name__ == "__main__":

    today = datetime.now().date()

    log_file = f"{today}_duplicates_file_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    arguments = _get_arguments(sys.argv)

    if len(arguments) != 2:
        logger.error(
            "Invalid arguments.", module="find_duplicates..__main__", message="Invalid arguments."
        )
    else:
        get_duplicates_by_type(arguments[0], arguments[1])

    logger.info("Processing completed", module="find_duplicates.__main__")
