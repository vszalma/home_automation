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
import argparse


def _get_arguments():
    """
    Parses command-line arguments for directory and file type.
    Returns:
        tuple: A tuple containing:
            - directory (str): Path to the directory to process. Defaults to 'F:\\'.
            - filetype (str): Type of files to process (e.g., '.jpg'). Defaults to '.jpg'.
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
        default="F:\\",
        help="Path to the directory to process. Defaults to 'F:\\'.",
    )
    parser.add_argument(
        "--filetype",
        "-f",
        type=str,
        required=True,
        default=".jpg",
        help="Type of files to process (e.g., '.jpg'). Defaults to '.jpg'.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a dictionary (or list if preferred)
    # return {"directory": args.directory, "filetype": args.filetype}
    return args.directory, args.filetype


def _calculate_file_hash(file_path, chunk_size=8192):
    """
    Calculate the SHA256 hash of a file.

    Args:
        file_path (str): The path to the file to hash.
        chunk_size (int, optional): The size of each chunk to read from the file. Defaults to 8192.

    Returns:
        str: The SHA256 hash of the file in hexadecimal format, or None if an error occurs.

    Raises:
        Exception: If there is an error reading the file, it will be logged and None will be returned.
    """
    sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
    except Exception as e:
        logger.exception(
            "File error",
            module="find_duplicates._calculate_file_hash",
            message=e,
            file_path=file_path,
        )
        return None


def _group_files_by_size(directory, file_extension):
    """
    Group files by size after filtering by extension.
    This function scans a given directory and its subdirectories for files with a specified extension,
    groups them by their size, and returns a dictionary where the keys are file sizes and the values
    are lists of file paths with that size. It also uses a progress bar to indicate the scanning progress.
    Args:
        directory (str): The root directory to start scanning for files.
        file_extension (str): The file extension to filter files by.
    Returns:
        defaultdict: A dictionary where keys are file sizes (in bytes) and values are lists of file paths
        with that size.
    Raises:
        Exception: If there is an error accessing a file's size, it logs the exception with the file path.
    Notes:
        - The function uses the `tqdm` library to display a progress bar.
        - It excludes directories listed in the exclusion list obtained from `home_automation_common.get_exclusion_list`.
        - If no matching files are found, it prints a message and returns an empty dictionary.
    """
    from tqdm import tqdm

    exclusions = home_automation_common.get_exclusion_list("collector", None)
    size_map = defaultdict(list)

    # Accurately count only files to be processed
    total_files = 0
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in exclusions]
        total_files += sum(
            1 for file in files if file.lower().endswith(file_extension.lower())
        )

    print(f"Total files to process: {total_files}")

    if total_files == 0:
        print("No matching files found.")
        return size_map  # Exit early if no files to process

    # Initialize tqdm progress bar
    with tqdm(
        total=total_files,
        desc="Scanning files",
        unit="file",
        leave=True,
        mininterval=0.1,
    ) as pbar:
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
    """
    Generate the output filename for duplicate files with the given extension.

    Args:
        file_extension (str): The file extension for which the output filename is generated.

    Returns:
        str: The full path of the output filename.
    """
    return home_automation_common.get_full_filename(
        "output", f"duplicate.{file_extension}s.output.csv"
    )


def _process_duplicates(duplicates, output_file):
    """
    Processes and writes duplicate file information to a CSV file.
    Args:
        duplicates (dict): A dictionary where the keys are hash values and the values are lists of file paths that have the same hash.
        output_file (str): The path to the output CSV file where the duplicate information will be written.
    Returns:
        None
    """
    headers = [
        "duplicate_count",
        "hash",
        "duplicate_file_count",
        "full_file_name",
        "path",
        "file_name",
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

                # Split file into path and file name
                path, file_name = os.path.split(file)

                duplicate_data = [
                    hash_count,
                    hash_val,
                    file_count_per_hash,
                    file,
                    path,
                    file_name,
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
    """
    Searches for duplicate files of a specific type within a given directory.
    Args:
        directory (str): The path to the directory where the search will be conducted.
        file_extension (str): The file extension of the files to search for duplicates.
    Returns:
        None
    Logs:
        - Error if the provided directory is invalid.
        - Info when the search for duplicates starts.
        - Warning if duplicate files are found, along with the output file where duplicates are listed.
        - Info if no duplicates are found.
    """

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
        logger.info(
            "No duplicate found.", module="find_duplicates.get_duplicates_by_type"
        )


if __name__ == "__main__":

    today = datetime.now().date()

    log_file = f"{today}_duplicates_file_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    directory, filetype = _get_arguments()

    get_duplicates_by_type(directory, filetype)

    logger.info("Processing completed", module="find_duplicates.__main__")
