import os
from collections import defaultdict
import csv
from datetime import datetime
import re
import stat
import sys
import structlog
import time
import home_automation_common
from validate_file import FILE_TYPE_GROUPS
import argparse

""" 
    Collects and analyzes file information (e.g., counts, sizes) based on their types within a specified directory.
    Outputs the results to a CSV file for further use or review.
"""

def _get_arguments():
    """
    Parse and return command-line arguments for the script.
    This function sets up an argument parser to handle command-line arguments
    for calculating the total file size for each unique filetype in a directory.
    It requires a directory path to be specified.
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    Raises:
        ArgumentError: If the provided action is not 'backup' or 'restore'.
    """
    parser = argparse.ArgumentParser(
        description="Calculate the total file size for each unique filetype in a directory."
    )

    # Add named arguments
    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        required=True,
        help="Path to the directory to process.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a dictionary (or list if preferred)
    return args


def collect_file_info(directory):
    """
    Collects information about files in a specified directory.
    This function logs the start of the directory search, retrieves an exclusion list,
    builds a reverse file type lookup, and calculates file information if the directory
    is valid. It logs the completion of the collection process along with the duration,
    output file, and total file size. If the directory is invalid, it logs an error message.
    Args:
        directory (str): The path to the directory to be searched.
    Returns:
        tuple: A tuple containing:
            - bool: True if the collection was successful, False otherwise.
            - str: The path to the output file or an error message.
            - int: The total size of the files in the directory or 0 if the directory is invalid.
    """
    logger = structlog.get_logger()
    start_time = time.time()

    logger.info(
        "Directory search.",
        module="collector.collect_file_info",
        message=f"Directory to be searched is {directory}.",
    )
    exclusions = home_automation_common.get_exclusion_list("collector")
    filetype_lookup = _build_reverse_filetype_lookup(FILE_TYPE_GROUPS)
    if os.path.isdir(directory):
        file_info, file_size_total, total_file_count = _calculate_file_info(
            directory, logger, exclusions, filetype_lookup
        )
        output_file = _output_file_info(directory, file_info)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(
            "Collection completed.",
            module="collector.collect_file_info",
            message="Collection completed.",
            duration=duration,
            file=output_file,
            total_size=file_size_total,
            total_file_count=total_file_count,
        )
        return True, output_file, file_size_total, total_file_count
    else:
        logger.error(
            "Invalid directory.",
            module="collector.collect_file_info",
            message="Invalid directory. Please correct and try again.",
        )
        return False, "Invalid directory. Please try again.", 0


def _build_reverse_filetype_lookup(file_type_groups):
    """
    Create a reverse lookup dictionary from a dictionary of file type groups.

    Args:
        file_type_groups (dict): A dictionary where keys are group names and values are lists of patterns.
                                 Patterns are strings that contain file extensions enclosed in parentheses,
                                 separated by the '|' character.

    Returns:
        dict: A dictionary where keys are file extensions (with a leading dot) in lowercase and values are group names.

    Example:
        file_type_groups = {
            "images": ["(jpg|jpeg|png|gif)"],
            "documents": ["(pdf|doc|docx)"]
        }
        reverse_lookup = _build_reverse_filetype_lookup(file_type_groups)
        # reverse_lookup will be:
        # {
        #     ".jpg": "images",
        #     ".jpeg": "images",
        #     ".png": "images",
        #     ".gif": "images",
        #     ".pdf": "documents",
        #     ".doc": "documents",
        #     ".docx": "documents"
        # }
    """
    """Create a reverse lookup dictionary from FILE_TYPE_GROUPS."""
    reverse_lookup = {}
    for group, patterns in file_type_groups.items():
        for pattern in patterns:
            # Extract extensions from the pattern
            matches = re.findall(r"\((.*?)\)", pattern)  # Capture inside parentheses
            if matches:  # Ensure there's a match
                extensions = matches[0].split("|")  # Split the matched string by '|'
                for ext in extensions:
                    reverse_lookup[f".{ext.lower()}"] = group
    return reverse_lookup


def _is_hidden_or_system(file_path):
    """
    Check if a file is hidden or a system file.

    This function checks the file attributes of the given file path to determine
    if the file is hidden or a system file. It uses the `os.stat` function to
    retrieve the file attributes and checks for the `FILE_ATTRIBUTE_HIDDEN` and
    `FILE_ATTRIBUTE_SYSTEM` flags.

    Args:
        file_path (str): The path to the file to check.

    Returns:
        bool: True if the file is hidden or a system file, False otherwise.

    Note:
        This function is designed for Windows systems. On non-Windows systems,
        it will always return False as hidden/system attributes are not applicable.
    """
    try:
        file_attributes = os.stat(file_path).st_file_attributes
        return bool(
            file_attributes & (stat.FILE_ATTRIBUTE_HIDDEN | stat.FILE_ATTRIBUTE_SYSTEM)
        )
    except AttributeError:
        # For non-Windows systems, return False (no hidden/system attributes)
        return False


def _calculate_file_info(directory, logger, exclusions, filetype_lookup):
    """
    Calculate file information for a given directory, excluding specified directories.
    Args:
        directory (str): The root directory to scan for files.
        logger (logging.Logger): Logger instance for logging messages.
        exclusions (list): List of directory names to exclude from scanning.
        filetype_lookup (dict): Dictionary mapping file extensions to group names.
    Returns:
        tuple: A tuple containing:
            - file_info (defaultdict): A dictionary with file extensions as keys and dictionaries with
              "count", "size", and "group" as values.
            - file_size_total (int): The total size of all files in bytes.
    """
    # Dictionary to store file type information
    file_info = defaultdict(lambda: {"count": 0, "size": 0, "group": ""})

    # Add the \\?\ prefix to the root directory
    if os.name == "nt":  # Only apply on Windows
        directory = r"\\?\\" + os.path.abspath(directory)
    else:
        directory = os.path.abspath(directory)

    file_size_total = 0
    total_file_count = 0

    # Walk through the directory tree
    for root, dirs, files in os.walk(directory):
        # Modify the dirs list to exclude specified directories
        dirs[:] = [d for d in dirs if d not in exclusions]

        for file in files:
            # Get file extension and size
            file_path = os.path.join(root, file)

            # Skip hidden or system files
            # if _is_hidden_or_system(file_path):
            #     logger.info("Skipping file(s).", module="collector.collect_file_info", message=f"Skipping hidden or system file: {file_path}")
            #     continue

            file_extension = os.path.splitext(file)[
                1
            ].lower()  # Get the extension (case-insensitive)
            try:
                # Ensure the file path uses the extended-length prefix
                # if os.name == "nt":
                #     file_path = r"\\?\\" + os.path.abspath(file_path)
                file_size = os.path.getsize(file_path)
                group_name = filetype_lookup.get(file_extension, "unknown")
                file_size_total += file_size
                total_file_count += 1
            except OSError:
                logger.warning(
                    "File skipped.",
                    module="collector.collect_file_info",
                    message=f"Skipped file {file_path} due to error.",
                )
                continue  # Skip files that can't be accessed

            # Update dictionary
            file_info[file_extension]["count"] += 1
            file_info[file_extension]["size"] += file_size
            file_info[file_extension]["group"] += group_name

    return file_info, file_size_total, total_file_count


def _output_file_info(directory, file_info):
    """
    Generates a CSV file containing information about files in a directory.
    Args:
        directory (str): The directory containing the files.
        file_info (dict): A dictionary where keys are file types and values are
                          dictionaries with 'count' and 'size' of the files.
    Returns:
        str: The full path to the generated CSV file.
    The CSV file will have the following columns:
        - file_type: The type of the file.
        - count: The number of files of that type.
        - total_size: The total size of the files of that type.
    """
    sanitized_name = home_automation_common.sanitize_filename(directory)

    output_file = f"{datetime.now().date()}-collector-output-{sanitized_name}.csv"

    output_file = home_automation_common.get_full_filename("output", output_file)

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["file_type", "count", "total_size"])  # Write the headers

        for file_type, stats in sorted(file_info.items()):
            writer.writerow([file_type, stats["count"], stats["size"]])

    return output_file


if __name__ == "__main__":

    today = datetime.now().date()

    log_file = f"{today}_collector_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    args = _get_arguments()

    ret, output_file, file_size_total, total_file_count = collect_file_info(args.directory)
