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


def collect_file_info(directory):
    logger = structlog.get_logger()
    start_time = time.time()

    logger.info("Directory search.", module="collector.collect_file_info", message=f"Directory to be searched is {directory}.")
    exclusions = home_automation_common.get_exclusion_list("collector")
    filetype_lookup  = _build_reverse_filetype_lookup(FILE_TYPE_GROUPS)
    if os.path.isdir(directory):
        file_info, file_size_total = _calculate_file_info(directory, logger, exclusions, filetype_lookup)
        output_file = _output_file_info(directory, file_info)
        end_time = time.time()
        duration = end_time - start_time
        logger.info("Collection completed.", module="collector.collect_file_info", message="Collection completed.", duration=duration, file=output_file, total_size=file_size_total)
        return True, output_file, file_size_total
    else:
        logger.error("Invalid directory.", module="collector.collect_file_info", message="Invalid directory. Please correct and try again.")
        return False, "Invalid directory. Please try again.", 0


def _build_reverse_filetype_lookup(file_type_groups):
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
    Determine if a file is hidden or a system file (Windows only).
    Returns True if the file is hidden or system, False otherwise.
    """
    try:
        file_attributes = os.stat(file_path).st_file_attributes
        return bool(file_attributes & (stat.FILE_ATTRIBUTE_HIDDEN | stat.FILE_ATTRIBUTE_SYSTEM))
    except AttributeError:
        # For non-Windows systems, return False (no hidden/system attributes)
        return False


def _calculate_file_info(directory, logger, exclusions, filetype_lookup):
    # Dictionary to store file type information
    file_info = defaultdict(lambda: {"count": 0, "size": 0, "group": ""})

    # Add the \\?\ prefix to the root directory
    if os.name == "nt":  # Only apply on Windows
        directory = r"\\?\\" + os.path.abspath(directory)
    else:
        directory = os.path.abspath(directory)

    file_size_total = 0

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

            file_extension = os.path.splitext(file)[1].lower()  # Get the extension (case-insensitive)
            try:
                # Ensure the file path uses the extended-length prefix
                # if os.name == "nt":
                #     file_path = r"\\?\\" + os.path.abspath(file_path)
                file_size = os.path.getsize(file_path)
                group_name = filetype_lookup.get(file_extension, "unknown")
                file_size_total += file_size
            except OSError:
                logger.warning("File skipped.", module="collector.collect_file_info", message=f"Skipped file {file_path} due to error.")
                continue  # Skip files that can't be accessed

            # Update dictionary
            file_info[file_extension]["count"] += 1
            file_info[file_extension]["size"] += file_size
            file_info[file_extension]["group"] += group_name

    return file_info, file_size_total


def _output_file_info(directory, file_info):
    # Print a summary of the results

    sanitized_name = home_automation_common.sanitize_filename(directory)

    output_file = (
        f"{datetime.now().date()}-collector-output-{sanitized_name}.csv"
    )

    output_file = home_automation_common.get_full_filename("output", output_file)

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["file_type", "count", "total_size"])  # Write the headers

        for file_type, stats in sorted(file_info.items()):
            writer.writerow([file_type, stats["count"], stats["size"]])

    return output_file


def _get_arguments(argv):
    arg_help = "{0} <directory>".format(argv[0])

    try:
        arg_directory = (
            sys.argv[1]
            if len(sys.argv) > 1
            else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
        )
    except:
        print(arg_help)
        sys.exit(2)

    return [arg_directory]


if __name__ == "__main__":

    today = datetime.now().date()

    log_file = f"{today}_collector_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    arguments = _get_arguments(sys.argv)

    ret, output_file, file_size_total = collect_file_info(arguments[0])
    print ("done.")