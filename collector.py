
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
from tqdm import tqdm
from pathlib import Path

def _get_arguments():
    parser = argparse.ArgumentParser(
        description="Calculate the total file size for each unique filetype in a directory."
    )
    parser.add_argument(
        "--directory", "-d", type=str, required=True,
        help="Path to the directory to process. UNC paths are supported."
    )
    return parser.parse_args()

# def normalize_path(directory):
#     # If already extended path, return as-is
#     if directory.startswith("\\\\?\\"):
#         return directory
#     # If UNC path, add extended prefix directly
#     if directory.startswith("\\\\"):
#         return f"\\\\?\\UNC\\{directory[2:]}"
#     # Otherwise, local path
#     return f"\\\\?\\{os.path.abspath(directory)}"

def collect_file_info(directory):
    logger = structlog.get_logger()
    start_time = time.time()
    logger.info("Directory search.", module="collector.collect_file_info", message=f"Directory to be searched is {directory}.")
    exclusions = home_automation_common.get_exclusion_list("collector")
    filetype_lookup = _build_reverse_filetype_lookup(FILE_TYPE_GROUPS)

    if os.path.isdir(directory):
        if os.name == "nt":
            directory = home_automation_common.normalize_path(directory)
        else:
            directory = os.path.abspath(directory)

        file_info, file_size_total, total_file_count = _calculate_file_info(
            directory, logger, exclusions, filetype_lookup
        )
        output_file = _output_file_info(directory, file_info)
        duration = time.time() - start_time
        logger.info("Collection completed.", module="collector.collect_file_info", message="Collection completed.",
                    duration=duration, file=output_file, total_size=file_size_total, total_file_count=total_file_count)
        return True, output_file, file_size_total, total_file_count
    else:
        logger.error("Invalid directory.", module="collector.collect_file_info",
                     message="Invalid directory. Please correct and try again.")
        return False, "Invalid directory. Please try again.", 0, 0

def _build_reverse_filetype_lookup(file_type_groups):
    reverse_lookup = {}
    for group, patterns in file_type_groups.items():
        for pattern in patterns:
            matches = re.findall(r"\((.*?)\)", pattern)
            if matches:
                extensions = matches[0].split("|")
                for ext in extensions:
                    reverse_lookup[f".{ext.lower()}"] = group
    return reverse_lookup

def _calculate_file_info(directory, logger, exclusions, filetype_lookup):
    file_info = defaultdict(lambda: {"count": 0, "size": 0, "group": ""})
    file_size_total = 0
    total_file_count = 0
    all_files = []

    with tqdm(desc=f"Counting files in {directory}", unit="file") as pbar:
        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if d not in exclusions]
            for file in files:
                file_path = os.path.join(root, file)
                all_files.append(file_path)
                pbar.update(1)

    for file_path in tqdm(all_files, desc=f"Scanning files in {directory}", unit="file"):
        file_extension = os.path.splitext(file_path)[1].lower()
        try:
            file_size = os.path.getsize(file_path)
            group_name = filetype_lookup.get(file_extension, "unknown")
            file_size_total += file_size
            total_file_count += 1
        except OSError:
            logger.warning("File skipped.", module="collector.collect_file_info",
                           message=f"Skipped file {file_path} due to error.")
            continue

        file_info[file_extension]["count"] += 1
        file_info[file_extension]["size"] += file_size
        file_info[file_extension]["group"] = group_name

    return file_info, file_size_total, total_file_count

def _output_file_info(directory, file_info):
    sanitized_name = home_automation_common.sanitize_filename(directory)
    output_file = f"{datetime.now().date()}-collector-output-{sanitized_name}.csv"
    output_file = home_automation_common.get_full_filename("output", output_file)
    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["file_type", "count", "total_size"])
        for file_type, stats in sorted(file_info.items()):
            writer.writerow([file_type, stats["count"], stats["size"]])
    return output_file

if __name__ == "__main__":
    today = datetime.now().date()
    log_file = home_automation_common.get_full_filename("log", f"{today}_collector_log.txt")
    home_automation_common.configure_logging(log_file)
    logger = structlog.get_logger()
    args = _get_arguments()
    collect_file_info(args.directory)
