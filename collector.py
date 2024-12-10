import os
from collections import defaultdict
import csv
from datetime import datetime
import re
import sys
import structlog
import home_automation_common


def collect_file_info(directory):
    logger = structlog.get_logger()
    logger.info(f"Directory to be searched is {directory}.")
    if os.path.isdir(directory):
        file_info = _calculate_file_info(directory, logger)
        output_file = _output_file_info(directory, file_info)
        logger.info("Collection completed.")
        return True, output_file
    else:
        logger.error("Invalid directory. Please try again.")
        return False, "Invalid directory. Please try again."


def _calculate_file_info(directory, logger):
    # Dictionary to store file type information
    file_info = defaultdict(lambda: {"count": 0, "size": 0})

    # Walk through the directory tree
    for root, _, files in os.walk(directory):
        for file in files:
            # Get file extension and size
            file_path = os.path.join(root, file)
            file_extension = os.path.splitext(file)[
                1
            ].lower()  # Get the extension (case-insensitive)
            try:
                file_size = os.path.getsize(file_path)
            except OSError:
                logger.warning("Skipped file {file_path} due to error.")
                continue  # Skip files that can't be accessed

            # Update dictionary
            file_info[file_extension]["count"] += 1
            file_info[file_extension]["size"] += file_size

    return file_info


def _sanitize_filename(directory):
    invalid_chars = r'[<>:"/\\|?*]'  # Windows-invalid characters
    sanitized = re.sub(invalid_chars, "", directory)
    sanitized = sanitized.strip()  # Remove leading and trailing spaces
    return sanitized


def _output_file_info(directory, file_info):
    # Print a summary of the results

    sanitized_name = _sanitize_filename(directory)

    output_file = (
        f"{datetime.today().strftime('%Y-%m-%d')}-collector-output-{sanitized_name}.csv"
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

    today = datetime.today().strftime("%Y-%m-%d")

    log_file = f"{today}_validation_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    arguments = _get_arguments(sys.argv)

    ret, error_message = collect_file_info(arguments[0])
