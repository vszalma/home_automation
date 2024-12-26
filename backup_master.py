import datetime
import shutil
import collector
import compare
import robocopy_helper
import os
import sys

# from datetime import date
from datetime import datetime
import time
import home_automation_common
import structlog
import logging


def _backup_needed(source, destination):

    # Get most recent backup file location.
    backup_dir_list = _list_and_sort_directories(destination)

    if not backup_dir_list:
        return True
    else:
        most_recent_backup = os.path.join(destination, backup_dir_list[0])
        return _has_data_changed_since_last_backup(source, most_recent_backup)
    

def _get_free_space(file_path):
    # Get disk usage statistics
    total, used, free = shutil.disk_usage(file_path)
    return free


def _calculate_enough_space_available(most_recent_backup, file_size_total):
    free_space = _get_free_space(most_recent_backup)
    return free_space > file_size_total * .01  # allow for a bit of buffer in file size.


def _has_data_changed_since_last_backup(source, most_recent_backup):

    ret_destination, output_destination, file_size_total = collector.collect_file_info(
        most_recent_backup
    )

    # calculate free space available on destination (most_recent_back) volume.
    if not _calculate_enough_space_available(most_recent_backup, file_size_total):
        logger = structlog.get_logger()
        logger.error(
            "Not enough storage",
            module="backup_master._has_data_changed_since_last_backup",
            message=f"There is not enough storage space to run backup. {file_size_total} is needed."
        )
        return False


    ret_source, output_source, file_size_total = collector.collect_file_info(source)

    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            home_automation_common.send_email(
                "Backup not run.",
                "There was no need to backup files as the content hasn't changed.",
            )
            return False
        else:
            return True
    else:
        logger = structlog.get_logger()
        logger.error(
            "Unable to collect data.",
            module="backup_master._has_data_changed_since_last_backup",
        )
        return False


def _backup_and_validate(source, destination):

    logger = structlog.get_logger()
    destination = f"{destination}\BU-{datetime.now().date()}"
    start_time = time.time()
    # backup_result = robocopy_helper.execute_robocopy(source, destination, "Backup")
    backup_result = True
    if not backup_result:
        subject = "BACKUP FAILED!"
        body = "The backup failed. Please review the logs and rerun."
        home_automation_common.send_email(subject, body)
        return False

    end_time = time.time()
    backup_duration = end_time - start_time
    logger.info(
        "Backup completed.",
        module="backup_master._backup_and_validate",
        message="Backup completed.",
        start_time=start_time,
        end_time=end_time,
        duration=backup_duration,
    )

    return _validate_backup_results(source, destination)


def _get_arguments(argv):
    arg_help = "{0} <source directory> <destination directory>".format(argv[0])

    try:
        arg_source = (
            sys.argv[1]
            if len(sys.argv) > 1
            else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
        )
        arg_destination = (
            sys.argv[2] if len(sys.argv) > 2 else "C:\\Users\\vszal\\OneDrive\\Pictures"
        )
    except:
        print(arg_help)
        sys.exit(2)

    return [arg_source, arg_destination]


def _validate_backup_results(source, destination):

    logger = structlog.get_logger()

    # After backup, validate backup was successful (i.e. matches source file counts and sizes.)
    ret_source, output_source, file_size_total = collector.collect_file_info(source)

    ret_destination, output_destination, file_size_total = collector.collect_file_info(destination)

    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            subject = "Successful backup"
            body = "The files match"
            home_automation_common.send_email(subject, body)
            return True
        else:
            return False
    else:
        logger.error(
            "Unable to collect data.",
            module="backup_master._validate_backup_results",
            message="Unable to validate if the backup matches the source.",
        )
        return False


def coordinate_backup_process(source, destination, create_logger=True):

    if create_logger:
        home_automation_common.create_logger(module_name="backup_master")

    # execute backup if needed.
    if _backup_needed(source, destination):
        return _backup_and_validate(source, destination)
    else:
        return False


def _list_and_sort_directories(path):

    logger = structlog.get_logger()

    try:
        # List all directories one level deep
        all_items = os.listdir(path)
        directories = [d for d in all_items if os.path.isdir(os.path.join(path, d))]

        # Filter directories matching the format 'BU-YYYYMMDD'
        filtered_dirs = []
        for d in directories:
            if d.startswith("BU-") and len(d) == 13:  # Check basic format
                try:
                    # Extract YYYY-MM-DD and validate the date format
                    date_part = d[3:]  # Remove 'BU-' prefix
                    year, month, day = date_part.split(
                        "-"
                    )  # Split the date part by '-'

                    # Validate that the year, month, and day are numeric and valid
                    if len(year) == 4 and len(month) == 2 and len(day) == 2:
                        int(year)  # Ensure year is numeric
                        int(month)  # Ensure month is numeric
                        int(day)  # Ensure day is numeric

                        # Further validation for proper date format (e.g., Feb 30 should not pass)
                        # from datetime import datetime
                        datetime.strptime(date_part, "%Y-%m-%d")

                        # Add to the filtered list
                        filtered_dirs.append((d, date_part))
                except ValueError:
                    continue

        # Sort by the extracted date part
        sorted_dirs = sorted(filtered_dirs, key=lambda x: x[1], reverse=True)

        # Return only the directory names
        # most_recent_bu_dir = os.path.join(path, [d[0] for d in sorted_dirs][0])
        most_recent_bu_dir = [d[0] for d in sorted_dirs]

        return most_recent_bu_dir

    except Exception as e:
        logger.exception(
            "File collection exception found.",
            module="backup_master._list_and_sort_directories",
            message=f"An error occurred: {e}",
        )
        return []


if __name__ == "__main__":

    arguments = _get_arguments(sys.argv)
    coordinate_backup_process(arguments[0], arguments[1], True)
