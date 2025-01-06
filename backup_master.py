import argparse
import datetime
import shutil
import collector
import compare
import robocopy_helper
import os
import sys
from datetime import datetime
import time
import home_automation_common
import structlog
import logging

""" 
    The script backs up a source directory to a destination directory.
    It checks whether a backup is necessary by comparing the source to the most recent backup.
    If a backup is needed, it performs the backup using robocopy, validates the results, and logs the outcome.
    Notifications (e.g., success or failure) are sent via email.
 """
def _get_arguments():
    """
    Parses command-line arguments for source and destination directories.
    Returns:
        argparse.Namespace: An object containing the parsed arguments:
            - source (str): Path to the source directory to process.
            - destination (str): Path to the destination directory to process.
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
        "--destination",
        "-d",
        required=True,
        type=str,
        help="Path to the destination directory to process.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a dictionary (or list if preferred)
    return args


def _backup_needed(source, destination):
    """
    Determines if a backup is needed by comparing the source directory with the most recent backup in the destination directory.
    Args:
        source (str): The path to the source directory that needs to be backed up.
        destination (str): The path to the destination directory where backups are stored.
    Returns:
        bool: True if a backup is needed, False otherwise.
    """

    # Get most recent backup file location.
    backup_dir_list = _list_and_sort_directories(destination)

    if not backup_dir_list:
        return True
    else:
        most_recent_backup = os.path.join(destination, backup_dir_list[0])
        return _has_data_changed_since_last_backup(source, most_recent_backup)
    

def _get_free_space(file_path):
    """
    Get the available free space on the disk where the given file path is located.

    Args:
        file_path (str): The path to the file or directory to check the disk space for.

    Returns:
        int: The amount of free space in bytes.
    """
    # Get disk usage statistics
    total, used, free = shutil.disk_usage(file_path)
    return free


def _calculate_enough_space_available(most_recent_backup, file_size_total):
    """
    Determines if there is enough free space available for a backup.

    Args:
        most_recent_backup (str): The path to the most recent backup.
        file_size_total (float): The total size of the files to be backed up.

    Returns:
        bool: True if there is enough free space available, False otherwise.
    """
    free_space = _get_free_space(most_recent_backup)
    return free_space > file_size_total * .01  # allow for a bit of buffer in file size.


def _has_data_changed_since_last_backup(source, most_recent_backup):
    """
    Checks if the data in the source directory has changed since the last backup.
    This function collects file information from the source and the most recent backup,
    compares the files, and determines if a backup is necessary. It also checks if there
    is enough storage space available on the destination volume to perform the backup.
    Args:
        source (str): The path to the source directory to be backed up.
        most_recent_backup (str): The path to the most recent backup directory.
    Returns:
        bool: True if the data has changed and a backup is needed, False otherwise.
    Logs:
        - Error if there is not enough storage space available.
        - Error if unable to collect data from the source or destination.
    """

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
    """
    Backs up the source directory to the destination directory and validates the backup.
    Args:
        source (str): The source directory to back up.
        destination (str): The destination directory where the backup will be stored.
    Returns:
        bool: True if the backup and validation are successful, False otherwise.
    Raises:
        Exception: If there is an error during the backup process.
    Logs:
        Logs the start and end time of the backup process, as well as the duration.
        Logs an error message if the backup fails.
    Sends Email:
        Sends an email notification if the backup fails.
    """

    logger = structlog.get_logger()
    destination = fr"{destination}\BU-{datetime.now().date()}"
    start_time = time.time()
    backup_result = robocopy_helper.execute_robocopy(source, destination, "Backup")
    # backup_result = True
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



def _validate_backup_results(source, destination):
    """
    Validate the results of a backup operation by comparing the source and destination files.
    This function collects file information from the source and destination directories,
    compares the files, and sends an email notification if the backup is successful.
    Args:
        source (str): The path to the source directory.
        destination (str): The path to the destination directory.
    Returns:
        bool: True if the backup is successful and the files match, False otherwise.
    """

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
    """
    Coordinates the backup process between the source and destination.
    Args:
        source (str): The source directory or file path to be backed up.
        destination (str): The destination directory or file path where the backup will be stored.
        create_logger (bool, optional): If True, a logger will be created for the module. Defaults to True.
    Returns:
        bool: True if the backup was needed and successfully executed, False otherwise.
    """

    if create_logger:
        home_automation_common.create_logger(module_name="backup_master")

    # execute backup if needed.
    if _backup_needed(source, destination):
        return _backup_and_validate(source, destination)
    else:
        return False


def _list_and_sort_directories(path):
    """
    List and sort directories in the given path that match the format 'BU-YYYY-MM-DD'.
    Args:
        path (str): The path to the directory containing the directories to be listed and sorted.
    Returns:
        list: A list of directory names sorted by date in descending order. If an error occurs, an empty list is returned.
    Raises:
        ValueError: If the date part of the directory name is not a valid date.
    The function performs the following steps:
        1. Lists all items in the given path and filters out non-directory items.
        2. Filters directories that match the format 'BU-YYYY-MM-DD'.
        3. Validates the date part of the directory name.
        4. Sorts the filtered directories by date in descending order.
        5. Returns the sorted list of directory names.
    """

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

    args = _get_arguments()
    coordinate_backup_process(args.source, args.destination, True)
