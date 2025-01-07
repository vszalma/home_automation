import datetime
import collector
import compare
import os
import sys

# from datetime import date
from datetime import datetime
import time
import home_automation_common
import structlog
import argparse
import robocopy_helper


""" 
    This script facilitates the restoration of data from a backup directory to a specified destination. 
    It integrates backup validation, logging, and error handling to ensure the restored data matches the original source.
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


def _backup_available(source):
    """
    Checks if a backup is available in the given source directory.
    If the source contains "BU-", it validates if the source directory exists.
    If the source directory does not exist, it logs an error and returns False and "None".
    Otherwise, it lists and sorts the directories in the source to find the most recent backup.
    Args:
        source (str): The source directory or path to check for backups.
    Returns:
        tuple: A tuple containing a boolean indicating if a backup is available and the path to the most recent backup or "None".
    """

    logger = structlog.get_logger()

    if "BU-" in source.upper():
        # validate if source dir exists if argument is a full path to a backup file.
        if not os.path.exists(source):
            logger.error(
                "Restore not found.",
                module="restore_master._backup_available",
                message="The source directory provided does not exist.",
            )
            return False, "None"
        else:
            return True, source
    else:
        # Get most recent backup file location.
        backup_dir_list = _list_and_sort_directories(source)
        if not backup_dir_list:
            logger.error(
                "Restore not found.",
                module="restore_master._backup_available",
                message="The source directory does not contain any valid backups.",
            )
            return False, "None"
        else:
            most_recent_backup = os.path.join(source, backup_dir_list[0])
            return True, most_recent_backup


def _restore_and_validate(source, destination):
    """
    Restores data from the source to the destination and validates the results.
    This function uses the robocopy_helper to perform the restore operation and logs the process.
    If the restore fails, it sends an email notification and logs the error details.
    If the restore is successful, it validates the results.
    Args:
        source (str): The source path from which data is to be restored.
        destination (str): The destination path to which data is to be restored.
    Returns:
        bool: True if the restore and validation are successful, False otherwise.
    """

    logger = structlog.get_logger()

    start_time = time.time()

    restore_result = robocopy_helper.execute_robocopy(source, destination)
    # restore_result = True

    end_time = time.time()
    backup_duration = end_time - start_time

    if not restore_result:
        subject = "RESTORE FAILED!"
        body = "The restore failed. Please review the logs and rerun."
        home_automation_common.send_email(subject, body)

        logger.error(
            "Restore failed.",
            module="restore_master._handle_restore_and_validate",
            message="The restore failed. Please review the logs and rerun",
            start_time=start_time,
            end_time=end_time,
            duration=backup_duration,
            source=source,
            destination=destination,
        )

        return False
    else:

        logger.info(
            "Restore completed.",
            module="restore_master._handle_restore_and_validate",
            message="Backup completed.",
            start_time=start_time,
            end_time=end_time,
            duration=backup_duration,
            source=source,
            destination=destination,
        )

        return _validate_results(source, destination)


def _validate_results(source, destination):
    """
    Validate the results of a backup operation by comparing the source and destination file information.
    Args:
        source (str): The source directory or file path to validate.
        destination (str): The destination directory or file path to validate.
    Returns:
        bool: True if the source and destination are identical, indicating a successful restore.
              False if there are differences or if an error occurred during validation.
    Logs:
        Logs information about the validation process, including the output of the file collector,
        and whether the restore was successful or not.
    Sends Email:
        Sends an email notification if the restore is successful, indicating that the source and
        destination are identical.
    """

    logger = structlog.get_logger()

    # After backup, validate backup was successful (i.e. matches source file counts and sizes.)
    ret_source, output_source = collector.collect_file_info(source)
    if ret_source:
        logger.info(
            "Collector output.",
            module="backup_master._validate_results",
            message=f"output file: {output_source}",
        )

    ret_destination, output_destination = collector.collect_file_info(destination)
    if ret_destination:
        logger.info(
            "Collector output",
            module="backup_master._validate_results",
            message=f"output file: {output_destination}",
        )

    # validate source and destination have same content
    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            subject = "Successful restore"
            body = "The restore was successful. Source and destination are identical. Restore has been validated."
            home_automation_common.send_email(subject, body)
            logger.info(
                "Restore validated.",
                module="restore_master._validate_results",
                message="Source and destination are identical. Restore has been validated.",
            )

            return True
        else:
            logger.error(
                "Restore not valid.",
                module="restore_master._validate_results",
                message="Source and destination are different. Restore unsuccessful, review and retry.",
            )
            return False
    else:
        logger.error(
            "Restore validation failed.",
            module="restore_master._validate_results",
            message="An error occurred while validating source and destination. Restore failed, review and retry.",
        )
        return False


def coordinate_restore_process(source, destination, create_logger=True):
    """
    Coordinates the restore process from a source to a destination.
    Args:
        source (str): The source path where the backup is located.
        destination (str): The destination path where the backup should be restored.
        create_logger (bool, optional): Flag to create a logger for the restore process. Defaults to True.
    Returns:
        bool: True if the restore and validation process is successful, False otherwise.
    """

    if create_logger:
        home_automation_common.create_logger("restore")

    backup_is_available, backup_file_source = _backup_available(source)
    # execute backup if needed.
    if backup_is_available:
        return _restore_and_validate(backup_file_source, destination)
    else:
        return False


def _list_and_sort_directories(path):
    """
    List and sort directories in the given path that match the format 'BU-YYYYMMDD'.
    Args:
        path (str): The path to the directory containing subdirectories to be listed and sorted.
    Returns:
        list: A list of directory names sorted by the date part in descending order. If an error occurs, an empty list is returned.
    Raises:
        ValueError: If the date part of a directory name does not match the expected format.
        Exception: For any other exceptions that occur during the listing and sorting process.
    Example:
        Given the following directory structure:
        /path/to/dir/
            BU-20230101/
            BU-20230201/
            BU-20230301/
            other_dir/
        Calling _list_and_sort_directories('/path/to/dir') will return:
        ['BU-20230301', 'BU-20230201', 'BU-20230101']
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
            module="restore_master._list_and_sort_directories",
            message=f"An error occurred: {e}",
        )
        return []


if __name__ == "__main__":

    args = _get_arguments()
    coordinate_restore_process(args.source, args.destination, True)
