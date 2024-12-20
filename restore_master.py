import datetime
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


def _backup_available(source, destination):
    
    logger = structlog.get_logger()

    if "BU-" in source:
        # validate if source dir exists if argument is a full path to a backup file.
        if not os.path.exists(source):
            logger.error("Restore not found.", module="restore._backup_available", message="The source directory provided does not exist.")
            return False, "None"
    else:
        # Get most recent backup file location.
        backup_dir_list = _list_and_sort_directories(destination)
        if not backup_dir_list:
            logger.error("Restore not found.", module="restore._backup_available", message="The source directory does not contain any valid backups.")
            return False, "None"
        else:
            most_recent_backup = os.path.join(destination, backup_dir_list[0])
            return True, most_recent_backup



def _restore_and_validate(source, destination):
    
    logger = structlog.get_logger()

    start_time = time.time()

    # restore_result = backup.execute_backup(source, destination)
    restore_result = True

    end_time = time.time()
    backup_duration = end_time - start_time

    if not restore_result:
        subject = "RESTORE FAILED!"
        body = "The restore failed. Please review the logs and rerun."
        home_automation_common.send_email(subject, body)

        logger.error(
            "Restore failed.",
            module="restore._handle_restore_and_validate",
            message="The restore failed. Please review the logs and rerun",
            start_time=start_time,
            end_time=end_time,
            duration=backup_duration,
            source=source,
            destination=destination
        )

        return False
    else:
    
        logger.info(
            "Restore completed.",
            module="restore._handle_restore_and_validate",
            message="Backup completed.",
            start_time=start_time,
            end_time=end_time,
            duration=backup_duration,
            source=source,
            destination=destination
        )

        return _validate_results(source, destination)


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


def _validate_results(source, destination):
    
    logger = structlog.get_logger()

    # After backup, validate backup was successful (i.e. matches source file counts and sizes.)
    ret_source, output_source = collector.collect_file_info(source)
    if ret_source:
        logger.info(
            "Collector output.",
            module="home_automation_master",
            message=f"output file: {output_source}",
        )

    ret_destination, output_destination = collector.collect_file_info(destination)
    if ret_destination:
        logger.info(
            "Collector output",
            module="home_automation_master",
            message=f"output file: {output_destination}",
        )

    # validate source and destination have same content
    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            subject = "Successful backup"
            body = "The files match"
            home_automation_common.send_email(subject, body)
            logger.info(
                    "Restore validated.",
                    module="restore._validate_results",
                    message="Source and destination are identical. Restore has been validated.",
                )

            return True
        else:
            logger.error(
                    "Restore not valid.",
                    module="restore._validate_results",
                    message="Source and destination are different. Restore unsuccessful, review and retry.",
                )
            return False
    else:
        logger.error(
                "Restore validation failed.",
                module="restore._validate_results",
                message="An error occurred while validating source and destination. Restore failed, review and retry.",
            )
        return False


def coordinate_restore_process(source, destination, create_logger=True):

    if create_logger:
        home_automation_common.create_logger("restore")

    backup_is_available, backup_file_source = _backup_available(source, destination)
    # execute backup if needed.
    if backup_is_available:
        return _restore_and_validate(backup_file_source, destination)
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
            module="home_automation_master",
            message=f"An error occurred: {e}",
        )
        return []


if __name__ == "__main__":

    arguments = _get_arguments(sys.argv)
    coordinate_restore_process(arguments[0], arguments[1], True)
