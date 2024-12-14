import datetime
import collector
import compare
import backup
import os
import sys
#from datetime import date
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
        most_recent_backup = backup_dir_list[0]

    start_time = time.time()
    ret_destination, output_destination = collector.collect_file_info(
        most_recent_backup
    )
    end_time = time.time()
    destination_duration = end_time - start_time
    logging.info(f"Time taken go collect file info for destination prior to backup: {destination_duration}")

    if ret_destination:
        logging.info(f"output file: {output_destination}")

    start_time = time.time()
    ret_source, output_source = collector.collect_file_info(source)
    if ret_source:
        logging.info(f"output file: {output_source}")
    end_time = time.time()
    source_duration = end_time - start_time
    logger.info("File metadata collection duration.", module="home_automation_master", message=f"Time taken go collect file info for source prior to backup: {source_duration}")

    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            home_automation_common.send_email(
                "Backup not run.",
                "There was no need to backup files as the content hasn't changed.",
            )
            return False
        else:
            return True


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


def main(source, destination):

    # execute backup if needed.
    if _backup_needed(source, destination):
        destination = f"{destination}\BU-{datetime.now().date()}"
        start_time = time.time()
        backup_result = backup.execute_backup(source, destination)
        if not backup_result:
            subject = "BACKUP FAILED!"
            body = "The backup failed. Please review the logs and rerun."
            home_automation_common.send_email(subject, body)
            return
        end_time = time.time()
        backup_duration = end_time - start_time
        logger.info("Backup completed.", module="home_automation_master", message="Backup completed.", start_time=start_time, end_time=end_time, duration=backup_duration)


    # After backup, validate backup was successful (i.e. matches source file counts and sizes.)
    ret_source, output_source = collector.collect_file_info(source)
    if ret_source:
        logger.info("Collector output.", module="home_automation_master", message=f"output file: {output_source}")

    ret_destination, output_destination = collector.collect_file_info(destination)
    if ret_destination:
        logger.info("Collector output", module="home_automation_master", message=f"output file: {output_destination}")

    # if different, run a backup
    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            subject = "Successful backup"
            body = "The files match"
            home_automation_common.send_email(subject, body)


def _list_and_sort_directories(path):
    try:
        # List all directories one level deep
        all_items = os.listdir(path)
        directories = [d for d in all_items if os.path.isdir(os.path.join(path, d))]

        # Filter directories matching the format 'BU-YYYYMMDD'
        filtered_dirs = []
        for d in directories:
            if d.startswith("BU-") and len(d) == 13:  # Check basic format
                try:
                    # Extract YYYYMMDD and check if it is valid
                    date_part = d[3:]
                    int(date_part)  # Check if it is a valid number
                    filtered_dirs.append((d, date_part))
                except ValueError:
                    continue

        # Sort by the extracted date part
        sorted_dirs = sorted(filtered_dirs, key=lambda x: x[1], reverse=True)

        # Return only the directory names
        return [d[0] for d in sorted_dirs]

    except Exception as e:
        logger.exception("File collection exception found.", module="home_automation_master", message=f"An error occurred: {e}")
        return []


if __name__ == "__main__":

    today = datetime.now().date()    #.strftime("%Y-%m-%d")

    log_file = f"{today}_home_automation_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    arguments = _get_arguments(sys.argv)
    main(arguments[0], arguments[1])
