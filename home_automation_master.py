import collector
import compare
import backup
import os
import sys
from datetime import date
import time
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
    print(f"Time taken go collect file info for destination prior to backup: {destination_duration}")

    if ret_destination:
        print(f"output file: {output_destination}")

    start_time = time.time()
    ret_source, output_source = collector.collect_file_info(source)
    if ret_source:
        print(f"output file: {output_source}")
    end_time = time.time()
    source_duration = end_time - start_time
    print(f"Time taken go collect file info for source prior to backup: {source_duration}")

    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            compare.send_email(
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
        destination = f"{destination}/BU-{date.today()}"
        start_time = time.time()
        backup.execute_backup(source, destination)
        end_time = time.time()
        backup_duration = end_time - start_time
        print(f"Backup duration: {backup_duration}")


    # After backup, validate backup was successful (i.e. matches source file counts and sizes.)
    ret_source, output_source = collector.collect_file_info(source)
    if ret_source:
        print(f"output file: {output_source}")

    ret_destination, output_destination = collector.collect_file_info(destination)
    if ret_destination:
        print(f"output file: {output_destination}")

    # if different, run a backup
    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            subject = "Successful backup"
            body = "The files match"
            compare.send_email(subject, body)


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
        print(f"An error occurred: {e}")
        return []


if __name__ == "__main__":
    arguments = _get_arguments(sys.argv)
    main(arguments[0], arguments[1])
