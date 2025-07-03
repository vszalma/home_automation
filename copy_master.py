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
import collector

""" 
    This script compares 2 directories to determine if they are the same.
    If they are not the same, it run the 
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

"""
    code here needs to mimic backup_master.py
    It should compare the source directory with the destination directory.
    
    if they are different display the list of dirs and their counts. from collect_file_info function.
"""

    
def _are_the_directories_different(source, destination):
    """
    Checks if the data in the source directory is different than the destination directory.
    This function collects file information from the source and destination directories,
    compares the files, and determines if they are different. It also checks if there
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

    if not os.path.exists(destination):
        os.makedirs(destination, exist_ok=True)

    ret_destination, output_destination, dest_file_size_total, dest_total_file_count = collector.collect_file_info(
        destination
    )

    ret_source, output_source, source_file_size_total, source_total_file_count = collector.collect_file_info(source)

    # calculate free space available on destination (most_recent_back) volume.
    if not home_automation_common.calculate_enough_space_available(destination, source_file_size_total):
        logger = structlog.get_logger()
        logger.error(
            "Not enough storage",
            module="compare_dirs._are_the_directories_different",
            message=f"There is not enough storage space to copy to destination. {source_file_size_total} is needed."
        )
        return False

    if ret_source and ret_destination:
        files_unchanged = compare.compare_files(output_source, output_destination)
        if files_unchanged:
            logger = structlog.get_logger()
            logger.info(
                "Directories are the same.",
                module="compare_dirs._are_the_directories_different",
                message=f"Source ({source}) and destination ({destination}) are the same."
            )
            return False
        else:
            logger = structlog.get_logger()
            logger.info(
                "Directories are different.",
                module="compare_dirs._are_the_directories_different",
                message=f"Source ({source}) and destination ({destination}) contain differences. Preparing to copy files."
            )

            _copy_files(source, destination, source_total_file_count)

            logger.info(
                "Copy completed.",
                module="compare_dirs._are_the_directories_different",
                message=f"Source ({source}) and destination ({destination}) contain differences. Preparing to copy files."
            )

            return True
    else:
        logger = structlog.get_logger()
        logger.error(
            "Unable to collect data.",
            module="copy_master._are_the_directories_different",
        )
        return False

def _copy_files(source, destination, total_files=0):
    """
    Copies files from the source directory to the destination directory.
    Args:
        source (str): The path to the source directory to copy files from.
        destination (str): The path to the destination directory to copy files to.
    Returns:
        None
    """
    
    logger = structlog.get_logger()
    logger.info(
        "Copying files.",
        module="copy_master._copy_files",
        message=f"Copying files from {source} to {destination}."
    )

    robocopy_helper.execute_robocopy(source, destination, action="Copy", total_files=total_files)   


if __name__ == "__main__":

    args = _get_arguments()

    _are_the_directories_different(args.source, args.destination)
