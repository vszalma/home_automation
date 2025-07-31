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
    parser.add_argument(
        "--retry",
        "-r",
        required=False,
        default=5,
        type=str,
        help="Path to the destination directory to process.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a dictionary (or list if preferred)
    return args

def _compare_files(source, destination):
    """
    Compares files in the source and destination directories to determine if they are the same.
    Args:
        source (str): The path to the source directory to compare.
        destination (str): The path to the destination directory to compare.
    Returns:
        bool: True if files are the same, False if they differ.
    """
    ret_destination, output_destination, dest_file_size_total, dest_total_file_count = collector.collect_file_info(destination)

    ret_source, output_source, source_file_size_total, source_total_file_count = collector.collect_file_info(source)
    
    if ret_source and ret_destination:
        files_match = compare.compare_files(output_source, output_destination)
        return files_match, source_file_size_total, source_total_file_count
    else:
        logger = structlog.get_logger()
        logger.error(
            "Unable to compare files, exiting. Retry.",
            module="compare_dirs._compare_files",
            message=f"An error appeared while running collector.collect_file_info."
            )
        sys.exit(1)
    
def _coordinate_copy_process(source, destination):
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

    folders_already_match, source_file_size_total, source_total_file_count = _compare_files(source, destination)

    # ret_destination, output_destination, dest_file_size_total, dest_total_file_count = collector.collect_file_info(destination)

    # ret_source, output_source, source_file_size_total, source_total_file_count = collector.collect_file_info(source)

    
    if not folders_already_match:
        # calculate free space available on destination (most_recent_back) volume.
        if not home_automation_common.calculate_enough_space_available(destination, source_file_size_total):
            logger = structlog.get_logger()
            logger.error(
                "Not enough storage",
                module="compare_dirs._are_the_directories_different",
                message=f"There is not enough storage space to copy to destination. {source_file_size_total} is needed."
            )
            return False
        else:
            logger = structlog.get_logger()
            logger.info(
                "Directories are different.",
                module="compare_dirs._are_the_directories_different",
                message=f"Source ({source}) and destination ({destination}) contain differences. Preparing to copy files."
                )
    else:
        logger = structlog.get_logger()
        logger.info(
            "Directories are the same.",
            module="compare_dirs._are_the_directories_different",
            message=f"Source ({source}) and destination ({destination}) are the same."
        )
        return False

    _copy_files(source, destination, source_total_file_count)

    logger.info(
        "Copy completed.",
        module="copy_master._coordinate_copy_process",
        message=f"Source ({source}) has been copied to ({destination}). Preparing to compare files to ensure they are the same."
    )

    files_now_match, _, _ = _compare_files(source, destination)

    return files_now_match

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

    robocopy_helper.execute_robocopy(source, destination, action="Copy", total_files=total_files, move=False, retry_count=args.retry)   




if __name__ == "__main__":

    args = _get_arguments()

    folders_match = _coordinate_copy_process(args.source, args.destination)

    logger = structlog.get_logger()

    if folders_match:
        logger.info(
            "Folders now match.",
            module="copy_master.__main__",
            message=f"Folder {args.destination} matches {args.source}."
        )
