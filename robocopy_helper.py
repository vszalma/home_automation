import subprocess
import sys
import structlog
import home_automation_common
from datetime import datetime
import os
import argparse

def _get_arguments():
    """
    Parses command-line arguments for file operations.
    This function sets up an argument parser to handle command-line arguments
    for processing a directory and file type for file operations. It supports
    the following arguments:
    - --source (-s): Path to the source directory to process (required).
    - --destination (-d): Path to the destination directory to process (required).
    - --action (-a): Action to be taken, either 'backup' or 'restore' (default is 'backup').
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    Raises:
        ArgumentError: If the provided action is not 'backup' or 'restore'.
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
        "--action",
        "-a",
        type=str,
        default="backup",
        help="Action to be taken, backup or restore.",
    )

    # Parse the arguments
    args = parser.parse_args()

    if not args.action not in ["backup", "restore"]:
        parser.error(f"The action '{args.action}' does not equal backup or restore.")

    # Return arguments as a dictionary (or list if preferred)
    return args

# def _get_arguments(argv):
#     arg_help = "{0} <source directory> <destination directory>".format(argv[0])

#     try:
#         arg_source = (
#             sys.argv[1]
#             if len(sys.argv) > 1
#             else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
#         )
#         arg_destination = (
#             sys.argv[2] if len(sys.argv) > 2 else "C:\\Users\\vszal\\OneDrive\\Pictures"
#         )
#     except:
#         print(arg_help)
#         sys.exit(2)

#     return [arg_source, arg_destination]


def _run_robocopy(source, destination, options=None, log_file="robocopy_log.txt"):
    """
    Executes the robocopy command to copy files from the source to the destination.
    Parameters:
    source (str): The source directory path.
    destination (str): The destination directory path.
    options (list, optional): Additional options for the robocopy command. Defaults to None.
    log_file (str, optional): The file path to write the robocopy log. Defaults to "robocopy_log.txt".
    Returns:
    bool: True if the robocopy command executed successfully or with warnings, False if an error occurred.
    """

    logger = structlog.get_logger()

    try:
        # Construct the robocopy command
        command = ["robocopy", source, destination]
        if options:
            command.extend(options)

        # Open the log file for writing
        with open(log_file, "w") as log:
            result = subprocess.run(command, stdout=log, stderr=log, text=True)

        # Check the exit code
        if result.returncode == 0:
            logger.info("Robocopy completed successfully.", module="robocopy_helper._run_robocopy")
        elif result.returncode >= 1 and result.returncode <= 7:
            logger.warning(
                "Robocopy completed with warnings or skipped files.", module="robocopy_helper._run_robocopy", message="Check the log for details."
            )
        else:
            logger.error("Robocopy encountered an error.", module="robocopy_helper._run_robocopy", message="Check the log for details.")
        
        return True

    except Exception as e:
        logger.error("Error executing robocopy", module="robocopy_helper._run_robocopy", message=e)
        return False


def execute_robocopy(source, destination, action="Backup"):
    """
    Executes the Robocopy command to copy files from the source to the destination.
    Parameters:
    source (str): The source directory path.
    destination (str): The destination directory path.
    action (str): The action being performed, default is "Backup".
    Returns:
    bool: True if the Robocopy operation completed successfully, False otherwise.
    Logs:
    Logs the start and completion of the Robocopy operation, including the start time, end time, duration, source, and destination.
    """

    logger = structlog.get_logger()

    start_time = datetime.now().time()

    output_file = (f"{datetime.now().date()}_robocopy_log.txt")

    output_file = home_automation_common.get_full_filename("log", output_file)

    if not os.path.exists(source):
        logger.error("File does not exist", module="robocopy_helper.execute_robocopy", message="Source file location does not exist", location=source)
        return False

    if not os.path.exists(destination):
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        os.makedirs(destination, exist_ok=True)
        # logger.error("File does not exist", module="robocopy_helper.execute_robocopy", message="Destination file location does not exist", location=destination)
        # return False

    logger.info(f"{action} running.", module="robocopy_helper.execute_robocopy", message="Backup is being run.")
    #options = ["/E", "/MT:8", "/XA:S", "/xo", "/nfl", "/ndl"]
    options = ["/E", "/MT:8", "/xo"]

    isCompleted =_run_robocopy(source, destination, options, output_file)

    if isCompleted:
        end_time = datetime.now().time()
        duration = home_automation_common.duration_from_times(end_time, start_time)
        logger.info("Backup completed.", module="robocopy_helper.execute_robocopy", message="Backup completed.", start_time=start_time, end_time=end_time, duration=duration, source=source, destination=destination)
        return True


if __name__ == "__main__":

    args = _get_arguments()

    today = datetime.now().date()

    log_file = f"{today}_backup_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    result = execute_robocopy(args.source, args.destination, args.action)
