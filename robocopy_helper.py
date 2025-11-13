import shlex
import subprocess
import sys
import structlog
import home_automation_common
from datetime import datetime
import os
import argparse
import unicodedata

from tqdm import tqdm
import re
from home_automation_common import get_log_path


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


def sanitize_path(path):
    """Remove control characters and quotes from a path."""
    return ''.join(c for c in path if unicodedata.category(c)[0] != "C").strip('"')

def _count_files(directory):
    return sum(len(files) for _, _, files in os.walk(directory))

def make_unc_path(path: str) -> str:
    """
    Converts a local or UNC path to an extended-length path using the \\?\\ prefix.
    Handles both local (e.g., C:\...) and network (e.g., \\Server\Share\...) paths.
    """
    path = os.path.abspath(path)

    # If already using extended-length format, return as-is
    if path.startswith(r"\\?\\"):
        return path

    # If it's a UNC path (starts with double backslash), convert to \\?\UNC\ format
    if path.startswith(r"\\"):
        # Strip leading backslashes and prefix with \\?\UNC\
        unc_path = path.lstrip("\\")
        return f"\\\\?\\UNC\\{unc_path}"

    # Otherwise, it's a local path (e.g., C:\...), prefix with \\?\
    return f"\\\\?\\{path}"


def _run_robocopy(source, destination, options=None, log_file=None, total_files=0):
    """
    Executes the robocopy command with live output, error logging, tqdm progress bar,
    and a final summary table.
    """
    logger = structlog.get_logger()

    try:
        source = make_unc_path(source)
        destination = make_unc_path(destination)

        command = ["robocopy", source, destination]
        if options:
            command.extend(options)

        logger.info("Starting robocopy", command=shlex.join(command))

        log = open(log_file, "w", encoding="utf-8", errors="replace") if log_file else None

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        progress_bar = tqdm(total=total_files, unit="files", desc="Copying", ncols=80)
        file_counter = 0
        summary_lines = []
        failed_files = set()

        for line in process.stdout:
            line = line.strip("\r\n")
            if log:
                log.write(line + "\n")

            # Summary capture (first time + final stats)
            if file_counter == 0 and not re.search(r"\bNew File\b", line):
                summary_lines.append(line)
            elif file_counter == total_files and not re.search(r"\bNew File\b", line):
                summary_lines.append(line)

            # Error detection and failed file extraction
            if (file_counter < total_files) and ("ERROR" in line or "fail" in line.lower()):
                logger.error("Robocopy error", detail=line)
                match = re.search(r'Copying File\s+(.*)', line, re.IGNORECASE)
                if match:
                    failed_file = sanitize_path(match.group(1))
                    failed_files.add(failed_file)
                else:
                    failed_files.add(line)  # fallback if no path found
                summary_lines.append(line)

            # Progress bar update
            if re.search(r"\bNew File\b", line):
                file_counter += 1
                progress_bar.update(1)

        process.stdout.close()
        return_code = process.wait()
        progress_bar.close()
        if log:
            log.close()

        # Write failed files to output file
        if failed_files:
            today = datetime.now().date()
            failed_files_path = home_automation_common.get_full_filename("output", f"{today}_robocopy_failed_files.txt")
            with open(failed_files_path, "w", encoding="utf-8", errors="replace") as f:
                for file in sorted(failed_files):
                    f.write(file + "\n")

        # Summary output
        print("\n---------------- Robocopy Summary ----------------\n")
        for line in summary_lines:
            print(line)

        # Exit code interpretation
        if return_code == 0:
            logger.info("Robocopy completed successfully.")
        elif 1 <= return_code <= 7:
            logger.warning("Robocopy completed with warnings.", code=return_code)
        else:
            logger.error("Robocopy failed with error code.", code=return_code)
            return False

        return True

    except Exception as e:
        logger.exception("Exception occurred while executing robocopy", error=str(e))
        return False

def _run_robocopy_include_hidden_files(source, destination, options, output_file):
    logger = structlog.get_logger()
    source = make_unc_path(source)
    destination = make_unc_path(destination)

    # Build robocopy command
    command = ["robocopy", source, destination]
    command.extend(options)

    logger.info("Running secondary robocopy pass to include hidden files", command=shlex.join(command))
    
    try:
        with open(output_file, "a", encoding="utf-8", errors="replace") as log:
            result = subprocess.run(command, stdout=log, stderr=log, text=True)
        
        if result.returncode > 7:
            logger.warning("Secondary robocopy pass completed with errors", code=result.returncode)
            return False
        return True

    except Exception as e:
        logger.exception("Error in secondary robocopy for hidden files", error=str(e))
        return False


def execute_robocopy(source, destination, action="Backup", total_files=0, move=False, retry_count=3):
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

    output_file = f"{datetime.now().date()}_robocopy_log.txt"

    output_file = home_automation_common.get_full_filename("log", output_file)

    if not os.path.exists(source):
        logger.error(
            "File does not exist",
            module="robocopy_helper.execute_robocopy",
            message="Source file location does not exist",
            location=source,
        )
        return False, f"Source {source} does not exist."

    if not os.path.exists(destination):
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        os.makedirs(destination, exist_ok=True)
        # logger.error("File does not exist", module="robocopy_helper.execute_robocopy", message="Destination file location does not exist", location=destination)
        # return False

    if total_files <= 0:
        total_files = _count_files(source)

    core_count = os.cpu_count()
    if core_count is None:
        core_count = 2  # fallback
    core_count = max(1, core_count - 1)

    logger.info(
        f"{action} running.",
        module="robocopy_helper.execute_robocopy",
        message="robocopy is being run.",
    )
    options = ["/E", f"/MT:{core_count}", "/XA:S", "/xo", "/ndl", f"/R:{retry_count}", "/W:5", "/256", "/A-:SHR"]

    exclusions = home_automation_common.get_exclusion_list("collector")

    if exclusions:
        options += ["/XD"] + list(exclusions if isinstance(exclusions, (list, set)) else [exclusions])

    if move:
        options.append("/MOV")

    # # Add excluded folders (relative or absolute)
    # excluded_folders = ["/XD", "System Volume Information", "$RECYCLE.BIN"] 

    # # Combine options
    # options += excluded_folders

    isCompleted = _run_robocopy(source, destination, options, output_file, total_files)

    if isCompleted:
        # Optional second pass to include important hidden files
        options = ["/E", f"/MT:{core_count}", "/xo", "/ndl", f"/R:{retry_count}", "/W:5", "/256", "/IF"]
        extra_hidden_files = [".gitignore", ".gitattributes", "*.onetoc2"]
        options += extra_hidden_files
        if exclusions:
            options += ["/XD"] + list(exclusions if isinstance(exclusions, (list, set)) else [exclusions])
        isCompleted = _run_robocopy_include_hidden_files(source, destination, options, output_file)

    if isCompleted:
        end_time = datetime.now().time()
        duration = home_automation_common.duration_from_times(end_time, start_time)
        logger.info(
            f"{action} completed.",
            module="robocopy_helper.execute_robocopy",
            message="robocopy completed.",
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            source=source,
            destination=destination,
        )
        return True, "Robocopy completed successfully."


if __name__ == "__main__":

    args = _get_arguments()

    today = datetime.now().date()

    log_file = f"{today}_backup_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    robocopy_result = execute_robocopy(args.source, args.destination, args.action, total_files=0, move=False, retry_count=10)
    robocopy_success, robocopy_message = robocopy_result
