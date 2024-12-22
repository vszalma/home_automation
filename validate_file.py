from PIL import Image
import os
import re
import sys
import csv
import warnings
import structlog
import logging
from datetime import datetime, timedelta
from time import time
import home_automation_common

warnings.simplefilter("ignore", UserWarning)


# Define logical groupings for file types - also used in collector.py
FILE_TYPE_GROUPS = {
    "image": [r".*\.(jpg|jpeg|png|gif|bmp|tiff)$"],
    "document": [r".*\.(docx)$"],
    "video": [r".*\.(mp4|avi|mkv|mov|flv|wmv|webm)$"],
    "audio": [r".*\.(mp3|wav|aac|flac|m4a|ogg)$"],
    "excel": [r".*\.(xlsx)$"],
    "pdf": [r".*\.(pdf)$"],
}


def _get_arguments(argv):
    arg_help = "{0} <directory> <filetype>".format(argv[0])

    try:
        arg_directory = (
            sys.argv[1]
            if len(sys.argv) > 1
            else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
        )
        arg_filetypes = sys.argv[2] if len(sys.argv) > 2 else "image"
    except:
        print(arg_help)
        sys.exit(2)

    return [arg_directory, arg_filetypes]


def _validate_document(file_path):
    from docx import Document

    try:
        # Try to open the document
        doc = Document(file_path)

        # Check if the document contains paragraphs
        if len(doc.paragraphs) > 0:
            return True, "Valid document file."
        else:
            return False, f"Document {file_path} is empty."

    except Exception as e:
        return False, f"Document {file_path} is not valid."


def _validate_audio(file_path):
    from pydub import AudioSegment

    try:
        # Attempt to load the audio file
        audio = AudioSegment.from_file(file_path)
        return True, f"Valid audio file: {file_path}"
    except Exception as e:
        return False, f"Invalid audio file: {file_path} (Error: {e})"


def _validate_excel(file_path):
    from openpyxl import load_workbook

    try:
        # Try to open the workbook
        workbook = load_workbook(file_path, read_only=True)
        # Check if the workbook contains any sheets
        if not workbook.sheetnames:
            return False, f"Invalid .xlsx file: {file_path} (No sheets found)."
        else:
            return True, "Valid .xlsx file."
    except Exception as e:
        return False, f"Invalid .xlsx file: {file_path} (Error: {e})."


def _validate_pdf(file_path):
    from contextlib import redirect_stderr
    import io

    try:
        # Importing PyPDF2 inside the function
        from PyPDF2 import PdfReader

        with io.StringIO() as err_buffer, redirect_stderr(err_buffer):
            reader = PdfReader(file_path)

            if reader.pages:
                if reader.is_encrypted:
                    return True, "File is encrypted"
                else:
                    return True, "Valid pdf file."
            else:
                return False, f"Invalid PDF: {file_path} (No pages found)"

    except Exception as e:
        return False, f"Invalid PDF: {file_path} (Error: {e})"


def _validate_video(file_path):
    import subprocess

    try:
        # Run FFprobe command
        command = [
            "ffprobe",
            "-v",
            "error",  # Only show errors
            "-show_entries",
            "format=duration",  # Check for duration (validity indicator)
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            file_path,
        ]
        subprocess.check_output(command, stderr=subprocess.STDOUT)
        return True, "Valid video file."
    except subprocess.CalledProcessError as e:
        return False, f"Video file {file_path} is not a valid video file."


def _validate_image(file_path):
    try:
        with Image.open(file_path) as img:
            img.verify()  # Verify that it is a valid image
        return True, "Valid image file."  # the file is not invalid
    except Exception as e:
        return False, f"Invalid image file: {e}"


def _get_total_file_count(directory, exclusions):
    file_count = 0

    for root, dirs, files in os.walk(directory):
        # Exclude directories
        dirs[:] = [d for d in dirs if os.path.join(root, d) not in exclusions]

        # Count files, excluding those in the exclusions
        for file in files:
            file_path = os.path.join(root, file)
            if file_path not in exclusions:
                file_count += 1

    return file_count


def _normalize_path(path, directory=None):
    if directory:
        path = f"{directory}{path}"
    return os.path.normpath(path)


def _get_file_count_for_type(directory, compiled_pattern, exclusions):
    file_count = 0

    # Walk through the directory and subdirectories
    for root, dirs, files in os.walk(directory):
        # Exclude directories
        dirs[:] = [d for d in dirs if os.path.join(root, d) not in exclusions]

        # Count matching files, excluding those in the exclusions
        for file in files:
            file_path = os.path.join(root, file)
            if file_path not in exclusions and compiled_pattern.match(file):
                file_count += 1

    return file_count


def _write_summary_file(
    file_type_or_group, total_files, file_count, error_count, start_time, end_time
):
    summary_headers = [
        "date",
        "file_type",
        "total_file_count",
        "matching_file_count",
        "error_count",
        "start_time",
        "end_time",
        "duration",
    ]
    summary_output_file = "validation_summary_output.csv"

    duration = home_automation_common.duration_from_times(end_time, start_time)

    summary_output_data = [
        today,
        file_type_or_group,
        total_files,
        file_count,
        error_count,
        start_time,
        end_time,
        duration,
    ]
    # headers = ["Date", "Description", "Details"]

    _append_to_csv(summary_output_file, summary_output_data, summary_headers)


def _append_to_csv(file_path, data, headers=None):

    output_file = home_automation_common.get_full_filename("output", file_path)

    file_exists = os.path.exists(output_file)  # Check if the file already exists

    with open(output_file, mode="a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # Write headers if the file is newly created
        if not file_exists and headers:
            writer.writerow(headers)

        # Append the data
        writer.writerow(data)


def validate_files_by_type(start_folder, file_type_or_group):
    from tqdm import tqdm

    start_time = datetime.now().time()

    logger = structlog.get_logger()

    logger.info("Searching for file types.", module="validate_file.validate_files_by_type", message="Directory to be searched for file types.", folder=start_folder, type=file_type_or_group)

    # Resolve regex patterns based on group or specific type
    if isinstance(file_type_or_group, str):
        patterns = FILE_TYPE_GROUPS.get(
            file_type_or_group.lower(), [file_type_or_group]
        )
    elif isinstance(file_type_or_group, list):
        patterns = file_type_or_group
    else:
        logger.error("Invalid file type argument.", module="validate_file.validate_files_by_type", message="file_type_or_group must be a string or a list")
        raise ValueError("file_type_or_group must be a string or a list")

    if not os.path.exists(start_folder):
        logger.error("Folder does not exist", module="validate_file.validate_files_by_type", message=f"Folder {start_folder} does not exist. Retry.")
        raise ValueError("Invalid folder was specified. It does not exist.")

    # Compile all patterns into a single regex for efficiency
    combined_pattern = re.compile("|".join(patterns), re.IGNORECASE)

    exclusion_file = f"{file_type_or_group}_exclusions.txt"

    # Load exclusions
    exclusions = set()
    if exclusion_file:
        try:
            with open(exclusion_file, "r", encoding="utf-8") as f:
                # exclusions = set(line.strip() for line in f if line.strip())
                exclusions = set(
                    _normalize_path(line.strip(), start_folder)
                    for line in f
                    if line.strip()
                )
        except FileNotFoundError:
            logger.info(
                "No exclusions found.", module="validate_file.validate_files_by_type", message=f"Exclusion file {exclusion_file} not found. Continuing without exclusions."
            )

    file_count = 0
    error_count = 0
    logger.info("File type search beginning.", module="validate_file.validate_files_by_type", message=f"Looking for {file_type_or_group} files in directory {start_folder}.")

    total_files = _get_total_file_count(start_folder, exclusions)
    matching_files = _get_file_count_for_type(
        start_folder, combined_pattern, exclusions
    )

    logger.info("Matching files found.", module="validate_file.validate_files_by_type", message=f"Found a total of {matching_files} found. Validating files now.")

    headers = ["file_name", "error_message"]
    output_file = (
        f"{datetime.now().date()}-error-output-{file_type_or_group}.csv"
    )

    output_file = home_automation_common.get_full_filename("output", output_file)

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)  # Write the headers

        with tqdm(
            total=matching_files, desc="Processing matching files", unit="file"
        ) as pbar:  # Walk through the directory and subdirectories
            for root, dirs, files in os.walk(start_folder):
                # Skip excluded directories
                dirs[:] = [d for d in dirs if os.path.join(root, d) not in exclusions]

                for filename in files:
                    file_path = os.path.join(root, filename)

                    # Skip excluded files
                    if file_path in exclusions:
                        continue

                    if combined_pattern.match(filename):
                        file_count += 1
                        pbar.set_description(f"Processing: {filename}")
                        file_path = r"\\?\\" + os.path.abspath(file_path)
                        # Validate file based on type
                        match file_type_or_group:
                            case "image":
                                is_valid, error_message = _validate_image(file_path)
                            case "pdf":
                                is_valid, error_message = _validate_pdf(file_path)
                            case "video":
                                is_valid, error_message = _validate_video(file_path)
                            case "excel":
                                is_valid, error_message = _validate_excel(file_path)
                            case "audio":
                                is_valid, error_message = _validate_audio(file_path)
                            case "document":
                                is_valid, error_message = _validate_document(file_path)
                            case _:
                                logger.error("Incorrect file type found.", module="validate_file.validate_files_by_type", message="An undefined filetype is found")
                                is_valid, error_message = False, "Undefined filetype"

                        pbar.update(1)
                        if not is_valid:
                            error_count += 1
                            writer.writerow([file_path, error_message])
                            logger.info("File validation exception found.", module="validate_file.validate_files_by_type", message=error_message, file=file_path)

    if error_count == 0:
        os.remove(output_file)

    end_time = datetime.now().time()

    _write_summary_file(
        file_type_or_group, total_files, file_count, error_count, start_time, end_time
    )

    logger.info(
        "Validation completed.", module="validate_file.validate_files_by_type", message="Analysis complete.",
        total_files=total_files,
        matching_files=matching_files,
        error_count=error_count,
    )


if __name__ == "__main__":

    today = datetime.now().date()

    log_file = f"{today}_validate_file_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    arguments = _get_arguments(sys.argv)

    if len(arguments) != 2:
        logger.error("Invalid arguments.", module="validate_file.__main__", message="Invalid arguments.")
    else:
        # Validate files
        validate_files_by_type(arguments[0], arguments[1])
