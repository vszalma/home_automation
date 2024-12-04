from PIL import Image
import os
import re
import sys
import csv
import warnings

warnings.simplefilter("ignore", UserWarning)


# Define logical groupings for file types
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
        workbook = load_workbook(file_path)
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

            # Check if the PDF is encrypted
            if reader.is_encrypted:
                return False, "File is encrypted"

            if reader.pages:
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


def _get_total_file_count(directory):
    file_count = 0

    for root, dirs, files in os.walk(directory):
        file_count += len(files)

    return file_count


def _get_file_count_for_type(directory, compiled_pattern):
    file_count = 0

    # Walk through the directory and subdirectories
    for root, dirs, files in os.walk(directory):
        for file in files:
            if compiled_pattern.match(file):  # Check if file matches the pattern
                file_count += 1

    return file_count


def validate_files_by_type(start_folder, file_type_or_group):
    from datetime import datetime
    from tqdm import tqdm

    # Resolve regex patterns based on group or specific type
    if isinstance(file_type_or_group, str):
        patterns = FILE_TYPE_GROUPS.get(
            file_type_or_group.lower(), [file_type_or_group]
        )
    elif isinstance(file_type_or_group, list):
        patterns = file_type_or_group
    else:
        raise ValueError("file_type_or_group must be a string or a list")

    # Compile all patterns into a single regex for efficiency
    combined_pattern = re.compile("|".join(patterns), re.IGNORECASE)

    file_count = 0
    error_count = 0
    print(f"Looking for {file_type_or_group} files in directory {start_folder}.")

    total_files = _get_total_file_count(start_folder)
    matching_files = _get_file_count_for_type(start_folder, combined_pattern)

    print(f"Found a total of {matching_files} found. Validating files now.")

    headers = ["file_name", "error_message"]
    output_file = (
        f"{datetime.today().strftime('%Y-%m-%d')}-error-output-{file_type_or_group}.csv"
    )

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["file_name", "error_message"])  # Write the headers

        with tqdm(
            total=matching_files, desc="Processing matching files", unit="file"
        ) as pbar:  # Walk through the directory and subdirectories
            for root, dirs, files in os.walk(start_folder):
                for filename in files:
                    if combined_pattern.match(filename):
                        file_count += 1
                        invalid_file = False
                        file_name = f"{root}\\{filename}"
                        # matching_files.append(os.path.join(root, filename))
                        pbar.set_description(f"Processing: {filename}")
                        match file_type_or_group:
                            case "image":
                                is_valid, error_message = _validate_image(f"{file_name}")
                            case "pdf":
                                is_valid, error_message = _validate_pdf(f"{file_name}")
                            case "video":
                                is_valid, error_message = _validate_video(f"{file_name}")
                            case "excel":
                                is_valid, error_message = _validate_excel(f"{file_name}")
                            case "audio":
                                is_valid, error_message = _validate_audio(f"{file_name}")
                            case "document":
                                is_valid, error_message = _validate_document(
                                    f"{file_name}"
                                )
                            case _:
                                print("An undefined filetype is found")
                        pbar.update(1)
                        if not is_valid:
                            error_count += 1
                            # pass filename {root}\\{filename} and error_message to method to write file
                            writer.writerow([file_name, error_message])

    if error_count == 0:
        os.remove(output_file)

    print(f"Total files read: {file_count}")
    print(f"Total errors found: {error_count}")


if __name__ == "__main__":
    arguments = _get_arguments(sys.argv)
    print("Directory to be searched: ", arguments[0])
    print("Type of file to be validated: ", arguments[1])

    # Validate files
    validate_files_by_type(arguments[0], arguments[1])

    print("Analysis complete.")
