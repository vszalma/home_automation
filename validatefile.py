from PIL import Image
import os
import re
import sys

# Define logical groupings for file types
FILE_TYPE_GROUPS = {
    "images": [r".*\.(jpg|jpeg|png|gif|bmp|tiff)$"],
    "documents": [r".*\.(pdf|doc|docx|txt|xls|xlsx)$"],
    "videos": [r".*\.(mp4|avi|mkv|mov|flv|wmv|webm)$"],
    "audio": [r".*\.(mp3|wav|aac|flac|ogg)$"],
    "excel": [r".*\.(xlsx)$"],
    "pdf": [r".*\.(pdf)$"],
}


def list_files_by_regex(start_folder, file_type_or_group):
    """
    List files in the given folder and subfolders filtered by a specific file type or logical group using regex.

    Parameters:
        start_folder (str): The root folder to start searching from.
        file_type_or_group (str or list): The file type(s) to filter by (e.g., "*.pdf")
                                          or a logical group (e.g., "images").

    Returns:
        list: A list of matching file paths.
    """
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
    print(f"Looking for {file_type_or_group} files in directory {start_folder}.")

    # Walk through the directory and subdirectories
    for root, dirs, files in os.walk(start_folder):
        for filename in files:
            if combined_pattern.match(filename):
                file_count += 1
                # matching_files.append(os.path.join(root, filename))
                match file_type_or_group:
                    case "images":
                        validate_image_with_pillow(f"{root}\\{filename}")
                    case "pdf":
                        validate_pdf(f"{root}\\{filename}")
                    case "videos":
                        x = validate_video_ffprobe(f"{root}\\{filename}")
                    case "excel":
                        validate_xlsx_openpyxl(f"{root}\\{filename}")
                    case _:
                        print("An undefined filetype is found")

    print(f"Total files read: {file_count}")


def validate_xlsx_openpyxl(file_path):
    from openpyxl import load_workbook

    try:
        # Try to open the workbook
        workbook = load_workbook(file_path)
        # Check if the workbook contains any sheets
        if not workbook.sheetnames:
            print(f"Invalid .xlsx file: {file_path} (No sheets found).")

        return
    except Exception as e:
        print(f"Invalid .xlsx file: {file_path} (Error: {e}).")


def validate_pdf(file_path):
    try:
        # Importing PyPDF2 inside the function
        from PyPDF2 import PdfReader

        # Open and validate the PDF
        reader = PdfReader(file_path)
        if not reader.pages:
            print(f"Invalid PDF: {file_path} (No pages found)")

    except Exception as e:
        print(f"Invalid PDF: {file_path} (Error: {e})")


def validate_video_moviepy(file_path):
    from moviepy import VideoFileClip

    try:
        clip = VideoFileClip(file_path)
        duration = clip.duration  # Check if the video has a valid duration
        clip.close()
        print(f"Valid video file. Duration: {duration:.2f} seconds.")

    except Exception as e:
        print(f"Invalid video file. Error: {e}")


def validate_video_opencv(file_path):
    import cv2

    try:
        cap = cv2.VideoCapture(file_path)
        if not cap.isOpened():
            print(f"Invalid video file (cannot be opened): {file_path}.")

        # Check if the video has at least one frame
        ret, _ = cap.read()
        cap.release()
        if not ret:
            print(f"Invalid video file (no frames found): {file_path}.")
    except Exception as e:
        print(f"Error validating video file: {file_path} - {e}")



def validate_video_ffprobe(file_path):
    import subprocess

    print("PATH:", os.environ.get("PATH"))
    
    try:
        subprocess.check_output(["ffprobe", "-version"], stderr=subprocess.STDOUT)
        print("FFprobe is installed and accessible.")
    except FileNotFoundError:
        print("FFprobe is not installed or not in PATH.")

    try:
        # Run FFprobe command
        command = [
            "ffprobe",
            "-v", "error",  # Only show errors
            "-show_entries", "format=duration",  # Check for duration (validity indicator)
            "-of", "default=noprint_wrappers=1:nokey=1",
            file_path
        ]
        print(f"Command is: {command}")
        subprocess.check_output(command, stderr=subprocess.STDOUT)
        return {"valid": True, "error": None}
    except subprocess.CalledProcessError as e:
        # If FFprobe returns an error, the file is likely corrupted
        return {"valid": False, "error": e.output.decode().strip()}

def get_arguments(argv):
    arg_help = "{0} <directory> <filetype>".format(argv[0])

    try:
        arg_directory = (
            sys.argv[1]
            if len(sys.argv) > 1
            else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
        )
        arg_filetypes = sys.argv[2] if len(sys.argv) > 2 else "images"
    except:
        print(arg_help)
        sys.exit(2)

    print("directory:", arg_directory)
    print("filetypes:", arg_filetypes)

    return [arg_directory, arg_filetypes]


def validate_image_with_pillow(file_path):
    try:
        with Image.open(file_path) as img:
            img.verify()  # Verify that it is a valid image
        # return "Valid image file."
    except Exception as e:
        print(f"Invalid image file: {e}")


if __name__ == "__main__":
    arguments = get_arguments(sys.argv)
    print("directory: ", arguments[0])
    print("filetypes: ", arguments[1])

    # List files
    list_files_by_regex(arguments[0], arguments[1])

    # Print results
    # print(f"Found {len(matching_files)} matching files:")
    print("Analysis complete.")


# file_path = r"C:\Users\vszal\Downloads\IMG_2724.jpg"  # Use raw string for Windows paths

# print(validate_image_with_pillow(file_path))


# file_path = r"C:\Users\vszal\OneDrive\Pictures\5_27_22, 4_46 PM Microsoft Lens.jpg"
# print(validate_image_with_pillow(file_path))

# file_path = r"C:\Users\vszal\OneDrive\Pictures\The Sequel.pdf"
# print(validate_image_with_pillow(file_path))
