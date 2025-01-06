import os
from pathlib import Path
from datetime import datetime
import sys
import home_automation_common
import structlog
import csv
import argparse

""" 
    This script is designed to compare two directory structures for differences in files of a specified type
    and generate a report in a CSV file.
"""

def _get_arguments():
    """
    Parses command-line arguments for comparing two directories for differences in files/structures of a given file type.
    Returns:
        argparse.Namespace: Parsed command-line arguments with the following attributes:
            directory1 (str): Path to the first directory to compare.
            directory2 (str): Path to the second directory to compare.
            filetype (str): The file type to search for differences in the two file structures. Defaults to ".jpg".
    """
    parser = argparse.ArgumentParser(
        description="Compare 2 directories for differences in files/structures of a given file type."
    )

    # Add named arguments
    parser.add_argument(
        "--directory1",
        "-d1",
        type=str,
        required=True,
        help="Path to first directory to compare with file structure of directory2.",
    )
    parser.add_argument(
        "--directory2",
        "-d2",
        type=str,
        required=True,
        help="Path to second directory to compare with file structure of directory1.",
    )
    parser.add_argument(
        "--filetype",
        "-f",
        type=str,
        default=".jpg",
        help="The filetype to search for differences in the 2 file structures.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a dictionary (or list if preferred)
    return args


def _output_file_name(file_extension):
    """
    Generates an output file name based on the given file extension.
    The function sanitizes the file extension, creates a file name with the current date,
    and appends the sanitized file extension. The file name is then converted to a full
    path in the "output" directory.
    Args:
        file_extension (str): The file extension to be included in the output file name.
    Returns:
        str: The full path of the output file name.
    """

    sanitized_extension = file_extension.replace(".", "")

    output_file = (
        f"{datetime.now().date()}-compare-by-type-output-{sanitized_extension}.csv"
    )

    output_file = home_automation_common.get_full_filename("output", output_file)

    return output_file



def compare_file_structures(root1, root2, file_extension, output_file):
    """
    Compare two folder structures for differences in files of a given type.

    Parameters:
        root1 (str): Path to the first root directory.
        root2 (str): Path to the second root directory.
        file_extension (str): File extension to filter by (e.g., '.txt').
        output_file (str): Path to the output CSV file.

    Returns:
        None: Writes the results directly to a CSV file.
    """
    # Normalize paths
    root1, root2 = Path(root1), Path(root2)

    # Set switch to determine if any differences were found
    differences_found = False

    # Helper function to build a file dictionary
    def build_file_dict(root):
        file_dict = {}
        exclusions = home_automation_common.get_exclusion_list("collector", None)

        # Add the \\?\ prefix to the root directory
        if os.name == "nt":  # Only apply on Windows
            root = r"\\?\\" + os.path.abspath(root)

        for dirpath, dirs, filenames in os.walk(root):
            dirs[:] = [d for d in dirs if d not in exclusions]
            for filename in filenames:
                filename_extension = os.path.splitext(filename)[1].lower()
                if filename_extension == file_extension.lower():
                    full_path = Path(dirpath) / filename
                    relative_path = full_path.relative_to(root)
                    file_info = {
                        "path": str(full_path),
                        "root": str(root),
                        "size": full_path.stat().st_size,
                        "modified": datetime.fromtimestamp(full_path.stat().st_mtime),
                    }
                    file_dict[relative_path] = file_info
        return file_dict

    # Build file dictionaries for both roots
    files_in_root1 = build_file_dict(root1)
    files_in_root2 = build_file_dict(root2)

    # Prepare for CSV output
    headers = [
        "file1_root",
        "file1_relative_path",
        "file1_size",
        "file1_modified_date",
        "file2_root",
        "file2_relative_path",
        "file2_size",
        "file2_modified_date",
    ]

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

        # Compare files
        all_files = set(files_in_root1.keys()).union(files_in_root2.keys())
        for relative_path in all_files:
            file1_info = files_in_root1.get(relative_path, {})
            file2_info = files_in_root2.get(relative_path, {})

            row = [
                file1_info.get("root", ""),
                str(relative_path) if file1_info else "missing",
                file1_info.get("size", 0),
                file1_info.get("modified", ""),
                file2_info.get("root", ""),
                str(relative_path) if file2_info else "missing",
                file2_info.get("size", 0),
                file2_info.get("modified", ""),
            ]
            # Write only if files differ
            if not (
                file1_info
                and file2_info
                and file1_info["size"] == file2_info["size"]
                and file1_info["modified"] == file2_info["modified"]
            ):
                writer.writerow(row)
                differences_found = True
    if differences_found:
        logger.info("Differences found.", module="compare_file_by_type.compare_file_structures",message=f"Comparison results saved to {output_file}")
    else:
        logger.info("No Differences were found.", module="compare_file_by_type.compare_file_structures")
        os.remove(output_file)


if __name__ == "__main__":

    today = datetime.now().date()

    log_file = f"{today}_compare_file_by_type_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    args = _get_arguments()

    logger.info("Comparison starting.", module="compare_file_by_type.__main__")

    output_file_name = _output_file_name(args.filetype)
    compare_file_structures(
        root1=args.directory1,
        root2=args.directory2,
        file_extension=args.filetype,
        output_file=output_file_name,
    )
    logger.info("Comparison completed.", module="compare_file_by_type.__main__")

