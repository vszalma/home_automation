
import os
from pathlib import Path
from datetime import datetime
import sys
import home_automation_common
import structlog
import csv
import argparse
from tqdm import tqdm

def _get_arguments():
    parser = argparse.ArgumentParser(
        description="Compare 2 directories for differences in files/structures of a given file type."
    )
    parser.add_argument("--directory1", "-d1", type=str, required=True, help="Path to first directory to compare.")
    parser.add_argument("--directory2", "-d2", type=str, required=True, help="Path to second directory to compare.")
    parser.add_argument("--filetype", "-f", type=str, default=".jpg", help="The filetype to search for differences. Use 'none' for files without an extension")
    return parser.parse_args()

def _output_file_name(file_extension):
    sanitized_extension = file_extension.replace(".", "")
    output_file = f"{datetime.now().date()}-compare-by-type-output-{sanitized_extension}.csv"
    return home_automation_common.get_full_filename("output", output_file)

def _pretty_path(path):
    path = str(path)
    if path.startswith("\\\\?\\UNC\\"):
        return "\\" + path[7:]  # Strip \\?\UNC and restore leading slash
    elif path.startswith("\\\\?\\"):
        return path[4:]  # Strip \\?\
    return path

def compare_file_structures(root1, root2, file_extension, output_file):
    root1, root2 = Path(root1), Path(root2)
    differences_found = False

    def build_file_dict(root):
        file_dict = {}
        exclusions = home_automation_common.get_exclusion_list("collector", None)
        if os.name == "nt":
            root = home_automation_common.normalize_path(root)
            # root = r"\\?\\" + os.path.abspath(root)
        with tqdm(desc=f"Scanning {_pretty_path(root)}", unit="dir") as pbar:
            for dirpath, dirs, filenames in os.walk(root):
                dirs[:] = [d for d in dirs if d not in exclusions]
                for filename in filenames:
                    filename_extension = os.path.splitext(filename)[1].lower()
                    if (file_extension.lower() == "none" and filename_extension == "") or \
                       (file_extension.lower() != "none" and filename_extension == file_extension.lower()):
                    # if os.path.splitext(filename)[1].lower() == file_extension.lower():
                        full_path = Path(dirpath) / filename
                        relative_path = full_path.relative_to(root)
                        file_info = {
                            "path": str(full_path),
                            "root": str(root),
                            "size": full_path.stat().st_size,
                            "modified": datetime.fromtimestamp(full_path.stat().st_mtime),
                        }
                        file_dict[relative_path] = file_info
                pbar.update(1)
        return file_dict

    files_in_root1 = build_file_dict(root1)
    files_in_root2 = build_file_dict(root2)

    headers = [
        "file1_root", "file1_relative_path", "file1_size", "file1_modified_date",
        "file2_root", "file2_relative_path", "file2_size", "file2_modified_date",
    ]

    all_files = set(files_in_root1.keys()).union(files_in_root2.keys())

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for relative_path in tqdm(all_files, desc="Comparing files", unit="file"):
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
            if not (
                file1_info and file2_info and
                file1_info["size"] == file2_info["size"] and
                file1_info["modified"] == file2_info["modified"]
            ):
                writer.writerow(row)
                differences_found = True

    if differences_found:
        logger.info("Differences found.", module="compare_file_by_type.compare_file_structures", message=f"Comparison results saved to {output_file}")
    else:
        logger.info("No Differences were found.", module="compare_file_by_type.compare_file_structures")
        os.remove(output_file)

if __name__ == "__main__":
    today = datetime.now().date()
    log_file = home_automation_common.get_full_filename("log", f"{today}_compare_file_by_type_log.txt")
    home_automation_common.configure_logging(log_file)
    logger = structlog.get_logger()
    args = _get_arguments()
    logger.info("Comparison starting.", module="compare_file_by_type.__main__")
    output_file_name = _output_file_name(args.filetype)
    compare_file_structures(args.directory1, args.directory2, args.filetype, output_file_name)
    logger.info("Comparison completed.", module="compare_file_by_type.__main__")
