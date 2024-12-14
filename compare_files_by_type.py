import os
from pathlib import Path
from datetime import datetime
import sys
import home_automation_common
import structlog


def _get_arguments(argv):
    arg_help = "{0} <directory> <filetype>".format(argv[0])

    try:
        arg_dir1 = (
            sys.argv[1]
            # if len(sys.argv) > 1
            # else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
        )
        arg_dir2 = (
            sys.argv[2]
            # if len(sys.argv) > 1
            # else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
        )
        arg_filetype = sys.argv[3] #if len(sys.argv) > 2 else "image"
    except:
        print(arg_help)
        sys.exit(2)

    return [arg_dir1, arg_dir2, arg_filetype]


def compare_file_structures(root1, root2, file_extension):
    """
    Compare two folder structures for differences in files of a given type.
    
    Parameters:
        root1 (str): Path to the first root directory.
        root2 (str): Path to the second root directory.
        file_extension (str): File extension to filter by (e.g., '.txt').
    
    Returns:
        dict: A dictionary summarizing missing files and mismatches.
    """
    # Normalize paths
    root1, root2 = Path(root1), Path(root2)
    differences = {"missing_in_root1": [], "missing_in_root2": [], "mismatched_files": []}
    excluded_dirs = {"$RECYCLE.BIN", "found.000"}
    
    # Helper function to build a file dictionary
    def build_file_dict(root):
        file_dict = {}
        for dirpath, dirs, filenames in os.walk(root):
            dirs[:] = [d for d in dirs if d not in excluded_dirs]
            for filename in filenames:
                if filename.endswith(file_extension):
                    full_path = Path(dirpath) / filename
                    relative_path = full_path.relative_to(root)
                    file_info = {
                        "size": full_path.stat().st_size,
                        "modified": full_path.stat().st_mtime,  # Timestamp in seconds
                    }
                    file_dict[relative_path] = file_info
        return file_dict

    # Build file dictionaries for both roots
    files_in_root1 = build_file_dict(root1)
    files_in_root2 = build_file_dict(root2)

    # Compare files
    all_files = set(files_in_root1.keys()).union(files_in_root2.keys())
    for relative_path in all_files:
        file1_info = files_in_root1.get(relative_path)
        file2_info = files_in_root2.get(relative_path)

        if file1_info is None:
            differences["missing_in_root1"].append(str(relative_path))
        elif file2_info is None:
            differences["missing_in_root2"].append(str(relative_path))
        else:
            # Compare size and modified time
            if file1_info["size"] != file2_info["size"] or abs(file1_info["modified"] - file2_info["modified"]) > 1:
                differences["mismatched_files"].append({
                    "file": str(relative_path),
                    "root1": {"size": file1_info["size"], "modified": datetime.fromtimestamp(file1_info["modified"])},
                    "root2": {"size": file2_info["size"], "modified": datetime.fromtimestamp(file2_info["modified"])},
                })

    return differences


# Example usage
# root_dir1 = "/path/to/root1"
# root_dir2 = "/path/to/root2"
# file_ext = ".txt"  # Replace with your desired file extension

# results = compare_file_structures(root_dir1, root_dir2, file_ext)



if __name__ == "__main__":

    today = datetime.now().date()

    log_file = f"{today}_compare_file_by_type_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    arguments = _get_arguments(sys.argv)

    if len(arguments) != 3:
        logger.error("Invalid arguments.", module="compare_file_by_type", message="Invalid arguments. Expected <directory> <directory> <file-extension>")
    else:
        # Validate files
        results = compare_file_structures(arguments[0], arguments[1], arguments[2])
        # Print results
        print("Missing in Root 1:")
        print("\n".join(results["missing_in_root1"]))

        print("\nMissing in Root 2:")
        print("\n".join(results["missing_in_root2"]))

        print("\nMismatched Files:")
        for mismatch in results["mismatched_files"]:
            print(mismatch)
