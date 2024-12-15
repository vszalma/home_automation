import os
from pathlib import Path
from datetime import datetime
import sys
import home_automation_common
import structlog
import csv


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

def _output_file_name(file_extension):

    sanitized_name = home_automation_common.sanitize_filename(arguments[2])

    output_file = (
        f"{datetime.now().date()}-compare-by-type-output-{arguments[2]}.csv"
    )

    output_file = home_automation_common.get_full_filename("output", output_file)

    return output_file

# def _output_file_info(directory, file_info):
#     # Print a summary of the results

#     sanitized_name = home_automation_common.sanitize_filename(arguments[2])

#     output_file = (
#         f"{datetime.now().date()}-compare-by-type-output-{arguments[2]}.csv"
#     )

#     output_file = home_automation_common.get_full_filename("output", output_file)

#     with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
#         writer = csv.writer(csvfile)
#         writer.writerow(["file_type", "count", "total_size"])  # Write the headers

#         for file_type, stats in sorted(file_info.items()):
#             writer.writerow([file_type, stats["count"], stats["size"]])

#     return output_file

# def _process_results(results):
#         # Print results
#         print("Missing in Root 1:")
#         print("\n".join(results["missing_in_root1"]))

#         print("\nMissing in Root 2:")
#         print("\n".join(results["missing_in_root2"]))

#         print("\nMismatched Files:")
#         for mismatch in results["mismatched_files"]:
#             print(mismatch)

        
#         sanitized_name = home_automation_common.sanitize_filename(arguments[2])

#         output_file = (
#             f"{datetime.now().date()}-compare-by-type-output-{sanitized_name}.csv"
#         )

#         with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
#             writer = csv.writer(csvfile)
#             writer.writerow(["root","file_path", "file_size", "date_modified", "root", "file_path", "count", "file_size", "date_modified"])  # Write the headers

#             for file_type, stats in sorted(file_info.items()):
#                 writer.writerow([file_type, stats["count"], stats["size"]])


#         _output_file_info()

#         with open("comparison_results.log", "a", encoding="utf-8") as log_file:
#             # log_file.write("Missing in Root 1:\n")
#             for missing1 in results["missing_in_root1"]:
                
#             if results["missing_in_root1"]:
#                 log_file.write("\n".join(results["missing_in_root1"]) + "\n")
#             else:
#                 log_file.write("None\n")

#             log_file.write("Missing in Root 2:\n")
#             if results["missing_in_root2"]:
#                 log_file.write("\n".join(results["missing_in_root2"]) + "\n")
#             else:
#                 log_file.write("None\n")




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
        logger.info("Differences found.", module="compare_file_by_type",message=f"Comparison results saved to {output_file}")
    else:
        logger.info("No Differences were found.", module="compare_file_by_type")
        os.remove(output_file)




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
        logger.info("Comparison starting.", module="compare_file_by_type")
        # Validate files
        output_file_name = _output_file_name(arguments[2])
        compare_file_structures(
            root1=arguments[0],
            root2=arguments[1],
            file_extension=arguments[2],
            output_file=output_file_name,
        )
        # results = compare_file_structures(arguments[0], arguments[1], arguments[2])
        # if results["missing_in_root1"] | results["missing_in_root2"] | results["mismatched_files"]:
        #     _process_results(results)
        #     logger.info("Differences found.", module="compare_file_by_type", message="Differences written to output file.")
        # else:
        #     logger.info("No differences found.", module="compare_file_by_type")
    logger.info("Comparison completed.", module="compare_file_by_type")

