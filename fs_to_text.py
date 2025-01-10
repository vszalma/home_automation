import argparse
import os

def _get_arguments():
    """
    Parses command-line arguments for source and destination directories.
    Returns:
        argparse.Namespace: An object containing the parsed arguments:
            - source (str): Path to the source directory to process.
            - destination (str): Path to the destination directory to process.
    """
    parser = argparse.ArgumentParser(
        description="Process a directory and write the folder structure to a text file."
    )

    # Add named arguments
    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        required=True,
        help="Path to the directory to process.",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        type=str,
        help="Path to the destination file to write output.",
    )
    parser.add_argument(
        "--extensions",
        "-e",
        nargs="*",  # "*" is 0 or more, "+" means one or more values are required
        help="A list of file extensions to process.",
        required=False,  # Ensure the argument is provided
    )
    parser.add_argument(
        "--exclusions",
        "-x",
        help="A list of file extensions to process.",
        required=False,  # Ensure the argument is provided
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a dictionary (or list if preferred)
    return args

def write_folder_structure_with_content(start_path, output_file, exclusion_file=None, extensions=None):
    """
    Writes the folder and file structure to an output file, excluding specific folders
    and including content from files with specified extensions.

    Args:
        start_path (str): The root directory to start scanning.
        output_file (str): The path to the output text file.
        exclusion_file (str): Path to a file containing folder names to exclude.
        extensions (list of str): List of file extensions to include content for.
    """
    # Load exclusions from the exclusion file
    exclusion_file = "fs_to_text_exclusions.txt"
    exclusions = set()
    if exclusion_file and os.path.exists(exclusion_file):
        with open(exclusion_file, "r", encoding="utf-8") as f:
            exclusions = {line.strip() for line in f if line.strip()}

    # Prepare the extensions set
    extensions = set(ext.lower() for ext in extensions) if extensions else set()

    def traverse_folder(path, prefix=""):
        """
        Recursively traverses the folder structure and yields lines with file content for matching extensions.

        Args:
            path (str): The current directory path.
            prefix (str): The current prefix for visual indentation.

        Yields:
            str: Formatted lines representing the folder/file structure.
        """
        try:
            items = sorted(os.listdir(path))  # Sort items for consistent order
            for i, item in enumerate(items):
                item_path = os.path.join(path, item)
                is_last = i == len(items) - 1
                connector = "└──" if is_last else "├──"

                # Skip excluded folders
                if os.path.isdir(item_path) and item in exclusions:
                    continue

                if os.path.isdir(item_path):
                    yield f"{prefix}{connector} {item}/"
                    extension = "    " if is_last else "│   "
                    yield from traverse_folder(item_path, prefix + extension)
                else:
                    yield f"{prefix}{connector} {item}"
                    if extensions and os.path.splitext(item)[1].lower() in extensions:
                        yield from read_file_content(item_path, prefix + "    ")
        except PermissionError:
            yield f"{prefix}└── [Permission Denied]"

    def read_file_content(file_path, prefix):
        """
        Reads the content of a file and yields lines with proper indentation.

        Args:
            file_path (str): The path to the file.
            prefix (str): The prefix for indentation.

        Yields:
            str: Lines of the file content.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.readlines()
            yield f"{prefix}[Contents of {file_path}]:"
            for line in content:
                yield f"{prefix}    {line.strip()}"
        except Exception as e:
            yield f"{prefix}    [Error reading file: {e}]"

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(f"Folder structure for: {start_path}\n")
        file.write("=" * 40 + "\n")
        for line in traverse_folder(start_path):
            file.write(line + "\n")

args = _get_arguments()

# Example usage:
start_directory = args.directory
output_txt_file = args.output

write_folder_structure_with_content(start_directory, output_txt_file, None, args.extensions)

print(f"Folder structure written to {output_txt_file}")
