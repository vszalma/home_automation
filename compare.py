import argparse
import hashlib
import sys
import home_automation_common
from datetime import datetime
import structlog

""" 
    This script compares two files by calculating their cryptographic hashes and determining if they are identical.
"""

def _get_arguments():
    """
    Parses command-line arguments for file comparison.
    Returns:
        argparse.Namespace: A namespace object containing the parsed arguments.
    Arguments:
        --file1, -f1 (str, required): Path to the file to compare to file2.
        --file2, -f2 (str, required): Path to the file to be compared to file1.
    """
    parser = argparse.ArgumentParser(
        description="Process a directory and file type for file operations."
    )

    # Add named arguments
    parser.add_argument(
        "--file1",
        "-f1",
        type=str,
        required=True,
        help="Path to the file to compare to file2.",
    )
    parser.add_argument(
        "--file2",
        "-f2",
        type=str,
        required=True,
        help="Path to the file to be compared to file1.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a namespace object
    return args


def _calculate_file_hash(file_path, hash_algorithm="sha256"):
    """
    Calculate the hash of a file using the specified hash algorithm.

    Args:
        file_path (str): The path to the file for which the hash is to be calculated.
        hash_algorithm (str, optional): The hash algorithm to use (default is "sha256").

    Returns:
        str: The hexadecimal digest of the file's hash.

    Raises:
        Exception: If the file is not found or an error occurs during hash calculation.
    """
    try:
        # Create a hash object
        hash_func = hashlib.new(hash_algorithm)
        # Read the file in chunks to handle large files
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):  # Read in 8KB chunks
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except FileNotFoundError:
        raise Exception(f"File not found: {file_path}")
    except Exception as e:
        raise Exception(f"Error calculating hash: {e}")


def compare_files(file1, file2, hash_algorithm="sha256"):
    """
    Compare the hashes of two files to determine if they are identical.
    Args:
        file1 (str): The path to the first file.
        file2 (str): The path to the second file.
        hash_algorithm (str, optional): The hash algorithm to use for comparison. Defaults to "sha256".
    Returns:
        bool: True if the files have the same hash, False otherwise.
    Raises:
        Exception: If an error occurs during file comparison, it logs the error and returns False.
    """

    logger = structlog.get_logger()

    try:
        hash1 = _calculate_file_hash(file1, hash_algorithm)
        hash2 = _calculate_file_hash(file2, hash_algorithm)
        return hash1 == hash2
    except Exception as e:
        logger.error(
            "File comparison error.",
            module="compare._calculate_file_hash",
            message="An error occurred while comparing files",
            exception=e,
        )
        return False


if __name__ == "__main__":

    today = datetime.now().date()

    log_file = f"{today}_compare_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    args = _get_arguments()
    logger.info(
        "File comparison starting.",
        module="compare.__main__",
        message="Files to be compared.",
        file1=args.file1,
        file2=args.file2,
    )

    if compare_files(args.file1, args.file2):
        logger.info(
            "File comparison successful",
            module="compare.__main__",
            message="The files are identical.",
        )
    else:
        logger.warning(
            "File comparison failed.",
            module="compare.__main__",
            message="The files are different.",
        )
