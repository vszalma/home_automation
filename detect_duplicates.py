
"""Detect duplicate files based on size, partial hash, and optionally full hash."""

import pandas as pd
from collections import defaultdict
import hashlib
from pathlib import Path
import logging
import time
from tqdm import tqdm
import argparse
import home_automation_common
from datetime import datetime
import structlog

def _get_arguments():
    """
    Parses command-line arguments for .
    Returns:
        argparse.Namespace: A namespace object containing the parsed arguments.
    Arguments:
        --input, -i (str, required): Path to the input file (output from gather_inventory.py).
        --output, -o (str, required): Output file to write deduplicated results to..
    """
    parser = argparse.ArgumentParser(
        description="Process a directory and file type for file operations."
    )

    # Add named arguments
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        required=True,
        help="Path to the input file (output from gather_inventory.py).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        required=True,
        help="Output file to write deduplicated results to.",
    )
    parser.add_argument(
        "--fullhash",
        "-f",
        type=bool,
        required=False,
        default=True,
        help="Use full for comparison. Defaults to True.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Return arguments as a namespace object
    return args

def compute_full_hash(filepath):
    BUF_SIZE = 65536
    sha256 = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha256.update(data)
        return sha256.hexdigest()
    except Exception as e:
        logging.warning(f"Error computing full hash for {filepath}: {e}")
        return None

def mark_duplicates(df, use_full_hash=True):
    df["duplicate_status"] = "not duplicate"
    if use_full_hash:
        df["full_hash"] = ""

    groups_by_size = df.groupby("size")

    for size, group in tqdm(groups_by_size, desc="Processing size groups"):
        if len(group) <= 1:
            continue

        for phash, phash_group in group.groupby("partial_hash"):
            if len(phash_group) <= 1:
                continue

            hash_map = defaultdict(list)

            for idx, row in phash_group.iterrows():
                path = row["path"]
                if use_full_hash:
                    full_hash = compute_full_hash(path)
                    if not full_hash:
                        continue
                    hash_map[full_hash].append(idx)
                    df.at[idx, "full_hash"] = full_hash
                else:
                    hash_map[phash].append(idx)

            for dup_list in hash_map.values():
                if len(dup_list) > 1:
                    df.at[dup_list[0], "duplicate_status"] = "duplicate keep"
                    for dup in dup_list[1:]:
                        df.at[dup, "duplicate_status"] = "duplicate delete"

    return df

def main():

    today = datetime.now().date()

    log_file = f"{today}_detect_duplicates_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    args = _get_arguments()

    logger = structlog.get_logger()
    logger.info(
        "Starting to detect duplicates.",
        module="detect_duplicates.main",
        message=f"Detecting duplicates in file {args.input} and writing to {args.output}.",
    )

    
    INPUT_CSV = args.input
    if not Path(INPUT_CSV).is_file():
        logger.error(
        "Invalid input file.",
        module="detect_duplicates.main",
        message=f"Input file {INPUT_CSV} does not exist.",
        )
        return
    
    log_file = f"{today}_detect_duplicates_log.txt"
    output_csv = f"{today}_{args.output}"
    OUTPUT_CSV = home_automation_common.get_full_filename("output", output_csv)

    if not OUTPUT_CSV.endswith('.csv'):
        logger.error(
        "Invalid output file name.",
        module="detect_duplicates.main",
        message=f"Output file {INPUT_CSV} must have a .csv extension.",
        )
        return
    
    USE_FULL_HASH = args.fullhash

    start_time = datetime.now().time()

    logger.info(
        "Loading input CSV file.",
        module="detect_duplicates.main",
        message=f"Loading file {INPUT_CSV}.",
    )

    df = pd.read_csv(INPUT_CSV)

    logger.info(
        "Beginning search for duplicates.",
        module="detect_duplicates.main",
        message=f"Beginning search for duplicates in file {INPUT_CSV}.",
    )

    df = mark_duplicates(df, use_full_hash=USE_FULL_HASH)

    logger.info(
        "Completed search for duplicates.",
        module="detect_duplicates.main",
        message=f"Completed search. Writing output to file {OUTPUT_CSV}.",
    )
    df.to_csv(OUTPUT_CSV, index=False)

    end_time = datetime.now().time()
    duration = home_automation_common.duration_from_times(end_time, start_time)

    logger.info(
        f"Process completed.",
        module="detect_duplicates.main",
        message=f"Process completed. Output file written to {OUTPUT_CSV}",
        start_time=start_time,
        end_time=end_time,
        duration=duration,
    )

if __name__ == "__main__":
    main()
