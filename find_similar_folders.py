
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import os
import argparse
import logging
import structlog
from datetime import datetime
from tqdm import tqdm
from rapidfuzz import fuzz

import home_automation_common

# def _read_exclusions(exclusion_file):
#     exclusions = set()
#     if exclusion_file and Path(exclusion_file).is_file():
#         with open(exclusion_file, 'r', encoding='utf-8') as f:
#             for line in f:
#                 cleaned = line.strip()
#                 if cleaned:
#                     exclusions.add(cleaned.lower())
#     return exclusions

def _get_arguments():
    parser = argparse.ArgumentParser(
        description="Search for folders with matching, similar, or fuzzy names, gathering metadata like file count and total size."
    )
    parser.add_argument(
        "--directory", "-d", type=str, required=True, help="Root directory to search."
    )
    parser.add_argument(
        "--match", "-m", type=str, required=True,
        help="Folder name to match. Use '*' as a wildcard suffix for prefix matches."
    )
    parser.add_argument(
        "--match-mode", "-t", type=str, choices=["exact", "prefix", "fuzzy"], default="prefix",
        help="Match type: exact, prefix, or fuzzy"
    )
    parser.add_argument(
        "--threshold", type=int, default=85,
        help="Threshold for fuzzy matching (only used if match-mode=fuzzy)"
    )
    # parser.add_argument(
    #     "--output", "-o", type=str, default="matching_folders_output.csv",
    #     help="CSV output file name."
    # )
    return parser.parse_args()

# def configure_logging(log_path):
#     logging.basicConfig(
#         filename=log_path,
#         level=logging.INFO,
#         format="%(message)s"
#     )
#     structlog.configure(
#         wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
#         logger_factory=structlog.stdlib.LoggerFactory(),
#     )

def _folder_matches(folder_name, match_pattern, mode, threshold):
    folder_name = folder_name.lower()
    match_pattern = match_pattern.lower()

    if mode == "exact":
        return folder_name == match_pattern
    elif mode == "prefix":
        if match_pattern.endswith("*"):
            return folder_name.startswith(match_pattern[:-1])
        return folder_name == match_pattern
    elif mode == "fuzzy":
        score = fuzz.ratio(folder_name, match_pattern)
        return score >= threshold
    return False

def _gather_folder_stats(folder_path):
    total_size = 0
    file_count = 0
    for file in folder_path.rglob('*'):
        if file.is_file():
            try:
                total_size += file.stat().st_size
                file_count += 1
            except Exception:
                continue
    return file_count, total_size

def _scan_folder(path, match_pattern, match_mode, threshold, exclusions):
    matching = []
    for sub in path.rglob("*"):
        if sub.is_dir() and sub.name.lower() not in exclusions and _folder_matches(sub.name, match_pattern, match_mode, threshold):
            file_count, total_size = _gather_folder_stats(sub)
            matching.append((str(sub), file_count, total_size))
    return matching

def main():
    args = _get_arguments()
    log_file = f"{datetime.now().date()}_matching_folders_log.txt"
    log_file = home_automation_common.get_full_filename("log", log_file)
    home_automation_common.configure_logging(log_file)
    logger = structlog.get_logger()

    root_path = Path(args.directory)
    if not root_path.is_dir():
        logger.error(
            "Folder does not exist",
            module="find_similar_folders.main",
            message=f"Invalid directory path ({root_path}) provided.",
        )
        return

    match_pattern = args.match
    match_mode = args.match_mode
    threshold = args.threshold
    core_count = max(1, (os.cpu_count() or 2) - 1)
    results = []

    folders_to_scan = [p for p in root_path.glob("*") if p.is_dir()]

    with ThreadPoolExecutor(max_workers=core_count) as executor:
        # exclusions = _read_exclusions(args.exclude_file)
        exclusions = home_automation_common.get_exclusion_list("collector")
        futures = {
            executor.submit(_scan_folder, folder, match_pattern, match_mode, threshold, exclusions): folder
            for folder in folders_to_scan
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Scanning folders"):
            try:
                result = future.result()
                results.extend(result)
            except Exception as e:
                logger.warning(
                    "Error scanning folder",
                    module="find_similar_folders.main",
                    message=f"Error scanning folder: {e}",
                )

    output_path = home_automation_common.get_full_filename("output", f"{datetime.now().date()}_matching_folders_output.csv")
    with output_path.open("w", encoding="utf-8") as f:
        f.write("folder_path,file_count,total_size_bytes\n")
        for path, count, size in results:
            f.write(f"{path},{count},{size}\n")

    logger.info("Scan completed.", total_matches=len(results), output_file=str(output_path))

if __name__ == "__main__":
    main()
