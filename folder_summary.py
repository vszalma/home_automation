
import os
import csv
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configuration
ROOT_DIR = Path("D:/source")  # Change this to your actual source root
OUTPUT_FILE = "folder_summary.csv"
MAX_WORKERS = 8

# Setup logging
logging.basicConfig(
    filename="folder_summary.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def analyze_folder(folder_path: Path, root: Path):
    try:
        total_size = 0
        file_count = 0
        folder_count = 0
        file_types = set()

        for dirpath, dirnames, filenames in os.walk(folder_path):
            folder_count += len(dirnames)
            for file in filenames:
                file_path = Path(dirpath) / file
                try:
                    stat = file_path.stat()
                    total_size += stat.st_size
                    file_count += 1
                    file_types.add(file_path.suffix.lower())
                except Exception:
                    logging.warning(f"Unable to access file: {file_path}")
                    continue

        depth = len(folder_path.relative_to(root).parts)
        return {
            "folder": str(folder_path),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "file_count": file_count,
            "folder_count": folder_count,
            "depth": depth,
            "file_types": ", ".join(sorted(file_types))
        }

    except Exception as e:
        logging.error(f"Error analyzing folder {folder_path}: {e}")
        return None

def main():
    logging.info(f"Starting folder summary analysis on: {ROOT_DIR}")
    all_folders = [Path(dirpath) for dirpath, _, _ in os.walk(ROOT_DIR)]
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_folder, folder, ROOT_DIR): folder for folder in all_folders}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Analyzing folders"):
            result = future.result()
            if result:
                results.append(result)

    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["folder", "total_size_mb", "file_count", "folder_count", "depth", "file_types"])
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    logging.info(f"Folder summary written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
