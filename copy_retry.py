import subprocess
import os
from pathlib import Path

def run_robocopy_for_file(file_path, source_root, destination_root, options=None):
    file_path = Path(file_path.strip())
    if not file_path.exists():
        print(f"Skipped missing file: {file_path}")
        return

    relative_path = file_path.relative_to(source_root)
    source_dir = file_path.parent
    destination_dir = destination_root / relative_path.parent
    destination_dir.mkdir(parents=True, exist_ok=True)

    command = ["robocopy", str(source_dir), str(destination_dir), file_path.name]
    if options:
        command.extend(options)

    print(f"Copying: {file_path}")
    result = subprocess.run(command, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode > 7:
        print(f"Failed to copy {file_path} (code {result.returncode})")

def main():
    failed_list_path = "failed_files.txt"
    source_root = Path("E:/vszalma/Downloads/_testcopy")  # UPDATE THIS
    destination_root = Path("Z:/RetryTest")  # UPDATE THIS

    options = ["/R:3", "/W:5", "/XO", "/NFL", "/NDL"]  # Optional robocopy options

    with open(failed_list_path, "r") as f:
        for line in f:
            if line.strip():
                run_robocopy_for_file(line.strip(), source_root, destination_root, options)

if __name__ == "__main__":
    main()
