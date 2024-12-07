import os
import fnmatch

def find_files_with_pattern(start_folder, pattern, delete=False):
    matching_files = []

    for root, dirs, files in os.walk(start_folder):
        for file in files:
            if fnmatch.fnmatch(file, pattern):
                file_path = os.path.join(root, file)
                matching_files.append(file_path)

                if delete:
                    try:
                        os.remove(file_path)
                        print(f"Deleted: {file_path}")
                    except Exception as e:
                        print(f"Failed to delete {file_path}: {e}")

    return matching_files

# Example Usage
start_folder = "F:\\_bu-2024-11-16"
pattern = "~$*.xlsx"

# List files
matching_files = find_files_with_pattern(start_folder, pattern)
print(f"Matching files({len(matching_files)}):")
for file in matching_files:
    print(file)

# Delete files
# find_files_with_pattern(start_folder, pattern, delete=True)
