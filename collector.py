import os
from collections import defaultdict
import csv
from datetime import datetime
import re

def main(directory):
    # directory = input("Enter the directory to scan: ")
    if os.path.isdir(directory):
        file_info = collect_file_info(directory)
        output_file = display_file_info(directory, file_info)
        return True, output_file
    else:
        return False, "Invalid directory. Please try again."

def collect_file_info(directory):
    # Dictionary to store file type information
    file_info = defaultdict(lambda: {'count': 0, 'size': 0})
    
    # Walk through the directory tree
    for root, _, files in os.walk(directory):
        for file in files:
            # Get file extension and size
            file_path = os.path.join(root, file)
            file_extension = os.path.splitext(file)[1].lower()  # Get the extension (case-insensitive)
            try:
                file_size = os.path.getsize(file_path)
            except OSError:
                continue  # Skip files that can't be accessed
            
            # Update dictionary
            file_info[file_extension]['count'] += 1
            file_info[file_extension]['size'] += file_size
    
    return file_info

def sanitize_filename(directory):
    # Define a regex to remove invalid characters for file names
    # Adjust as necessary for your operating system
    invalid_chars = r'[<>:"/\\|?*]'  # Windows-invalid characters
    sanitized = re.sub(invalid_chars, '', directory)
    sanitized = sanitized.strip()  # Remove leading and trailing spaces
    return sanitized

def display_file_info(directory, file_info):
    # Print a summary of the results
    
    sanitized_name = sanitize_filename(directory)

    output_file = f"{datetime.today().strftime('%Y-%m-%d')}-collector-output-{sanitized_name}.csv"

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["file_type", "count", "total_size"])  # Write the headers

        for file_type, stats in sorted(file_info.items()):
            writer.writerow([file_type, stats['count'], stats['size']])
            #print(f"{file_type:<15} {stats['count']:<10} {stats['size']:<20}")

    return output_file

""" if __name__ == "__main__":
        main() """