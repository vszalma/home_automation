import collector
import compare
import backup
import os

def _backup_needed(source, destination):
        
    # Get most recent backup file location.
    backup_dir_list = list_and_sort_directories(destination)
    
    if not backup_dir_list:
        return True
    else:
        most_recent_backup = backup_dir_list[0]
    
    ret_destination, output_destination = collector.main(most_recent_backup)
    if ret_destination:
        print(f"output file: {output_destination}")

    ret_source, output_source = collector.main(source)
    if ret_source:
        print(f"output file: {output_source}")

    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            compare.send_email("Backup not run.", "There was no need to backup files as the content hasn't changed.")
            return False
        else:
            return True

def main():
    source = "C:\\Users\\vszal\\Downloads"
    destination = "C:\\Users\\vszal\\Downloads"
    
    # execute backup if needed.
    if _backup_needed(source, destination):
        backup.executebackup()

    # After backup checks to ensure backup was succesful(i.e. matches source file counts and sizes.)
    ret_source, output_source = collector.main(source)
    if ret_source:
        print(f"output file: {output_source}")
    
    ret_destination, output_destination = collector.main(destination)
    if ret_destination:
        print(f"output file: {output_destination}")
    
    # if different, run a backup
    if ret_source and ret_destination:
        if compare.compare_files(output_source, output_destination):
            compare.send_email("Succesful backup", "The files match")

def list_and_sort_directories(path):
    try:
        # List all directories one level deep
        all_items = os.listdir(path)
        directories = [d for d in all_items if os.path.isdir(os.path.join(path, d))]

        # Filter directories matching the format 'BU-YYYYMMDD'
        filtered_dirs = []
        for d in directories:
            if d.startswith("BU-") and len(d) == 11:  # Check basic format
                try:
                    # Extract YYYYMMDD and check if it is valid
                    date_part = d[3:]
                    int(date_part)  # Check if it is a valid number
                    filtered_dirs.append((d, date_part))
                except ValueError:
                    continue

        # Sort by the extracted date part
        sorted_dirs = sorted(filtered_dirs, key=lambda x: x[1], reverse=True)

        # Return only the directory names
        return [d[0] for d in sorted_dirs]

    except Exception as e:
        print(f"An error occurred: {e}")
        return []

if __name__ == "__main__":
    main()