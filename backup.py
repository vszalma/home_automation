import subprocess

def _main(source, destination):
    print(f"Source: {source}")
    print(f"Destination: {destination}")
    #script1.main()  # Ensure each script has a main() or callable function


def _run_robocopy(source, destination, options=None, log_file="robocopy_log.txt"):
    try:
        # Construct the robocopy command
        command = ["robocopy", source, destination]
        if options:
            command.extend(options)

        # Open the log file for writing
        with open(log_file, "w") as log:
            result = subprocess.run(command, stdout=log, stderr=log, text=True)

        # Check the exit code
        if result.returncode == 0:
            print("Robocopy completed successfully.")
        elif result.returncode >= 1 and result.returncode <= 7:
            print("Robocopy completed with warnings or skipped files. Check the log for details.")
        else:
            print("Robocopy encountered an error. Check the log for details.")

    except Exception as e:
        print(f"Error executing robocopy: {e}")

# Example usage
#run_robocopy_with_logging("C:\\SourceFolder", "D:\\DestinationFolder", options=["/E", "/MT:8"])


def executebackup(source, destination):
    print("Backup is being run.")
    options = ["/E", "/MT:8", "/xo", "/nfl", "/ndl"]
    
    _run_robocopy(source, destination, options)

if __name__ == "__main__":
    _main()