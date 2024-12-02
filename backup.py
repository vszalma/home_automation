import subprocess
import sys


def _get_arguments(argv):
    arg_help = "{0} <source directory> <destination directory>".format(argv[0])

    try:
        arg_source = (
            sys.argv[1]
            if len(sys.argv) > 1
            else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
        )
        arg_destination = (
            sys.argv[2] if len(sys.argv) > 2 else "C:\\Users\\vszal\\OneDrive\\Pictures"
        )
    except:
        print(arg_help)
        sys.exit(2)

    return [arg_source, arg_destination]


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
            print(
                "Robocopy completed with warnings or skipped files. Check the log for details."
            )
        else:
            print("Robocopy encountered an error. Check the log for details.")

    except Exception as e:
        print(f"Error executing robocopy: {e}")


def execute_backup(source, destination):
    print("Backup is being run.")
    options = ["/E", "/MT:8", "/xo", "/nfl", "/ndl"]

    _run_robocopy(source, destination, options)


if __name__ == "__main__":

    arguments = _get_arguments(sys.argv)
    execute_backup(arguments[0], arguments[1])
