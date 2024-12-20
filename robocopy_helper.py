import subprocess
import sys
import structlog
import home_automation_common
from datetime import datetime
import os


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

    logger = structlog.get_logger()

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
            logger.info("Robocopy completed successfully.", module="robocopy_helper")
        elif result.returncode >= 1 and result.returncode <= 7:
            logger.warning(
                "Robocopy completed with warnings or skipped files.", module="robocopy_helper", message="Check the log for details."
            )
        else:
            logger.error("Robocopy encountered an error.", module="robocopy_helper", message="Check the log for details.")
        
        return True

    except Exception as e:
        logger.error("Error executing robocopy", module="robocopy_helper", message=e)
        return False


def execute_robocopy(source, destination, action="Backup"):

    logger = structlog.get_logger()

    start_time = datetime.now().time()

    output_file = (f"{datetime.now().date()}_robocopy_log.txt")

    output_file = home_automation_common.get_full_filename("log", output_file)

    if not os.path.exists(source):
        logger.error("File does not exist", module="robocopy_helper", message="Source file location does not exist", location=source)
        return False

    if not os.path.exists(destination):
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        os.makedirs(destination, exist_ok=True)
        # logger.error("File does not exist", module="robocopy_helper", message="Destination file location does not exist", location=destination)
        # return False

    logger.info(f"{action} running.", module="robocopy_helper", message="Backup is being run.")
    #options = ["/E", "/MT:8", "/XA:S", "/xo", "/nfl", "/ndl"]
    options = ["/E", "/MT:8", "/xo"]

    isCompleted =_run_robocopy(source, destination, options, output_file)

    if isCompleted:
        end_time = datetime.now().time()
        duration = home_automation_common.duration_from_times(end_time, start_time)
        logger.info("Backup completed.", module="robocopy_helper", message="Backup completed.", start_time=start_time, end_time=end_time, duration=duration, source=source, destination=destination)
        return True


if __name__ == "__main__":

    arguments = _get_arguments(sys.argv)

    today = datetime.now().date()

    log_file = f"{today}_backup_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()


    result = execute_robocopy(arguments[0], arguments[1], arguments[2])
