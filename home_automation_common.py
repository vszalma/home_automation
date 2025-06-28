import logging
import re
import structlog
import os
from mailersend import emails
from datetime import datetime, time
from datetime import timedelta
import shutil


def configure_logging(log_file_name, log_level=logging.INFO):
    """
    Configures logging for the application.
    This function sets up logging to both a file and the console. It also configures
    structlog for structured logging.
    Args:
        log_file_name (str): The name of the log file to write logs to.
        log_level (int, optional): The logging level to use. Defaults to logging.INFO.
    Returns:
        None
    """
    # Create handlers
    file_handler = logging.FileHandler(log_file_name, mode="a")
    file_handler.setFormatter(logging.Formatter(fmt="%(message)s"))
    file_handler.setLevel(log_level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(fmt="%(message)s"))
    console_handler.setLevel(log_level)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def create_logger(module_name="backup_master"):
    """
    Creates and configures a logger for the specified module.
    Args:
        module_name (str): The name of the module for which the logger is being created. Defaults to "backup_master".
    Returns:
        str: The full path to the log file.
    """
    today = datetime.now().date()  # .strftime("%Y-%m-%d")

    log_file = f"{today}_{module_name}_log.txt"

    log_file = get_full_filename("log", log_file)

    configure_logging(log_file)

    return log_file


def get_full_filename(directory_name, file_name):
    """
    Constructs the full file path for a given directory and file name, ensuring that the directory exists.
    Args:
        directory_name (str): The name of the directory relative to the script's location.
        file_name (str): The name of the file.
    Returns:
        str: The full file path.
    Raises:
        OSError: If the directory cannot be created.
    """
    # Get the directory where the script is located
    curr_dir = os.path.dirname(os.path.abspath(__file__))

    # Define the output file path relative to the script's directory
    output_file = os.path.join(curr_dir, directory_name, file_name)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    return output_file

def duration_from_times(start_time, end_time):
    """
    Calculate the duration between two times.
    Args:
        start_time (datetime.time): The start time.
        end_time (datetime.time): The end time.
    Returns:
        timedelta: The duration between the start and end times. If the end time is earlier than the start time, it is assumed to be on the following day.
    """
    # Ensure both are datetime.time objects
    if isinstance(start_time, str):
        start_time = datetime.strptime(start_time, "%H:%M:%S").time()
    if isinstance(end_time, str):
        end_time = datetime.strptime(end_time, "%H:%M:%S").time()

    date_today = datetime.today().date()
    start_datetime = datetime.combine(date_today, start_time)

    if end_time < start_time:
        end_datetime = datetime.combine(date_today + timedelta(days=1), end_time)
    else:
        end_datetime = datetime.combine(date_today, end_time)

    duration = end_datetime - start_datetime

    return duration

def send_email(subject, body):
    """
    Sends an email with the specified subject and body using mailersend SMTP service.
    Args:
        subject (str): The subject of the email.
        body (str): The body content of the email, can be in HTML or plaintext format.
    Raises:
        Exception: If there is an error sending the email, it will be logged and raised.
    Example:
        send_email("Test Subject", "<p>This is a test email body.</p>")
    """
    try:

        mailer = emails.NewEmail(
            "mlsn.011b4017977722058e2195d23680a996ed8e5204da1922fefe9e8e23a30bbc2f"
        )

        mail_body = {}

        mail_from = {
            "name": "me",
            "email": "MS_eLLyva@trial-x2p034766dkgzdrn.mlsender.net",
        }

        recipients = [
            {
                "name": "Victor Szalma",
                "email": "vszalma@hotmail.com",
            }
        ]

        reply_to = {
            "name": "Victor Szalma",
            "email": "vszalma@hotmail.com",
        }

        mailer.set_mail_from(mail_from, mail_body)
        mailer.set_mail_to(recipients, mail_body)
        mailer.set_subject(subject, mail_body)
        mailer.set_html_content(body, mail_body)
        mailer.set_plaintext_content(body, mail_body)
        mailer.set_reply_to(reply_to, mail_body)

        mailer.send(mail_body)

        logger = structlog.get_logger()

        logger.info(
            "Email sent.",
            module="home_automation_common.send_email",
            message=f"Email sent to {recipients}",
        )

    except Exception as e:
        logger.error(
            "Email failure.",
            module="home_automation_common.send_email",
            message="Error sending email.",
            exception=e,
        )


def _normalize_path(path, directory=None):
    """
    Normalize a given file path by optionally prepending a directory and normalizing the resulting path.

    Args:
        path (str): The file path to normalize.
        directory (str, optional): The directory to prepend to the path. Defaults to None.

    Returns:
        str: The normalized file path.
    """
    if directory:
        path = f"{directory}{path}"
        return os.path.normpath(path)
    else:
        return path


def sanitize_filename(directory):
    """
    Sanitize a directory name by removing invalid characters and trimming spaces.

    This function removes characters that are invalid in Windows file names
    (<>:"/\\|?*) and trims leading and trailing spaces from the input string.

    Args:
        directory (str): The directory name to sanitize.

    Returns:
        str: The sanitized directory name.
    """
    invalid_chars = r'[<>:"/\\|?*]'  # Windows-invalid characters
    sanitized = re.sub(invalid_chars, "", directory)
    sanitized = sanitized.strip()  # Remove leading and trailing spaces
    return sanitized


def get_exclusion_list(exclusion_type, start_folder=None):
    """
    Retrieve a set of exclusions from a file based on the given exclusion type.
    Args:
        exclusion_type (str): The type of exclusions to retrieve. This will be used to determine the filename.
        start_folder (str, optional): The starting folder to normalize paths against. Defaults to None.
    Returns:
        set: A set of normalized exclusion paths.
    Raises:
        FileNotFoundError: If the exclusion file does not exist, an info log is generated and an empty set is returned.
    """

    logger = structlog.get_logger()

    exclusion_file = f"{exclusion_type}_exclusions.txt"

    # Load exclusions
    exclusions = set()
    if exclusion_file:
        try:
            with open(exclusion_file, "r", encoding="utf-8") as f:
                # exclusions = set(line.strip() for line in f if line.strip())
                if start_folder:
                    exclusions = set(
                        _normalize_path(line.strip(), start_folder)
                        for line in f
                        if line.strip()
                    )
                else:
                    exclusions = set(
                        _normalize_path(line.strip()) for line in f if line.strip()
                    )
        except FileNotFoundError:
            logger.info(
                "No exclusions found.",
                module="home_automation_common.get_exclusion_list",
                message=f"Exclusion file {exclusion_file} not found. Continuing without exclusions.",
            )

    return exclusions

def calculate_enough_space_available(most_recent_backup, file_size_total):
    """
    Determines if there is enough free space available for a backup.

    Args:
        most_recent_backup (str): The path to the most recent backup.
        file_size_total (float): The total size of the files to be backed up.

    Returns:
        bool: True if there is enough free space available, False otherwise.
    """
    free_space = _get_free_space(most_recent_backup)
    return free_space > file_size_total * .01  # allow for a bit of buffer in file size.

def _get_free_space(file_path):
    """
    Get the available free space on the disk where the given file path is located.

    Args:
        file_path (str): The path to the file or directory to check the disk space for.

    Returns:
        int: The amount of free space in bytes.
    """
    # Get disk usage statistics
    total, used, free = shutil.disk_usage(file_path)
    return free
