import logging
import re
import structlog
import os
from mailersend import emails
from datetime import datetime
from datetime import timedelta


def configure_logging(log_file_name):
    # Create a FileHandler for the log file
    file_handler = logging.FileHandler(log_file_name, mode="a")
    file_handler.setFormatter(logging.Formatter(fmt="%(message)s"))

    # Set up the root logger with the file handler
    logging.basicConfig(level=logging.INFO, handlers=[file_handler])

    # Configure structlog to use the logging system
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_full_filename(directory_name, file_name):
    # Get the directory where the script is located
    curr_dir = os.path.dirname(os.path.abspath(__file__))

    # Define the output file path relative to the script's directory
    output_file = os.path.join(curr_dir, directory_name, file_name)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    return output_file


def duration_from_times(start_time, end_time):
    date_today = datetime.today().date()
    start_datetime = datetime.combine(date_today, start_time)

    if end_time < start_time:
        end_datetime = datetime.combine(date_today + timedelta(days=1), end_time)
    else:
        end_datetime = datetime.combine(date_today, end_time)

    duration = (end_datetime - start_datetime)

    return duration



def send_email(subject, body):
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

        logger.info("Email sent.", module="home_automation_common", message=f"Email sent to {recipients}")

    except Exception as e:
        logger.error("Email failure.", module="home_automation_common", message="Error sending email.", exception=e)

def _normalize_path(path, directory=None):
    if directory:
        path = f"{directory}{path}"
        return os.path.normpath(path)
    else:
        return path


def sanitize_filename(directory):
    invalid_chars = r'[<>:"/\\|?*]'  # Windows-invalid characters
    sanitized = re.sub(invalid_chars, "", directory)
    sanitized = sanitized.strip()  # Remove leading and trailing spaces
    return sanitized


def get_exclusion_list(exclusion_type, start_folder=None):

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
                        _normalize_path(line.strip())
                        for line in f
                        if line.strip()
                    )
        except FileNotFoundError:
            logger.info(
                "No exclusions found.", module="home_automation_common", message=f"Exclusion file {exclusion_file} not found. Continuing without exclusions."
            )
    
    return exclusions