import logging
import structlog
import os
from mailersend import emails


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

        logger.info(f"Email sent to {recipients}")

    except Exception as e:
        logger.error("Error sending email.", exception=e)
