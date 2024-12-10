import logging
import structlog
import os

def configure_logging(log_file_name):
    # Create a FileHandler for the log file
    file_handler = logging.FileHandler(log_file_name, mode="a")
    file_handler.setFormatter(logging.Formatter(
        fmt="%(message)s"
    ))

    # Set up the root logger with the file handler
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler]
    )

    # Configure structlog to use the logging system
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
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
