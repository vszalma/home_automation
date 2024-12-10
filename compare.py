import hashlib
import sys
import home_automation_common
import datetime
import structlog


def _calculate_file_hash(file_path, hash_algorithm="sha256"):
    try:
        # Create a hash object
        hash_func = hashlib.new(hash_algorithm)
        # Read the file in chunks to handle large files
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):  # Read in 8KB chunks
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except FileNotFoundError:
        raise Exception(f"File not found: {file_path}")
    except Exception as e:
        raise Exception(f"Error calculating hash: {e}")


def compare_files(file1, file2, hash_algorithm="sha256"):

    logger = structlog.get_logger()

    try:
        hash1 = _calculate_file_hash(file1, hash_algorithm)
        hash2 = _calculate_file_hash(file2, hash_algorithm)
        return hash1 == hash2
    except Exception as e:
        logger.error(f"An error occurred while comparing files", exception=e)
        return False


def get_arguments(argv):
    arg_help = "{0} <file1> <file2>".format(argv[0])

    try:
        file1 = (
            sys.argv[1]
            if len(sys.argv) > 1
            else '"C:\\Users\\vszal\\OneDrive\\Pictures"'
        )
        file2 = sys.argv[2] if len(sys.argv) > 2 else "image"
    except:
        print(arg_help)
        sys.exit(2)

    return [file1, file2]


if __name__ == "__main__":

    today = datetime.today().strftime("%Y-%m-%d")

    log_file = f"{today}_validation_log.txt"

    log_file = home_automation_common.get_full_filename("log", log_file)

    home_automation_common.configure_logging(log_file)

    logger = structlog.get_logger()

    arguments = get_arguments(sys.argv)
    logger.info("Files to be compared.", file1=arguments[0], file2=arguments[1])

    if compare_files(arguments[0], arguments[1]):
        logger.info("The files are identical.")
    else:
        logger.info("The files are different.")

    # home_automation_common.send_email(
    #     subject="Test Email from Python",
    #     body="This is a test email.",
    #     to_email="vszalma@hotmail.com",
    #     from_email="vszalma@hotmail.com",
    # )
