import hashlib
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from mailersend import emails


def calculate_file_hash(file_path, hash_algorithm="sha256"):
    """
    Calculate the hash of a file using a specified hash algorithm.

    Parameters:
        file_path (str): Path to the file.
        hash_algorithm (str): The hash algorithm to use (e.g., "md5", "sha256").

    Returns:
        str: The computed hash as a hexadecimal string.
    """
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
    """
    Compare two files based on their hash values.

    Parameters:
        file1 (str): Path to the first file.
        file2 (str): Path to the second file.
        hash_algorithm (str): The hash algorithm to use (e.g., "md5", "sha256").

    Returns:
        bool: True if the files are identical, False otherwise.
    """
    try:
        hash1 = calculate_file_hash(file1, hash_algorithm)
        hash2 = calculate_file_hash(file2, hash_algorithm)
        return hash1 == hash2
    except Exception as e:
        print(f"Error comparing files: {e}")
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


def send_outlook_email(subject, body, to_email, from_email, password):
    """
    Send an email using an Outlook.com account.

    Parameters:
        subject (str): Email subject.
        body (str): Email body.
        to_email (str): Recipient's email address.
        from_email (str): Sender's email address.
        password (str): Sender's email password.
    """
    try:

        mailer = emails.NewEmail()

        mail_body = {"Testing email from Python"}

        mail_from = {
            "name": "me",
            "email": "MS_eLLyva@trial-x2p034766dkgzdrn.mlsender.net",
            "password": "uq4qVb7vhIiX6P6y"
        }

        recipients = [
            {
                "name": "Victor",
                "email": "vszalma@hotmail.com"
            }
        ]

        mailer.set_mail_from(mail_from, mail_body)
        mailer.set_mail_to(recipients, mail_body)
        mailer.set_subject("Hello!", mail_body)
        mailer.set_html_content("Greetings from the team, you got this message through MailerSend.", mail_body)
        mailer.set_plaintext_content("Greetings from the team, you got this message through MailerSend.", mail_body)

        mailer.send(mail_body)
        print(f"Email sent to {to_email}")
        
    except Exception as e:
        print(f"Error sending email: {e}")





if __name__ == "__main__":
    arguments = get_arguments(sys.argv)
    print("File to be compared: ", arguments[0])
    print("File to be compared: ", arguments[1])

    if compare_files(arguments[0], arguments[1]):
        print("The files are identical.")
    else:
        print("The files are different.")

    send_outlook_email(
        subject="Test Email from Python",
        body="This is a test email sent using Outlook.com SMTP.",
        to_email="vszalma@hotmail.com",
        from_email="vszalma@hotmail.com",
        password="Ch1ck.C0r3a"
    )
