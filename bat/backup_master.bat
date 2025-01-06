@echo off

    @REM The script backs up a source directory to a destination directory.
    @REM It checks whether a backup is necessary by comparing the source to the most recent backup.
    @REM If a backup is needed, it performs the backup using robocopy, validates the results, and logs the outcome.
    @REM Notifications (e.g., success or failure) are sent via email.

    @REM Arguments:
    @REM     --source (str): Path to the source directory to process.
    @REM     --destination (str): Path to the destination directory to process.

C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe ^
    C:\Users\vszal\Documents\code\home_automation\backup_master.py --source F:\ --destination D:\backups  ^
    > C:\Users\vszal\Documents\code\home_automation\log\backup_master_bat.log 2>&1
