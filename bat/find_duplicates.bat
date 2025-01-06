@echo off

    @REM Arguments
    @REM     - directory (str): Path to the directory to process. Defaults to 'F:\\'.
    @REM     - filetype (str): Type of files to process (e.g., '.jpg'). Defaults to '.jpg'.

C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe ^
    C:\Users\vszal\Documents\code\home_automation\find_duplicates.py --directory D:\_bu-2024-11-16\_apps --filetype .jpg  ^
    > C:\Users\vszal\Documents\code\home_automation\log\find_duplicates_bat.log 2>&1
