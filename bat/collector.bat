@echo off

    @REM Arguments:
    @REM     --directory (str): Path to the directory to process.

C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe ^
    C:\Users\vszal\Documents\code\home_automation\collector.py --directory N:  ^
    > C:\Users\vszal\Documents\code\home_automation\log\collector_bat.log 2>&1
