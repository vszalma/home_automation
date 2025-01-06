@echo off

    @REM Arguments:
    @REM     directory1 (str): Path to the first directory to compare.
    @REM     directory2 (str): Path to the second directory to compare.
    @REM     filetype (str): The file type to search for differences in the two file structures. Defaults to ".jpg".

C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe ^
    C:\Users\vszal\Documents\code\home_automation\compare_files_by_type.py --directory1 F:\path\to\file1.ext ^
                                                                           --directory2  F:\path\to\file2.ext --filetype .jpg ^
    > C:\Users\vszal\Documents\code\home_automation\log\compare_files_by_type_bat.log 2>&1
