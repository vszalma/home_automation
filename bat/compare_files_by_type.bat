@echo off

    @REM Arguments:
    @REM     directory1 (str): Path to the first directory to compare.
    @REM     directory2 (str): Path to the second directory to compare.
    @REM     filetype (str): The file type to search for differences in the two file structures. Defaults to ".jpg".

set LOGROOT=C:\home_automation\log
set LOGFILE=%LOGROOT%\compare_files_by_type_bat.log
set DEBUGLOG=%LOGROOT%\debug.log
set PYTHON_EXE=C:\python313\python.exe

%PYTHON_EXE% C:\home_automation\compare_files_by_type.py --directory1 E:\Backups --directory2  N:\ --filetype .db >> %LOGFILE% 2>&1
