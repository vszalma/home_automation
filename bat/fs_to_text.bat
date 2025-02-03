@echo off

    @REM Arguments
    @REM     - directory (str): Path to the directory to process. Defaults to 'F:\\'.
    @REM     - filetype (str): Type of files to process (e.g., '.jpg'). Defaults to '.jpg'.

C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe ^
    E:\vszalma\Source\Repos\home-automation\home_automation\fs_to_text.py --directory E:\vszalma\Source\Repos\MusicJournal --output MusicJournal.source.txt --extensions .cs .js .css .json .sln .html ^
    > E:\vszalma\Source\Repos\home-automation\home_automation\log\fs_to_text_bat.log 2>&1
