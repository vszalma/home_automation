@echo off

@REM Arguments
@REM     - directory (str): Path to the directory to process. Defaults to 'F:\\'.
@REM     - filetype (str): Type of files to process (e.g., '.jpg'). Defaults to '.jpg'.

python ^
E:\vszalma\Source\Repos\home-automation\home_automation\fs_to_text.py ^
--directory E:\vszalma\Source\Repos\MusicJournal ^
--output E:\vszalma\Source\Repos\home-automation\home_automation\output\MusicJournal.source.txt ^
--exclusions E:\vszalma\Source\Repos\home-automation\home_automation\fs_to_text_exclusions.txt ^
--extensions .cs .js .css .json .sln .html .csproj .razor ^
> E:\vszalma\Source\Repos\home-automation\home_automation\log\fs_to_text_bat.log 2>&1
