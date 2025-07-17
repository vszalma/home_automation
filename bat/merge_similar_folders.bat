@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\merge_similar_folders.bat

REM Set Python executable from virtual environment
set PYTHON_EXE=C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe

REM Change directory to project root so relative imports work
cd /d C:\Users\vszal\Documents\code\home_automation

REM Display what is being executed
echo Running merge_similar_folders.py script...
echo Python: %PYTHON_EXE%

REM Call script interactively (console remains open)
%PYTHON_EXE% merge_similar_folders.py  -i C:\Users\vszal\Documents\code\home_automation\output\2025-07-14_matching_folders_output.csv -d N:\work\msft\fydata --cleanup -a F:\similar-folder-archive
REM  --dry-run --cleanup -a C:\Users\vszal\Downloads\archive

pause
