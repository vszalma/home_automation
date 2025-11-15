@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\find_similar_folders.bat

REM Set Python executable from virtual environment
set PYTHON_EXE=C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe

REM Change directory to project root so relative imports work
cd /d C:\Users\vszal\Documents\code\home_automation

REM Display what is being executed
echo Running find_similar_folders.py script...
echo Python: %PYTHON_EXE%

REM Call script interactively (console remains open)
%PYTHON_EXE% find_similar_folders.py -d N:\ -m FY11 -t exact

pause
