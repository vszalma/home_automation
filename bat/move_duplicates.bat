@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\move_duplicates.bat

REM Set Python executable from virtual environment
set PYTHON_EXE=C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe

REM Change directory to project root so relative imports work
cd /d C:\Users\vszal\Documents\code\home_automation

REM Display what is being executed
echo Running gather_inventory.py script...
echo Python: %PYTHON_EXE%

REM Call script interactively (console remains open)
%PYTHON_EXE% move_duplicates.py --csv "C:\Users\vszal\Documents\code\home_automation\output\2025-07-07_detect_duplicates.csv" --archive-root "N:\_testcopy\archive"  --directory "N:\_testcopy"

pause
