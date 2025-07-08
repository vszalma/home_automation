@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\detect_duplicates.bat

REM Set Python executable from virtual environment
set PYTHON_EXE=C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe

REM Change directory to project root so relative imports work
cd /d C:\Users\vszal\Documents\code\home_automation

REM Display what is being executed
echo Running detect_duplicates.py script...
echo Python: %PYTHON_EXE%

REM Call script interactively (console remains open)
%PYTHON_EXE% detect_duplicates.py --input "C:\Users\vszal\Documents\code\home_automation\output\2025-07-07_gather_inventory_output.csv" --output "detect_duplicates.csv"

pause
