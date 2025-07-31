@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\compare.bat

REM Set Python executable from virtual environment
set PYTHON_EXE=C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe

REM Change directory to project root so relative imports work
cd /d C:\Users\vszal\Documents\code\home_automation

REM Display what is being executed
echo Running compare.py script...
echo Python: %PYTHON_EXE%

REM Call script interactively (console remains open)
%PYTHON_EXE% compare.py --file1 "C:\Users\vszal\Documents\code\home_automation\output\2025-07-30-collector-output-NCathyJ.csv" --file2 "C:\Users\vszal\Documents\code\home_automation\output\2025-07-30-collector-output-E_tempCathyJ.csv"

pause
