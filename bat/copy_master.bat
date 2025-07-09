@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\copy_master.bat

REM Set Python executable from virtual environment
set PYTHON_EXE=C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe

REM Change directory to project root so relative imports work
cd /d C:\Users\vszal\Documents\code\home_automation

REM Display what is being executed
echo Running copy_master.py script...
echo Python: %PYTHON_EXE%

REM Call script interactively (console remains open)
%PYTHON_EXE% copy_master.py --source J:\ --destination N:\CathyJ

pause
