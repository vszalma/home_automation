@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\copy_master.bat

REM Set Python executable from virtual environment
set PYTHON_EXE=C:\python313\python.exe
set SCRIPT_PATH=c:\home_automation\

REM Change directory to project root so relative imports work
cd /d C:\home-automation\

REM Display what is being executed
echo Running organize_media_by_date.py script...
echo Python: %PYTHON_EXE%

REM Call script interactively (console remains open)
%PYTHON_EXE% %SCRIPT_PATH%organize_media_by_date.py --source N:\CathyK --destination N:\MediaArchive --mode report

pause
