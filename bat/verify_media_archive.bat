@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\copy_master.bat

REM Set Python executable from virtual environment
set PYTHON_EXE=C:\python313\python.exe
set SCRIPT_PATH=c:\home_automation\

REM Change directory to project root so relative imports work
cd /d C:\home-automation\

REM Display what is being executed
echo Running validate_media_archive.py script...
echo Python: %PYTHON_EXE%

REM Call script interactively (console remains open)
%PYTHON_EXE% %SCRIPT_PATH%verify_media_archive.py --input-csv D:\MediaArchive\2026-01-20-organize-media-report.csv --verified-out D:\MediaArchive\verified_media.csv --unverified-out D:\MediaArchive\unverified_media.csv --state-file D:\MediaArchive\verify_state.json --limit 2000

pause

