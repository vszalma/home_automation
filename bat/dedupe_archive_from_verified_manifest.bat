@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\copy_master.bat

REM Set Python executable from virtual environment
set PYTHON_EXE=C:\python313\python.exe
set SCRIPT_PATH=c:\home_automation\

REM Change directory to project root so relative imports work
cd /d C:\home-automation\

REM Display what is being executed
echo Running dedupe_archive_from_verified_manifest.py script...
echo Python: %PYTHON_EXE%

REM Call script interactively (console remains open)
%PYTHON_EXE% %SCRIPT_PATH%dedupe_archive_from_verified_manifest.py --manifest D:\MediaArchive\verified_media.csv ^
  --archive-root D:\MediaArchive ^
  --quarantine-root D:\MediaArchive_quarantine ^
  --keep-out D:\MediaArchive\dedupe_keep.csv ^
  --dupes-out D:\MediaArchive\dedupe_dupes.csv ^
  --expected-run-id auto ^
  --state-file D:\MediaArchive\dedupe_state.json ^
  --limit 500 ^
  --scope year

pause
