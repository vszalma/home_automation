@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\backup_master.bat

set LOGROOT=C:\Users\vszal\Documents\code\home_automation\log
set LOGFILE=%LOGROOT%\backup_master_bat.log
set DEBUGLOG=%LOGROOT%\debug.log
set PYTHON_EXE=C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe

echo ==== New Execution: %date% %time% ==== >> "%DEBUGLOG%"

REM Check if SESSIONNAME is empty (indicates non-interactive session, like Task Scheduler)
if "%SESSIONNAME%"=="" (
    echo Detected scheduled (non-interactive) execution. >> "%DEBUGLOG%"
    echo Date: %date%, Time: %time% > "%LOGFILE%"
    echo Running Python script... >> "%LOGFILE%"

    "%PYTHON_EXE%" C:\Users\vszal\Documents\code\home_automation\backup_master.py --source "N:\_testcopy" --destination F:\backups" >> "%LOGFILE%" 2>&1
) else (
    echo Detected interactive execution. >> "%DEBUGLOG%"
    "%PYTHON_EXE%" C:\Users\vszal\Documents\code\home_automation\backup_master.py --source "N:\_testcopy" --destination F:\backups"
)
