@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\backup_master.bat

set LOGROOT=C:\Users\vszal\Documents\code\home_automation\log
set LOGFILE=%LOGROOT%\backup_master_bat.log
set DEBUGLOG=%LOGROOT%\debug.log

echo ==== New Execution: %date% %time% ==== >> "%DEBUGLOG%"

set PYTHON_EXE=C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe

REM Redirect info to log
@REM echo Python exe: %PYTHON_EXE% >> "%DEBUGLOG%"
@REM echo Source path check: >> "%DEBUGLOG%"

@REM if exist "N:\_testcopy" (
@REM     echo Source path exists. >> "%DEBUGLOG%"
@REM ) else (
@REM     echo Source path NOT found. >> "%DEBUGLOG%"
@REM )

echo Date: %date%, Time: %time% > "%LOGFILE%"
echo Running Python script... >> "%LOGFILE%"

"%PYTHON_EXE%" C:\Users\vszal\Documents\code\home_automation\backup_master.py --source "N:\_testcopy" --destination F:\backups >> "%LOGFILE%" 2>&1
