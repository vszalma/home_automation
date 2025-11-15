@echo off
REM filepath: C:\Users\vszal\Documents\code\home_automation\bat\backup_master.bat

set LOGROOT=C:\home_automation\log
set LOGFILE=%LOGROOT%\backup_master_scheduled_bat.log
set DEBUGLOG=%LOGROOT%\debug.log
set PYTHON_EXE=C:\python313\python.exe

echo ==== New Scheduled Execution: %date% %time% ==== >> "%DEBUGLOG%"
echo Date: %date%, Time: %time% > "%LOGFILE%"
echo Running Python script from task scheduler... >> "%LOGFILE%"

%PYTHON_EXE% C:\home_automation\backup_master.py --source N:\ --destination E:\Backups --retry 10 >> %LOGFILE% 2>&1
