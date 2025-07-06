@echo off
REM filepath: /c:/Users/vszal/Documents/code/home_automation/bat/backup_master.bat

REM Check if the script is running interactively (manually) or via Task Scheduler
if "%SESSIONNAME%"=="" (
    REM Running via Task Scheduler (no interactive session)
    set LOGFILE=C:\Users\vszal\Documents\code\home_automation\log\backup_master_bat.log

    REM Output date and time to the log file
    echo Date: %date%, Time: %time% > "%LOGFILE%"
    
    REM Redirect all output to the log file
    >> "%LOGFILE%" 2>&1 (
        C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe ^
        C:\Users\vszal\Documents\code\home_automation\backup_master.py --source N:\_testcopy --destination F:\backups
    )
) else (
    REM Running manually (interactive session)
    C:\Users\vszal\Documents\code\home_automation\.venv\Scripts\python.exe ^
    C:\Users\vszal\Documents\code\home_automation\backup_master.py --source N:\_testcopy --destination F:\backups
)