@echo off

set PATH=c:\Users\vszal\.vscode\extensions\ms-python.python-2024.22.1-win32-x64\python_files\deactivate\powershell;%PATH%
set PATH=C:\Users\vszal\Documents\code\scripts\.venv\Scripts;%PATH%
set PATH=C:\Program Files\PowerShell\7;%PATH%
set PATH=c:\Users\vszal\.vscode\extensions\ms-python.python-2024.22.1-win32-x64\python_files\deactivate\powershell;%PATH%

echo %PATH% > C:\Users\vszal\task_scheduler_env.log
echo Starting batch file > C:\Users\vszal\Documents\logs\batch_debug.log
cd /d C:\Users\vszal\Documents\code\scripts
echo Set working directory > C:\Users\vszal\Documents\logs\batch_debug.log
echo Starting batch file2 >> C:\Users\vszal\Documents\logs\batch_debug.log
@REM C:\Users\vszal\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\python.exe C:\Users\vszal\Documents\code\scripts\simple.py >> C:\Users\vszal\batch_debug.log 2>&1
@REM C:\Users\vszal\AppData\Local\Microsoft\WindowsApps\python.exe C:\Users\vszal\Documents\code\scripts\simple.py >> C:\Users\vszal\batch_debug.log 2>&1
C:\Users\vszal\Documents\code\scripts\.venv\Scripts\python.exe C:\Users\vszal\Documents\code\scripts\simple.py >> C:\Users\vszal\Documents\logs\batch_debug.log 2>&1
@REM runas /user:vszal /password:Ch1ck.C0r3a C:\Users\vszal\Documents\code\scripts\.venv\Scripts\python.exe -m site >> C:\Users\vszal\batch_debug.log 2>&1
echo Completed batch file >> C:\Users\vszal\Documents\logs\batch_debug.log
exit 0