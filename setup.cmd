@echo off
rem Runs setup.ps1 without changing the PowerShell execution policy for the machine.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup.ps1" %*
exit /b %errorlevel%
