@echo off
rem Thin launcher for Windows (cmd.exe and PowerShell). Finds Python 3.8+ and runs lakehouse.py.
setlocal
where py >nul 2>nul || goto trypython
py -3 "%~dp0lakehouse.py" %*
exit /b

:trypython
where python >nul 2>nul || goto nopython
python "%~dp0lakehouse.py" %*
exit /b

:nopython
echo Python 3.8 or newer was not found. 1>&2
echo Run setup.cmd, or install it with:  winget install -e --id Python.Python.3.12 1>&2
exit /b 127
