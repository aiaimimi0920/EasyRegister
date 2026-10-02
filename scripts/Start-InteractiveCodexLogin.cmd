@echo off
rtk proxy powershell.exe -NoLogo -NoProfile -File "%~dp0Start-InteractiveCodexLogin.ps1" %*
set "RESULT=%ERRORLEVEL%"
echo.
echo Exit code: %RESULT%
pause
exit /b %RESULT%
