@echo off
for /f %%i in ('"@INSTALL_PATH\spikex\bin\spikex.exe" status') do set SRV_STATUS=%%i
if not "%SRV_STATUS%"=="NonExistent" (
    "@INSTALL_PATH\spikex\bin\spikex.exe" uninstall
)
for /f %%i in ('"@INSTALL_PATH\spikex\bin\spikex.exe" status') do set SRV_STATUS=%%i
if "%SRV_STATUS%"=="NonExistent" (
    "@INSTALL_PATH\spikex\bin\spikex.exe" install
)