@echo off
setlocal

set "NAS_USER=danin.dharmaperwira@unil.ch"
set "NAS_SHARE=\\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch"
set "NAS_DRIVE=Y:"
set "TARGET_FISH=Y:\default\D2c\07_Data\Danin\Microscopy\L765_f02"

echo [Helga NAS] Removing any stale %NAS_DRIVE% mapping.
net use %NAS_DRIVE% /delete /yes >nul 2>nul

echo [Helga NAS] Mapping %NAS_DRIVE% to %NAS_SHARE%.
echo [Helga NAS] Enter the password for %NAS_USER% when prompted.
net use %NAS_DRIVE% "%NAS_SHARE%" /user:%NAS_USER% * /persistent:no
if errorlevel 1 (
    echo [Helga NAS] ERROR: Failed to map %NAS_DRIVE%.
    exit /b 1
)

echo [Helga NAS] Verifying target fish path:
dir "%TARGET_FISH%"
set "STATUS=%ERRORLEVEL%"

echo [Helga NAS] Removing temporary %NAS_DRIVE% mapping.
net use %NAS_DRIVE% /delete /yes >nul 2>nul

exit /b %STATUS%
