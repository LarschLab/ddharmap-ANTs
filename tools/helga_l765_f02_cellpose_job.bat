@echo off
setlocal

set "NAS_USER=danin.dharmaperwira@unil.ch"
set "NAS_SHARE=\\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch"
set "NAS_DRIVE=Y:"
set "FISH_ID=L765_f02"
set "DATA_ROOT=Y:\default\D2c\07_Data"
set "FISH_PARENT=%DATA_ROOT%\Danin\Microscopy"
set "FISH_DIR=%FISH_PARENT%\%FISH_ID%"
set "EX_VIVO_STACK=%FISH_DIR%\01_raw\2p\anatomy\L765_02_anatomy_ex_vivo_00001.tif"
set "MANUAL_EX_VIVO_STACK=%FISH_DIR%\02_reg\00_preprocessing\2p_anatomy\ex_vivo\L765_f02_exvivo_anatomy_2P_GCaMP_uint8_manual_oriented.nrrd"
set "CODE_DIR=C:\Users\zebrafish\codeants_jobs\codeANTs_l765_f02"
set "PYTHON_EXE=C:\Users\zebrafish\.conda\envs\cellpose\python.exe"
set "ANAT_MODEL=C:\Users\zebrafish\.cellpose\models\2P_anat_cpsam_20260126_124615"
set "HCR_MODEL=C:\Users\zebrafish\.cellpose\models\HCR_cpsam_20251006_094039"
set "LOG_DIR=C:\Users\zebrafish\codeants_jobs\logs\L765_f02_cellpose"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
del "%LOG_DIR%\*.log" >nul 2>nul

echo [Helga Cellpose] Removing any stale %NAS_DRIVE% mapping.
net use %NAS_DRIVE% /delete /yes >nul 2>nul

echo [Helga Cellpose] Mapping %NAS_DRIVE% to %NAS_SHARE%.
echo [Helga Cellpose] Enter the password for %NAS_USER% when prompted.
net use %NAS_DRIVE% "%NAS_SHARE%" /user:%NAS_USER% * /persistent:no
if errorlevel 1 goto fail

echo [Helga Cellpose] Verifying target fish path.
dir "%FISH_DIR%"
if errorlevel 1 goto fail

echo [Helga Cellpose] Verifying expected inputs.
dir "%EX_VIVO_STACK%"
if errorlevel 1 goto fail
dir "%MANUAL_EX_VIVO_STACK%"
if errorlevel 1 goto fail
dir "%FISH_DIR%\02_reg\00_preprocessing\rbest\L765_f02_rbest_channel2_sst1_1.nrrd"
if errorlevel 1 goto fail
dir "%FISH_DIR%\02_reg\00_preprocessing\rbest\L765_f02_rbest_channel3_pth2.nrrd"
if errorlevel 1 goto fail

cd /d "%CODE_DIR%"
if errorlevel 1 goto fail
set "PYTHONPATH=%CODE_DIR%\src"

echo [Helga Cellpose] Segmenting manual-oriented ex vivo anatomy with Cellpose.
"%PYTHON_EXE%" tools\single_fish_pipeline.py segment-ex-vivo-anatomy-cellpose --fish-id %FISH_ID% --local-root "%FISH_PARENT%" --strict --write-manifest --anatomy-stack-path "%MANUAL_EX_VIVO_STACK%" --anat-cp-model-path "%ANAT_MODEL%" --use-gpu > "%LOG_DIR%\02_segment_ex_vivo_anatomy_cellpose.log" 2>&1
if errorlevel 1 goto fail

echo [Helga Cellpose] Segmenting rbest HCR intensity stacks with Cellpose.
"%PYTHON_EXE%" tools\single_fish_pipeline.py segment-hcr-cellpose --fish-id %FISH_ID% --local-root "%FISH_PARENT%" --strict --write-manifest --hcr-source rbest --cp-hcr-model-path "%HCR_MODEL%" --use-gpu > "%LOG_DIR%\03_segment_hcr_cellpose.log" 2>&1
if errorlevel 1 goto fail

echo [Helga Cellpose] Output inventory.
dir "%FISH_DIR%\03_analysis\structural\ex_vivo" /s > "%LOG_DIR%\04_output_inventory.log" 2>&1
dir "%FISH_DIR%\03_analysis\confocal\raw\cp_masks" >> "%LOG_DIR%\04_output_inventory.log" 2>&1
type "%LOG_DIR%\04_output_inventory.log"

echo [Helga Cellpose] Removing temporary %NAS_DRIVE% mapping.
net use %NAS_DRIVE% /delete /yes >nul 2>nul
echo [Helga Cellpose] Done. Logs: %LOG_DIR%
exit /b 0

:fail
echo [Helga Cellpose] ERROR: job failed. Logs: %LOG_DIR%
if exist "%LOG_DIR%\02_segment_ex_vivo_anatomy_cellpose.log" type "%LOG_DIR%\02_segment_ex_vivo_anatomy_cellpose.log"
if exist "%LOG_DIR%\03_segment_hcr_cellpose.log" type "%LOG_DIR%\03_segment_hcr_cellpose.log"
net use %NAS_DRIVE% /delete /yes >nul 2>nul
exit /b 1
