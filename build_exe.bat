@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

echo.
echo ================================================
echo   DB Importer - Build EXE
echo ================================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Please install Python 3.9+ and add to PATH.
    pause
    exit /b 1
)
for /f "tokens=*" %%v in ('python --version 2^>^&1') do echo [OK] %%v detected

:: Activate virtualenv if present
if exist ".venv\Scripts\activate.bat" (
    echo [OK] Activating .venv
    call .venv\Scripts\activate.bat
) else if defined CONDA_DEFAULT_ENV (
    echo [OK] conda env: %CONDA_DEFAULT_ENV%
) else (
    echo [  ] No virtualenv detected, using system Python
)

:: ---- Step 1: Install dependencies ------------------------------------------
echo.
echo [1/4] Installing dependencies (Tsinghua mirror)...
echo.

pip install pyinstaller chardet -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 (
    echo [ERROR] Failed to install base dependencies.
    pause
    exit /b 1
)

pip install xlsxwriter openpyxl -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 ( echo [WARN] xlsxwriter/openpyxl install failed. Excel export unavailable. )

pip install mysql-connector-python -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 ( echo [WARN] mysql-connector-python install failed. MySQL unavailable. )

:: oracledb 1.x is compatible with Instant Client 12.2
:: oracledb 2.x requires Oracle 19c+
pip install "oracledb<2.0" cryptography -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 ( echo [WARN] oracledb install failed. Oracle unavailable. )

echo.
echo [  ] Installed versions:
for %%p in (pyinstaller xlsxwriter openpyxl oracledb chardet) do (
    pip show %%p 2>nul | findstr "^Version" | (set /p ver=) & echo   %%p: !ver!
)

:: ---- Step 2: Clean previous build ------------------------------------------
echo.
echo [2/4] Cleaning previous build...
if exist build rmdir /s /q build
if exist dist  rmdir /s /q dist
echo [  ] Done

:: ---- Step 3: PyInstaller ----------------------------------------------------
echo.
echo [3/4] Building EXE (single file, no console window)...
echo       Includes: main app + locales + all dependencies
echo.

pyinstaller db_importer.spec
if errorlevel 1 (
    echo.
    echo [ERROR] PyInstaller failed. See above for details.
    pause
    exit /b 1
)

if not exist "dist\DB_Importer.exe" (
    echo [ERROR] dist\DB_Importer.exe not found after build.
    pause
    exit /b 1
)
echo [OK] dist\DB_Importer.exe built successfully.

:: ---- Step 4: Assemble release package --------------------------------------
echo.
echo [4/4] Assembling release package...

set RELEASE_DIR=dist\DB_Importer_release
if exist "%RELEASE_DIR%" rmdir /s /q "%RELEASE_DIR%"
mkdir "%RELEASE_DIR%"
mkdir "%RELEASE_DIR%\data"
mkdir "%RELEASE_DIR%\logs"

:: Main EXE
copy /y "dist\DB_Importer.exe" "%RELEASE_DIR%\" >nul
echo [+] DB_Importer.exe

:: Default configs (db_config.json, ui_state.json, data/default.db)
:: These go next to the EXE so the app finds them on first launch
xcopy /e /i /q "release_defaults\*" "%RELEASE_DIR%\" >nul
echo [+] db_config.json  (default SQLite connection -> data/default.db)
echo [+] ui_state.json   (default language: zh_CN)
echo [+] data\default.db (empty SQLite database, ready to use)
echo [+] logs\           (empty, will hold import/export logs)

:: Oracle Instant Client (optional)
set HAS_OCI=0
for /d %%i in (instantclient*) do (
    echo [+] Copying Oracle Instant Client: %%i
    xcopy /e /i /q "%%i" "%RELEASE_DIR%\%%i" >nul
    set HAS_OCI=1
)
if "!HAS_OCI!"=="0" (
    echo [  ] No instantclient* folder found. Oracle Thick mode requires it at runtime.
)

:: Create ZIP using PowerShell
for /f "tokens=1-3 delims=/-" %%a in ("%date%") do (
    set DATESTAMP=%%a%%b%%c
)
set ZIP_PATH=dist\DB_Importer_!DATESTAMP!.zip

powershell -NoProfile -Command "Compress-Archive -Path '%RELEASE_DIR%\*' -DestinationPath '!ZIP_PATH!' -Force"
if exist "!ZIP_PATH!" (
    echo [+] ZIP created: !ZIP_PATH!
) else (
    echo [  ] ZIP creation failed. Manually zip: %RELEASE_DIR%\
)

:: ---- Summary ----------------------------------------------------------------
echo.
echo ================================================
echo   Build complete!
echo.
echo   Release folder : %RELEASE_DIR%\
echo   Release ZIP    : !ZIP_PATH!
echo.
echo   Contents:
echo     DB_Importer.exe   main app (multilingual, no Python needed)
echo     db_config.json    pre-configured with default SQLite connection
echo     ui_state.json     default language: zh_CN
echo     data\default.db   empty SQLite database (ready to use)
echo     logs\             log output directory
if "!HAS_OCI!"=="1" (
echo     instantclient\    Oracle Thick mode client
)
echo.
echo   SQLite: works out of the box, no server needed.
echo   MySQL / Oracle: requires a running database server.
echo.
echo   Share the ZIP. Users unzip and double-click DB_Importer.exe.
echo ================================================
echo.

explorer "%RELEASE_DIR%"
pause
