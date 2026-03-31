@echo off
chcp 65001 >nul
echo ==============================
echo  打包 CSV 导入工具为 EXE
echo ==============================

:: 激活 conda 环境
call conda activate venv
if errorlevel 1 (
    echo 激活 conda 环境失败，使用当前 Python 继续...
)

:: 安装依赖
echo.
echo [1/3] 安装 PyInstaller 及必要依赖...
:: 锁定 oracledb 1.x（兼容 instantclient 12.2；2.x 需要 19c+）
pip install pyinstaller "oracledb<2.0" cryptography -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 (
    echo 安装失败
    pause
    exit /b 1
)

:: 确认实际安装的 oracledb 版本
echo 当前 oracledb 版本:
pip show oracledb | findstr Version

:: 清理上次构建
if exist build rmdir /s /q build
if exist dist  rmdir /s /q dist

:: 打包
echo.
echo [2/3] 开始打包（单文件、无控制台窗口）...
pyinstaller --onefile --windowed --name "CSV导入工具" ^
    --hidden-import cryptography ^
    --hidden-import cryptography.hazmat.primitives.ciphers.algorithms ^
    --hidden-import cryptography.hazmat.primitives.ciphers.modes ^
    --hidden-import cryptography.hazmat.backends.openssl ^
    --collect-all cryptography ^
    csv_importer.py
if errorlevel 1 (
    echo 打包失败，请查看上方错误信息
    pause
    exit /b 1
)

:: 把 instantclient 复制到 dist\ 旁边（exe 运行时自动检测同目录）
echo.
echo [3/3] 复制 Oracle Instant Client 到 dist\...
for /d %%i in (instantclient*) do (
    echo 复制 %%i 到 dist\%%i
    xcopy /e /i /q "%%i" "dist\%%i"
)

echo.
echo 打包完成！
echo.
echo 输出目录: %cd%\dist\
echo.
echo ══ dist\ 目录结构 ════════════════════════════
echo   CSV导入工具.exe       主程序
echo   instantclient_12_2\   Oracle Thick 模式客户端
echo ═════════════════════════════════════════════
echo.
echo 分发时请将整个 dist\ 文件夹一起打包给用户。
echo.
explorer dist
pause
