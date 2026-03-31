@echo off
chcp 65001 >nul
echo ==============================
echo  创建 CSV 导入工具虚拟环境
echo ==============================

:: 环境名称（可自行修改）
set ENV_NAME=csv_importer

:: 创建 conda 环境，指定 Python 3.11
echo.
echo [1/4] 创建 conda 环境: %ENV_NAME% (Python 3.11)
call conda create -n %ENV_NAME% python=3.11 -y
if errorlevel 1 (
    echo 创建环境失败，请检查 conda 是否已安装并添加到 PATH
    pause
    exit /b 1
)

:: 激活环境
echo.
echo [2/4] 激活环境...
call conda activate %ENV_NAME%
if errorlevel 1 (
    echo 激活环境失败
    pause
    exit /b 1
)

:: 安装依赖
echo.
echo [3/4] 安装依赖包...
pip install chardet mysql-connector-python cx_Oracle -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 (
    echo 依赖安装失败
    pause
    exit /b 1
)

:: 完成
echo.
echo [4/4] 完成！
echo.
echo ==============================
echo  环境名称 : %ENV_NAME%
echo  Python   : 3.11
echo  已安装   : chardet, mysql-connector-python, cx_Oracle
echo ==============================
echo.
echo 使用方式：
echo   conda activate %ENV_NAME%
echo   python csv_importer.py
echo.
pause
