@echo off
REM 简化版启动脚本 - 在当前终端执行
REM 使用方法：在终端中执行 run.bat 或直接运行 python start_analysis.py

cd /d "%~dp0"

if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
)

python start_analysis.py %*

