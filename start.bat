@echo off
REM 量化分析系统启动脚本（CMD版本）
REM 自动激活虚拟环境并运行程序
REM 确保在当前终端窗口执行，不打开新窗口

REM 切换到脚本所在目录
cd /d "%~dp0"

echo 启动量化分析系统...

REM 检查虚拟环境是否存在
if exist ".venv\Scripts\activate.bat" (
    echo 激活虚拟环境...
    call .venv\Scripts\activate.bat
) else (
    echo 虚拟环境不存在，使用系统 Python
)

REM 运行程序
python start_analysis.py %*

REM 如果是从命令行执行的，不暂停（避免阻塞）
REM 如果双击执行的，暂停以便查看结果
if "%CMDEXTVERSION%"=="" (
    pause
)

