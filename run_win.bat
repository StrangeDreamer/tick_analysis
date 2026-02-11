@echo off
REM 量化分析系统启动脚本 - Windows 版本
REM 功能与 run.sh 一致

cd /d "%~dp0"

REM 检查虚拟环境
set "VENV_PATH=venv\Scripts\activate.bat"
if not exist "%VENV_PATH%" (
    set "VENV_PATH=.venv\Scripts\activate.bat"
)

REM 检查参数
if "%1"=="" goto usage
if "%1"=="test" goto test_mode
if "%1"=="loop" goto loop_mode
if "%1"=="force" goto force_mode
goto usage

:test_mode
echo 运行测试模式（单次执行）...
if exist "%VENV_PATH%" call "%VENV_PATH%"
python quant_analysis.py
goto end

:loop_mode
echo 运行循环模式（仅开市时间）...
if exist "%VENV_PATH%" call "%VENV_PATH%"
python start_analysis.py
goto end

:force_mode
echo 运行强制循环模式（24小时）...
if exist "%VENV_PATH%" call "%VENV_PATH%"
python start_analysis.py --force
goto end

:usage
echo 用法：
echo   run_win.bat test   - 单次执行
echo   run_win.bat loop   - 循环执行（仅开市时间）
echo   run_win.bat force  - 强制循环执行（24小时）
goto end

:end
