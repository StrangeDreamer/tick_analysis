# 量化分析系统启动脚本 - Windows PowerShell 版本
# 功能与 run.sh 一致

# 切换到脚本目录
Set-Location $PSScriptRoot

# 检查虚拟环境
$venvPath = "venv\Scripts\Activate.ps1"
if (-not (Test-Path $venvPath)) {
    $venvPath = ".venv\Scripts\Activate.ps1"
}

# 检查参数
if ($args.Count -eq 0) {
    Write-Host "用法：" -ForegroundColor Yellow
    Write-Host "  .\run_win.ps1 test   - 单次执行" -ForegroundColor Cyan
    Write-Host "  .\run_win.ps1 loop   - 循环执行（仅开市时间）" -ForegroundColor Cyan
    Write-Host "  .\run_win.ps1 force  - 强制循环执行（24小时）" -ForegroundColor Cyan
    exit 1
}

$mode = $args[0]

switch ($mode) {
    "test" {
        Write-Host "运行测试模式（单次执行）..." -ForegroundColor Green
        if (Test-Path $venvPath) {
            & $venvPath
        }
        python quant_analysis.py
    }
    "loop" {
        Write-Host "运行循环模式（仅开市时间）..." -ForegroundColor Green
        if (Test-Path $venvPath) {
            & $venvPath
        }
        python start_analysis.py
    }
    "force" {
        Write-Host "运行强制循环模式（24小时）..." -ForegroundColor Green
        if (Test-Path $venvPath) {
            & $venvPath
        }
        python start_analysis.py --force
    }
    default {
        Write-Host "错误：未知参数 '$mode'" -ForegroundColor Red
        Write-Host ""
        Write-Host "用法：" -ForegroundColor Yellow
        Write-Host "  .\run_win.ps1 test   - 单次执行" -ForegroundColor Cyan
        Write-Host "  .\run_win.ps1 loop   - 循环执行（仅开市时间）" -ForegroundColor Cyan
        Write-Host "  .\run_win.ps1 force  - 强制循环执行（24小时）" -ForegroundColor Cyan
        exit 1
    }
}
