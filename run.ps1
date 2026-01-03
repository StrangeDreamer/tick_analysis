# 简化版启动脚本 - 确保在当前 PowerShell 窗口执行
# 使用方法: .\run.ps1 --code 603508

# 切换到脚本目录
Set-Location $PSScriptRoot

# 激活虚拟环境（如果存在）
if (Test-Path ".venv\Scripts\Activate.ps1") {
    Write-Host "激活虚拟环境..." -ForegroundColor Green
    .\.venv\Scripts\Activate.ps1
}

# 直接调用 Python，使用 & 操作符确保在当前窗口执行
# 优先使用 py 启动器
if (Get-Command py -ErrorAction SilentlyContinue) {
    & py -3 start_analysis.py $args
} else {
    & python start_analysis.py $args
}

