# 量化分析系统启动脚本
# 自动激活虚拟环境并运行程序

# 设置执行策略（如果需要）
$currentPolicy = Get-ExecutionPolicy -Scope CurrentUser
if ($currentPolicy -eq "Restricted") {
    Write-Host "设置执行策略..." -ForegroundColor Yellow
    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
}

# 激活虚拟环境
if (Test-Path ".venv\Scripts\Activate.ps1") {
    Write-Host "激活虚拟环境..." -ForegroundColor Green
    .\.venv\Scripts\Activate.ps1
} else {
    Write-Host "虚拟环境不存在，使用系统 Python" -ForegroundColor Yellow
}

# 运行程序
Write-Host "启动量化分析系统..." -ForegroundColor Green

# 优先使用 py 启动器（Windows 标准方式，更可靠）
# 使用 & 操作符直接调用，确保在当前 PowerShell 窗口执行
if (Get-Command py -ErrorAction SilentlyContinue) {
    # 使用 py -3 指定 Python 3，确保在当前窗口执行
    & py -3 start_analysis.py $args
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    # 如果 py 不可用，使用 python
    & python start_analysis.py $args
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    # 如果 python 不可用，尝试 python3
    & python3 start_analysis.py $args
} else {
    Write-Host "❌ 错误: 未找到 Python 解释器" -ForegroundColor Red
    Write-Host "请确保已安装 Python 并添加到 PATH 环境变量" -ForegroundColor Yellow
    exit 1
}

