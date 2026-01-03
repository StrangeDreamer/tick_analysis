# 测试 Python 调用方式
Write-Host "测试 Python 调用方式..." -ForegroundColor Green

# 方法1: 直接调用（应该在当前窗口）
Write-Host "`n方法1: 使用 & 操作符直接调用" -ForegroundColor Yellow
& python --version

# 方法2: 使用 py 启动器
Write-Host "`n方法2: 使用 py 启动器" -ForegroundColor Yellow
if (Get-Command py -ErrorAction SilentlyContinue) {
    & py --version
} else {
    Write-Host "py 启动器不可用" -ForegroundColor Red
}

# 方法3: 使用完整路径
Write-Host "`n方法3: 查找 Python 完整路径" -ForegroundColor Yellow
$pythonPath = (Get-Command python -ErrorAction SilentlyContinue).Source
if ($pythonPath) {
    Write-Host "Python 路径: $pythonPath" -ForegroundColor Cyan
    & $pythonPath --version
} else {
    Write-Host "未找到 python 命令" -ForegroundColor Red
}

# 检查文件关联
Write-Host "`n检查 .py 文件关联..." -ForegroundColor Yellow
$assoc = (Get-ItemProperty -Path "HKCU:\Software\Classes\.py" -ErrorAction SilentlyContinue).'(default)'
if ($assoc) {
    Write-Host ".py 文件关联: $assoc" -ForegroundColor Cyan
    $command = (Get-ItemProperty -Path "HKCU:\Software\Classes\$assoc\shell\open\command" -ErrorAction SilentlyContinue).'(default)'
    if ($command) {
        Write-Host "打开命令: $command" -ForegroundColor Cyan
        if ($command -match 'pythonw\.exe') {
            Write-Host "⚠️ 警告: 使用了 pythonw.exe，这会在后台运行（无窗口）" -ForegroundColor Yellow
        }
        if ($command -match 'start|cmd') {
            Write-Host "⚠️ 警告: 命令中包含 start 或 cmd，可能会打开新窗口" -ForegroundColor Yellow
        }
    }
} else {
    Write-Host "未找到 .py 文件关联配置" -ForegroundColor Yellow
}

