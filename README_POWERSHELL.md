# PowerShell 执行说明

## 问题：在 PowerShell 中执行命令会弹出新窗口

## 解决方案

### 方法1：使用 py 启动器（推荐）

在 PowerShell 中执行：

```powershell
py -3 start_analysis.py --code 603508
```

或者使用脚本：

```powershell
.\start.ps1 --code 603508
```

### 方法2：使用 & 操作符

确保使用 `&` 操作符直接调用：

```powershell
& python start_analysis.py --code 603508
```

### 方法3：使用简化脚本

```powershell
.\run.ps1 --code 603508
```

## 重要提示

1. **不要双击 .py 文件**：双击 .py 文件会在新窗口中打开
2. **在 PowerShell 中执行**：必须在 PowerShell 终端中输入命令
3. **使用 & 操作符**：确保使用 `&` 操作符，而不是直接输入命令名

## 如果仍然弹出新窗口

检查 Python 文件关联：

```powershell
# 检查 .py 文件关联
Get-ItemProperty -Path "HKCU:\Software\Classes\.py" | Select-Object '(default)'
```

如果关联到 `Python.File` 且打开命令包含 `start` 或 `cmd`，可能需要修改文件关联。

## 测试命令

```powershell
# 测试 Python 是否在当前窗口执行
py -3 --version

# 测试脚本
py -3 start_analysis.py --code 603508
```

