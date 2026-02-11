# Windows 使用指南

## 📋 平台兼容性说明

| 脚本 | 平台 | 说明 |
|------|------|------|
| `run.sh` | ❌ Windows 不支持 | Bash脚本，仅适用于 Linux/macOS |
| `run_win.bat` | ✅ Windows | CMD批处理脚本（推荐） |
| `run_win.ps1` | ✅ Windows | PowerShell脚本（功能更强） |

---

## 🚀 Windows 快速开始

### 1. 环境准备（首次使用）

#### 安装 Python
1. 访问 [Python官网](https://www.python.org/downloads/)
2. 下载并安装 Python 3.9 或更高版本
3. **重要**：安装时勾选 "Add Python to PATH"

#### 创建虚拟环境并安装依赖
```cmd
# 打开 CMD 或 PowerShell
cd C:\path\to\tick_analysis

# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
# CMD:
venv\Scripts\activate.bat
# PowerShell:
venv\Scripts\Activate.ps1

# 安装依赖
pip install -r requirements.txt
```

---

## 🎯 使用方法

### 方法1：使用 CMD（推荐新手）

```cmd
# 打开 CMD，进入项目目录
cd C:\path\to\tick_analysis

# 单次执行（测试用）
run_win.bat test

# 循环执行（仅开市时间）
run_win.bat loop

# 强制循环执行（24小时）
run_win.bat force
```

### 方法2：使用 PowerShell（推荐高级用户）

```powershell
# 打开 PowerShell，进入项目目录
cd C:\path\to\tick_analysis

# 首次执行可能需要设置执行策略
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# 单次执行（测试用）
.\run_win.ps1 test

# 循环执行（仅开市时间）
.\run_win.ps1 loop

# 强制循环执行（24小时）
.\run_win.ps1 force
```

### 方法3：手动运行（最灵活）

```cmd
# 激活虚拟环境
venv\Scripts\activate.bat

# 单次执行
python quant_analysis.py

# 循环执行
python start_analysis.py

# 强制循环（忽略开市时间）
python start_analysis.py --force
```

---

## ⚠️ PowerShell 执行策略问题

如果遇到以下错误：
```
.\run_win.ps1 : 无法加载文件，因为在此系统上禁止运行脚本。
```

**解决方法：**

```powershell
# 方法1：临时允许（仅当前窗口）
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process

# 方法2：永久允许（推荐）
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# 然后再运行脚本
.\run_win.ps1 test
```

---

## 🔧 后台运行（Windows）

### 使用 PowerShell 后台运行

```powershell
# 启动后台任务
Start-Job -ScriptBlock {
    Set-Location "C:\path\to\tick_analysis"
    .\venv\Scripts\Activate.ps1
    python start_analysis.py --force
} -Name "StockAnalysis"

# 查看任务状态
Get-Job

# 查看输出
Receive-Job -Name "StockAnalysis" -Keep

# 停止任务
Stop-Job -Name "StockAnalysis"
Remove-Job -Name "StockAnalysis"
```

### 使用任务计划程序（开机自启）

1. 打开 **任务计划程序** (`taskschd.msc`)
2. 创建基本任务
3. 触发器：选择"当计算机启动时"
4. 操作：启动程序
   - 程序：`C:\path\to\tick_analysis\run_win.bat`
   - 参数：`loop`
   - 起始于：`C:\path\to\tick_analysis`

---

## 📊 测试工具（Windows）

### 1. 测试 API 连接
```cmd
venv\Scripts\activate.bat
python test_connection.py
```

### 2. 快速测试
```cmd
venv\Scripts\activate.bat
python quick_test.py
```

---

## 🛑 停止程序

### 前台运行（CMD/PowerShell）
```
按 Ctrl+C 停止
```

### 后台运行（PowerShell Job）
```powershell
Stop-Job -Name "StockAnalysis"
Remove-Job -Name "StockAnalysis"
```

### 任务计划程序
1. 打开任务计划程序
2. 找到对应任务
3. 右键 → 结束

---

## 📁 项目结构

```
tick_analysis/
├── venv/                      # 虚拟环境（自己创建）
├── run_win.bat               # Windows CMD 启动脚本 ⭐
├── run_win.ps1               # Windows PowerShell 启动脚本 ⭐
├── run.sh                     # Linux/macOS 启动脚本
├── quant_analysis.py         # 核心分析脚本
├── start_analysis.py         # 循环执行脚本
├── test_connection.py        # API 连接测试
├── quick_test.py             # 快速测试
└── requirements.txt          # Python 依赖
```

---

## 🔄 完整工作流程（Windows）

### 首次使用
```cmd
# 1. 创建虚拟环境
python -m venv venv

# 2. 激活虚拟环境
venv\Scripts\activate.bat

# 3. 安装依赖
pip install -r requirements.txt

# 4. 测试运行
python test_connection.py

# 5. 单次测试
run_win.bat test
```

### 日常使用
```cmd
# 开市时间运行（推荐）
run_win.bat loop

# 或者 24 小时运行
run_win.bat force
```

---

## 💡 常见问题（Windows）

### Q1: 提示"python不是内部或外部命令"
**A:** Python 未添加到 PATH。解决方法：
1. 重新安装 Python，勾选"Add Python to PATH"
2. 或手动添加到环境变量

### Q2: 提示"无法加载虚拟环境"
**A:** 虚拟环境路径问题。解决方法：
```cmd
# 删除旧虚拟环境
rd /s /q venv

# 重新创建
python -m venv venv
venv\Scripts\activate.bat
pip install -r requirements.txt
```

### Q3: PowerShell 执行策略错误
**A:** 参考上面"PowerShell 执行策略问题"章节

### Q4: 网络连接错误（收盘后常见）
**A:** 这是正常的！参考主文档的"网络连接错误"章节

---

## 📝 对比表格

| 功能 | Linux/macOS | Windows CMD | Windows PowerShell |
|------|-------------|-------------|-------------------|
| 脚本名称 | `./run.sh` | `run_win.bat` | `.\run_win.ps1` |
| 单次执行 | `./run.sh test` | `run_win.bat test` | `.\run_win.ps1 test` |
| 循环执行 | `./run.sh loop` | `run_win.bat loop` | `.\run_win.ps1 loop` |
| 强制循环 | `./run.sh force` | `run_win.bat force` | `.\run_win.ps1 force` |
| 虚拟环境激活 | `source venv/bin/activate` | `venv\Scripts\activate.bat` | `venv\Scripts\Activate.ps1` |

---

## ✅ 推荐配置

### 新手推荐
- 使用 **CMD + run_win.bat**
- 简单易用，不需要设置执行策略

### 高级用户推荐
- 使用 **PowerShell + run_win.ps1**
- 功能更强大，输出更美观

### 服务器/长期运行推荐
- 使用 **任务计划程序**
- 开机自启，稳定可靠

---

## 🎉 总结

**Windows 用户使用流程：**

1. ✅ 安装 Python 3.9+
2. ✅ 创建虚拟环境：`python -m venv venv`
3. ✅ 安装依赖：`pip install -r requirements.txt`
4. ✅ 运行程序：`run_win.bat test` 或 `.\run_win.ps1 test`

**脚本对应关系：**
- `./run.sh` (Linux/macOS) → `run_win.bat` 或 `run_win.ps1` (Windows)

现在 Windows 用户也能享受相同的功能了！🎊
