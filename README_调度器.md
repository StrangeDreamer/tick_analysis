# Tick Analysis 量化分析循环执行调度器

## 功能说明
在A股开市时间内，循环执行量化分析，上一轮完成后立即开始下一轮，并将结果发送到钉钉群。

## 执行模式
- **循环执行**: 上一轮完成后立即开始下一轮
- **超时保护**: 每轮最多5分钟，超时后强制开始下一轮
- **开市时间**: 只在开市时间内执行
- **非开市时间**: 等待1分钟后重新检查

## 开市时间
- **工作日**: 周一至周五
- **交易时间**: 9:30-15:00（连续交易时间）
- **非开市时间**: 等待开市

## 文件说明
- `scheduler.py`: 主调度器脚本
- `start_scheduler.sh`: 启动脚本
- `quant_analysis copy.py`: 量化分析主程序

## 使用方法

### 方法1: 直接运行Python脚本
```bash
cd /Users/bytedance/Desktop/tick_analysis
python3 scheduler.py
```

### 方法2: 使用启动脚本
```bash
cd /Users/bytedance/Desktop/tick_analysis
./start_scheduler.sh
```

### 方法3: 后台运行
```bash
cd /Users/bytedance/Desktop/tick_analysis
nohup python3 scheduler.py > scheduler.log 2>&1 &
```

## 停止调度器
- 如果在前台运行：按 `Ctrl+C`
- 如果在后台运行：使用 `ps aux | grep scheduler` 找到进程ID，然后 `kill <进程ID>`

## 日志查看
如果使用后台运行，日志会保存在 `scheduler.log` 文件中：
```bash
tail -f scheduler.log
```

## 执行特点
1. **连续执行**: 上一轮完成后立即开始下一轮
2. **超时保护**: 每轮最多5分钟，防止卡死
3. **智能等待**: 非开市时间等待1分钟再检查
4. **异常恢复**: 出现异常后等待10秒再继续
5. **轮次统计**: 显示当前执行轮次和耗时

## 注意事项
1. 确保网络连接正常，能够访问AKShare接口
2. 确保钉钉webhook配置正确
3. 调度器会在非开市时间自动等待
4. 每次执行有5分钟超时限制
5. 建议在服务器上运行，确保稳定性
6. 循环执行模式会持续运行，请确保系统资源充足

## 故障排除
1. **执行超时**: 检查网络连接和AKShare接口状态
2. **钉钉发送失败**: 检查webhook配置和网络连接
3. **调度器停止**: 检查进程状态，重新启动
4. **资源不足**: 检查系统内存和CPU使用情况

## 自定义配置
可以在 `scheduler.py` 中修改：
- 超时时间（默认5分钟）
- 非开市时间等待间隔（默认60秒）
- 异常后等待时间（默认10秒）
- 开市时间判断逻辑
