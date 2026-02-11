#!/bin/bash
# 量化分析系统启动脚本

cd "$(dirname "$0")"

# 激活虚拟环境
source venv/bin/activate

# 检查参数
if [ "$1" == "test" ]; then
    echo "运行测试模式（单次执行）..."
    python3 quant_analysis.py
elif [ "$1" == "loop" ]; then
    echo "运行循环模式（仅开市时间）..."
    python3 start_analysis.py
elif [ "$1" == "force" ]; then
    echo "运行强制循环模式（24小时）..."
    python3 start_analysis.py --force
else
    echo "用法："
    echo "  ./run.sh test   - 单次执行"
    echo "  ./run.sh loop   - 循环执行（仅开市时间，使用缓存）"
    echo "  ./run.sh force  - 强制循环执行（24小时 + 强制刷新缓存）"
    exit 1
fi
