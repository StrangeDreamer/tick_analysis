#!/bin/bash

# Tick数据采集系统 - 一体化脚本
# 集成采集、查看、管理、定时任务等功能

# 设置工作目录
SCRIPT_DIR="/Users/bytedance/Desktop/tick_analysis"
cd "$SCRIPT_DIR"

# 设置日志文件
LOG_FILE="tick_system.log"

# 记录操作时间
log_action() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

# 检查环境
check_environment() {
    echo "检查环境..."
    
    # 检查Python
    if ! command -v python3 &> /dev/null; then
        echo "❌ 错误: Python3未安装"
        return 1
    fi
    
    # 检查依赖包
    python3 -c "import akshare, pandas, numpy" 2>/dev/null
    if [ $? -ne 0 ]; then
        echo "❌ 缺少必要的Python包，正在安装..."
        pip3 install akshare pandas numpy
        if [ $? -ne 0 ]; then
            echo "❌ 依赖包安装失败"
            return 1
        fi
    fi
    
    echo "✅ 环境检查完成"
    return 0
}

# 测试采集器功能
test_collector() {
    echo "运行测试..."
    log_action "开始测试采集器功能"
    
    # 测试环境
    echo "1. 测试环境..."
    if ! check_environment; then
        echo "❌ 环境测试失败"
        return 1
    fi
    echo "✅ 环境测试通过"
    
    # 测试数据目录
    echo "2. 测试数据目录..."
    if [ ! -d "tick_data" ]; then
        mkdir -p tick_data
        echo "✅ 创建数据目录"
    else
        echo "✅ 数据目录存在"
    fi
    
    # 测试采集器导入
    echo "3. 测试采集器导入..."
    python3 -c "from tick_data_collector import TickDataCollector; print('✅ 采集器导入成功')" 2>/dev/null
    if [ $? -ne 0 ]; then
        echo "❌ 采集器导入失败"
        return 1
    fi
    
    # 测试获取股票列表
    echo "4. 测试股票列表获取..."
    python3 -c "
from tick_data_collector import TickDataCollector
collector = TickDataCollector()
stocks = collector.get_qualified_stocks()
if stocks:
    print(f'✅ 找到 {len(stocks)} 只符合条件的股票')
    print('前3只股票:')
    for i, stock in enumerate(stocks[:3], 1):
        print(f'  {i}. {stock[\"symbol\"]} {stock[\"name\"]} - 价格: {stock[\"price\"]}元')
else:
    print('⚠️ 使用备用股票列表')
    stocks = collector.get_fallback_stocks()
    print(f'✅ 备用列表找到 {len(stocks)} 只股票')
" 2>/dev/null
    
    if [ $? -ne 0 ]; then
        echo "❌ 股票列表获取失败"
        return 1
    fi
    
    # 测试tick数据获取
    echo "5. 测试tick数据获取..."
    python3 -c "
from tick_data_collector import TickDataCollector
collector = TickDataCollector()
tick_df = collector.get_tick_data('000001')
if tick_df is not None and not tick_df.empty:
    print(f'✅ 成功获取tick数据: {len(tick_df)} 条')
    print(f'   列名: {list(tick_df.columns)}')
else:
    print('❌ tick数据获取失败')
" 2>/dev/null
    
    if [ $? -ne 0 ]; then
        echo "❌ tick数据获取失败"
        return 1
    fi
    
    echo "✅ 所有测试通过！"
    log_action "测试完成"
}

# 手动运行采集
run_collection() {
    echo "开始手动采集..."
    log_action "开始手动采集tick数据"
    python3 tick_data_collector.py
    if [ $? -eq 0 ]; then
        echo "✅ 采集完成"
        log_action "手动采集完成"
    else
        echo "❌ 采集失败"
        log_action "手动采集失败"
    fi
}

# 查看系统日志
view_logs() {
    echo "查看系统日志..."
    if [ -f "$LOG_FILE" ]; then
        echo "最近20行系统日志:"
        tail -20 "$LOG_FILE"
    else
        echo "系统日志文件不存在"
    fi
}

# 查看已采集的数据
view_data() {
    echo "查看已采集的数据..."
    
    if [ -d "tick_data" ]; then
        echo "数据文件列表:"
        ls -la tick_data/*.csv 2>/dev/null | head -10
        
        echo ""
        echo "数据统计:"
        csv_count=$(ls tick_data/*.csv 2>/dev/null | wc -l)
        total_lines=$(cat tick_data/*.csv 2>/dev/null | wc -l | tail -1)
        total_size=$(du -sh tick_data/ 2>/dev/null | cut -f1)
        
        echo "CSV文件数量: $csv_count"
        echo "总数据行数: $total_lines"
        echo "总文件大小: $total_size"
        
        # 显示最新采集的股票
        echo ""
        echo "最新采集的股票:"
        ls -lt tick_data/*.csv 2>/dev/null | head -5 | awk '{print $9}' | sed 's/.*\///' | sed 's/_tick.csv//'
    else
        echo "数据目录不存在"
    fi
}

# 查看具体股票数据
view_stock_data() {
    echo "查看具体股票数据..."
    
    if [ ! -d "tick_data" ]; then
        echo "数据目录不存在"
        return
    fi
    
    # 列出可用的股票
    echo "可用的股票:"
    ls tick_data/*.csv 2>/dev/null | sed 's/.*\///' | sed 's/_tick.csv//' | sort | uniq | head -10
    
    echo ""
    read -p "请输入股票代码 (如 000001): " symbol
    if [ -z "$symbol" ]; then
        echo "股票代码不能为空"
        return
    fi
    
    # 查找该股票的数据文件
    data_file=$(ls tick_data/${symbol}_*_tick.csv 2>/dev/null | head -1)
    if [ -z "$data_file" ]; then
        echo "未找到股票 $symbol 的数据"
        return
    fi
    
    echo "股票 $symbol 的数据文件: $data_file"
    echo "前10行数据:"
    head -11 "$data_file"  # 包含表头
}

# 设置定时任务
setup_cron() {
    echo "设置定时任务..."
    
    CRON_JOB="30 15 * * 1-5 $SCRIPT_DIR/tick_system.sh --auto-collect"
    
    echo "任务: $CRON_JOB"
    echo "说明: 每个工作日的15:30执行tick数据采集"
    
    # 检查是否已存在相同的任务
    if crontab -l 2>/dev/null | grep -q "tick_system.sh"; then
        echo "警告: 已存在相同的定时任务"
        echo "当前crontab内容:"
        crontab -l
        echo ""
        read -p "是否要替换现有任务？(y/N): " replace
        if [ "$replace" = "y" ] || [ "$replace" = "Y" ]; then
            # 删除现有任务并添加新任务
            (crontab -l 2>/dev/null | grep -v "tick_system.sh"; echo "$CRON_JOB") | crontab -
            echo "定时任务已更新"
        else
            echo "保持现有任务不变"
            return
        fi
    else
        # 添加新任务
        (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
        echo "定时任务已添加"
    fi
    
    echo ""
    echo "当前crontab内容:"
    crontab -l
}

# 删除定时任务
remove_cron() {
    echo "删除定时任务..."
    
    if crontab -l 2>/dev/null | grep -q "tick_system.sh"; then
        (crontab -l 2>/dev/null | grep -v "tick_system.sh") | crontab -
        echo "定时任务已删除"
    else
        echo "没有找到相关的定时任务"
    fi
}

# 自动采集模式（用于定时任务）
auto_collect() {
    log_action "开始自动采集tick数据"
    python3 tick_data_collector.py >> "$LOG_FILE" 2>&1
    
    if [ $? -eq 0 ]; then
        log_action "自动采集完成"
    else
        log_action "自动采集失败"
    fi
}

# 数据质量分析
analyze_data() {
    echo "分析数据质量..."
    python3 view_data.py --analyze
}

# 清理旧数据
cleanup_data() {
    echo "清理旧数据..."
    
    if [ ! -d "tick_data" ]; then
        echo "数据目录不存在"
        return
    fi
    
    echo "当前数据文件:"
    ls -la tick_data/*.csv 2>/dev/null
    
    echo ""
    read -p "是否要删除所有数据文件？(y/N): " confirm
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        rm -f tick_data/*.csv
        rm -f tick_data/*.txt
        echo "数据文件已删除"
        log_action "清理了所有数据文件"
    else
        echo "取消清理操作"
    fi
}

# 显示帮助信息
show_help() {
    echo "Tick数据采集系统 - 使用说明"
    echo "================================"
    echo ""
    echo "基本用法:"
    echo "  ./tick_system.sh                    # 显示交互菜单"
    echo "  ./tick_system.sh --auto-collect     # 自动采集模式（定时任务用）"
    echo ""
    echo "功能选项:"
    echo "  --test         测试采集器功能"
    echo "  --collect      手动运行一次采集"
    echo "  --logs         查看采集日志"
    echo "  --data         查看已采集的数据"
    echo "  --view         查看具体股票数据"
    echo "  --setup-cron   设置定时任务"
    echo "  --remove-cron  删除定时任务"
    echo "  --analyze      分析数据质量"
    echo "  --cleanup      清理旧数据"
    echo "  --help         显示帮助信息"
    echo ""
    echo "示例:"
    echo "  ./tick_system.sh --collect          # 立即采集数据"
    echo "  ./tick_system.sh --data             # 查看数据统计"
    echo "  ./tick_system.sh --setup-cron       # 设置每天自动采集"
}

# 主菜单
show_menu() {
    echo "=========================================="
    echo "Tick数据采集系统 - 一体化管理"
    echo "=========================================="
    
    # 检查环境
    if ! check_environment; then
        echo "环境检查失败，请先解决环境问题"
        exit 1
    fi
    
    echo ""
    echo "请选择操作:"
    echo "1. 测试采集器功能"
    echo "2. 手动运行一次采集"
    echo "3. 查看采集日志"
    echo "4. 查看已采集的数据"
    echo "5. 查看具体股票数据"
    echo "6. 设置定时任务（每天15:30执行）"
    echo "7. 删除定时任务"
    echo "8. 分析数据质量"
    echo "9. 清理旧数据"
    echo "0. 退出"
    
    read -p "请输入选项 (0-9): " choice
    
    case $choice in
        1) test_collector ;;
        2) run_collection ;;
        3) view_logs ;;
        4) view_data ;;
        5) view_stock_data ;;
        6) setup_cron ;;
        7) remove_cron ;;
        8) analyze_data ;;
        9) cleanup_data ;;
        0) echo "退出"; exit 0 ;;
        *) echo "无效选项"; exit 1 ;;
    esac
}

# 主程序
main() {
    # 处理命令行参数
    case "$1" in
        --auto-collect)
            auto_collect
            ;;
        --test)
            check_environment && test_collector
            ;;
        --collect)
            check_environment && run_collection
            ;;
        --logs)
            view_logs
            ;;
        --data)
            view_data
            ;;
        --view)
            view_stock_data
            ;;
        --setup-cron)
            setup_cron
            ;;
        --remove-cron)
            remove_cron
            ;;
        --analyze)
            analyze_data
            ;;
        --cleanup)
            cleanup_data
            ;;
        --help)
            show_help
            ;;
        "")
            show_menu
            ;;
        *)
            echo "未知选项: $1"
            show_help
            exit 1
            ;;
    esac
}

# 执行主程序
main "$@"
