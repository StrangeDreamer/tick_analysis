#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tick数据查看工具
用于查看和分析已采集的tick数据
"""

import pandas as pd
import os
import sys
from datetime import datetime
import argparse

def list_data_files(data_dir="tick_data"):
    """列出所有数据文件"""
    if not os.path.exists(data_dir):
        print(f"数据目录不存在: {data_dir}")
        return []
    
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_tick.csv')]
    csv_files.sort()
    
    print(f"找到 {len(csv_files)} 个tick数据文件:")
    print("-" * 80)
    print(f"{'序号':<4} {'文件名':<25} {'大小(KB)':<10} {'行数':<8} {'股票代码':<10}")
    print("-" * 80)
    
    for i, filename in enumerate(csv_files, 1):
        filepath = os.path.join(data_dir, filename)
        size_kb = os.path.getsize(filepath) // 1024
        
        # 读取文件行数
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                line_count = sum(1 for _ in f) - 1  # 减去表头
        except:
            line_count = 0
        
        # 提取股票代码
        symbol = filename.split('_')[0]
        
        print(f"{i:<4} {filename:<25} {size_kb:<10} {line_count:<8} {symbol:<10}")
    
    return csv_files

def view_stock_data(symbol, date=None, data_dir="tick_data", rows=10):
    """查看指定股票的tick数据"""
    if date is None:
        date = datetime.now().strftime('%Y%m%d')
    
    filename = f"{symbol}_{date}_tick.csv"
    filepath = os.path.join(data_dir, filename)
    
    if not os.path.exists(filepath):
        print(f"文件不存在: {filepath}")
        return
    
    try:
        df = pd.read_csv(filepath)
        
        print(f"股票代码: {symbol}")
        print(f"日期: {date}")
        print(f"数据条数: {len(df)}")
        print(f"列名: {list(df.columns)}")
        print("\n前{}条数据:".format(rows))
        print("-" * 100)
        print(df.head(rows).to_string(index=False))
        
        if len(df) > rows:
            print(f"\n... 还有 {len(df) - rows} 条数据")
        
        # 数据统计
        print(f"\n数据统计:")
        print(f"  时间范围: {df['时间'].min()} 到 {df['时间'].max()}")
        print(f"  价格范围: {df['成交价'].min():.2f} 到 {df['成交价'].max():.2f}")
        print(f"  成交量范围: {df['成交量'].min()} 到 {df['成交量'].max()}")
        
        # 买卖盘统计
        if '买卖盘性质' in df.columns:
            trade_counts = df['买卖盘性质'].value_counts()
            print(f"  买卖盘统计:")
            for trade_type, count in trade_counts.items():
                print(f"    {trade_type}: {count} 笔 ({count/len(df)*100:.1f}%)")
        
    except Exception as e:
        print(f"读取数据失败: {e}")

def analyze_data_quality(data_dir="tick_data"):
    """分析数据质量"""
    print("数据质量分析")
    print("=" * 50)
    
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_tick.csv')]
    
    if not csv_files:
        print("没有找到数据文件")
        return
    
    total_rows = 0
    total_size = 0
    missing_data = 0
    
    for filename in csv_files:
        filepath = os.path.join(data_dir, filename)
        try:
            df = pd.read_csv(filepath)
            total_rows += len(df)
            total_size += os.path.getsize(filepath)
            
            # 检查缺失数据
            missing_count = df.isnull().sum().sum()
            missing_data += missing_count
            
        except Exception as e:
            print(f"分析文件 {filename} 时出错: {e}")
    
    print(f"总文件数: {len(csv_files)}")
    print(f"总数据行数: {total_rows:,}")
    print(f"总文件大小: {total_size / 1024 / 1024:.2f} MB")
    print(f"平均每文件行数: {total_rows / len(csv_files):.0f}")
    print(f"缺失数据总数: {missing_data}")
    print(f"数据完整性: {(1 - missing_data / (total_rows * 6)) * 100:.2f}%")

def search_stocks_by_name(name_keyword, data_dir="tick_data"):
    """根据股票名称搜索"""
    print(f"搜索包含 '{name_keyword}' 的股票...")
    
    # 这里需要从股票名称映射到代码，简化处理
    # 实际应用中可以从股票列表API获取
    stock_mapping = {
        '平安': '000001',
        '万科': '000002', 
        '浦发': '600000',
        '海康': '002415',
        '恒瑞': '600276',
        '中兴': '000063',
        '伊利': '600887'
    }
    
    found_stocks = []
    for name, symbol in stock_mapping.items():
        if name_keyword.lower() in name.lower():
            found_stocks.append((symbol, name))
    
    if found_stocks:
        print(f"找到 {len(found_stocks)} 只匹配的股票:")
        for symbol, name in found_stocks:
            print(f"  {symbol} - {name}")
    else:
        print("没有找到匹配的股票")

def main():
    parser = argparse.ArgumentParser(description='Tick数据查看工具')
    parser.add_argument('--list', action='store_true', help='列出所有数据文件')
    parser.add_argument('--view', type=str, help='查看指定股票的数据 (格式: 股票代码)')
    parser.add_argument('--date', type=str, help='指定日期 (格式: YYYYMMDD)')
    parser.add_argument('--rows', type=int, default=10, help='显示行数 (默认: 10)')
    parser.add_argument('--analyze', action='store_true', help='分析数据质量')
    parser.add_argument('--search', type=str, help='搜索股票名称')
    parser.add_argument('--data-dir', type=str, default='tick_data', help='数据目录 (默认: tick_data)')
    
    args = parser.parse_args()
    
    if args.list:
        list_data_files(args.data_dir)
    elif args.view:
        view_stock_data(args.view, args.date, args.data_dir, args.rows)
    elif args.analyze:
        analyze_data_quality(args.data_dir)
    elif args.search:
        search_stocks_by_name(args.search, args.data_dir)
    else:
        # 默认显示帮助和文件列表
        print("Tick数据查看工具")
        print("=" * 50)
        print("使用方法:")
        print("  python3 view_data.py --list                    # 列出所有数据文件")
        print("  python3 view_data.py --view 000001             # 查看平安银行数据")
        print("  python3 view_data.py --view 000001 --rows 20   # 查看前20行")
        print("  python3 view_data.py --analyze                 # 分析数据质量")
        print("  python3 view_data.py --search 平安             # 搜索股票")
        print()
        list_data_files(args.data_dir)

if __name__ == "__main__":
    main()
