#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化分析定时任务调度器
在开市时间内每2分钟执行一次量化分析
"""

import time
import subprocess
import sys
from datetime import datetime, time as dt_time
import os

def is_trading_time():
    """判断当前是否在开市时间内"""
    now = datetime.now()
    current_time = now.time()
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    
    # 周一到周五
    if weekday < 5:
        # 开市时间：9:30-15:00（连续交易时间）
        trading_start = dt_time(9, 30)
        trading_end = dt_time(15, 0)
        
        return trading_start <= current_time <= trading_end
    
    return False

def run_quant_analysis():
    """执行量化分析"""
    if not is_trading_time():
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 非开市时间，跳过执行")
        return False
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始执行量化分析...")
    
    try:
        # 切换到脚本目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        
        # 执行量化分析脚本（直接输出到终端）
        result = subprocess.run([
            sys.executable, "quant_analysis copy.py"
        ], timeout=1200)  # 20分钟超时，不捕获输出，直接显示在终端
        
        if result.returncode == 0:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 量化分析执行成功")
            return True
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 量化分析执行失败，错误码: {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 量化分析执行超时（20分钟）")
        return False
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 执行异常: {e}")
        return False

def main():
    """主函数 - 循环执行模式"""
    print("=" * 60)
    print("量化分析循环执行调度器启动")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("开市时间: 周一至周五 9:30-15:00")
    print("执行模式: 循环执行（上一轮完成后立即开始下一轮）")
    print("超时时间: 20分钟")
    print("=" * 60)
    
    round_count = 0
    
    # 主循环
    while True:
        try:
            round_count += 1
            print(f"\n{'='*20} 第 {round_count} 轮执行 {'='*20}")
            
            # 检查是否在开市时间
            if not is_trading_time():
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 非开市时间，等待开市...")
                time.sleep(60)  # 非开市时间等待1分钟再检查
                continue
            
            # 执行量化分析
            start_time = time.time()
            success = run_quant_analysis()
            end_time = time.time()
            
            execution_time = end_time - start_time
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 第 {round_count} 轮执行完成，耗时: {execution_time:.1f}秒")
            
            if success:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 第 {round_count} 轮执行成功，立即开始下一轮...")
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 第 {round_count} 轮执行失败，立即开始下一轮...")
            
            # 短暂休息1秒，避免过于频繁
            time.sleep(1)
            
        except KeyboardInterrupt:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 收到中断信号，停止调度器")
            print(f"总共执行了 {round_count} 轮")
            break
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 调度器异常: {e}")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 异常后等待10秒再继续...")
            time.sleep(10)  # 异常时等待10秒再继续

if __name__ == "__main__":
    main()
