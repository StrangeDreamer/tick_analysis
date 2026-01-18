#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化分析系统启动脚本
支持单次执行和循环执行模式
"""

import sys
import argparse
import time
import os
from datetime import datetime, time as dt_time
from quant_analysis import QuantAnalysis

def is_trading_time():
    """判断当前是否在开市时间内"""
    now = datetime.now()
    current_time = now.time()
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    
    # 周一到周五
    if weekday < 5:
        # 上午交易时间：9:30-11:30
        morning_start = dt_time(9, 30)
        morning_end = dt_time(11, 30)
        # 下午交易时间：13:00-15:00
        afternoon_start = dt_time(13, 0)
        afternoon_end = dt_time(15, 0)
        
        # 检查是否在上午或下午交易时间内
        is_morning = morning_start <= current_time <= morning_end
        is_afternoon = afternoon_start <= current_time <= afternoon_end
        
        return is_morning or is_afternoon
    
    return False

def run_analysis():
    """执行量化分析 - 直接导入模块调用"""
    try:
        analyzer = QuantAnalysis()
        
        # 执行分析，不再需要任何参数
        analyzer.run_analysis()
        return True
            
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断分析")
        return False
    except Exception as e:
        print(f"❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """启动量化分析系统"""
    parser = argparse.ArgumentParser(description='量化分析系统启动器')
    parser.add_argument('--force', '-f', action='store_true', help='强制循环执行，忽略开市时间限制')
    
    args = parser.parse_args()
    
    # 热门股票分析 - 循环执行模式
    print("=" * 60)
    print("量化分析循环执行调度器启动")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.force:
        print("执行模式: 强制循环执行（忽略开市时间限制）")
    else:
        print("开市时间: 周一至周五 9:30-11:30, 13:00-15:00")
        print("执行模式: 循环执行（每2分钟执行一轮）")
    print("执行间隔: 2分钟")
    print("分析模式: 热门股票排行榜")
    print("=" * 60)
    
    round_count = 0
    
    # 主循环
    while True:
        try:
            round_count += 1
            print(f"\n{'='*20} 第 {round_count} 轮执行 {'='*20}")
            
            # 检查是否在开市时间（除非使用--force参数）
            if not args.force and not is_trading_time():
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 非开市时间，等待开市...")
                time.sleep(60)  # 非开市时间等待1分钟再检查
                continue
            
            # 执行量化分析
            start_time = time.time()
            success = run_analysis()
            end_time = time.time()
            
            execution_time = end_time - start_time
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 第 {round_count} 轮执行完成，耗时: {execution_time:.1f}秒")
            
            if success:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 第 {round_count} 轮执行成功")
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 第 {round_count} 轮执行失败")
            
            # 判断是否在开市日上午9:30-10:00时间段
            now = datetime.now()
            current_time = now.time()
            morning_rush_start = dt_time(9, 30)  # 9:30
            morning_rush_end = dt_time(10, 0)    # 10:00
            
            # 如果在9:30-10:00时间段，立即执行下一轮（不等待）
            if morning_rush_start <= current_time <= morning_rush_end:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 当前在开市日上午9:30-10:00时间段，立即执行下一轮（无等待）")
                continue  # 直接进入下一轮循环，不等待
            
            # 其他时间段，等待2分钟后执行下一轮
            wait_minutes = 2
            wait_seconds = wait_minutes * 60
            next_time = datetime.now().timestamp() + wait_seconds
            next_datetime = datetime.fromtimestamp(next_time)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 等待{wait_minutes}分钟后执行下一轮（下次执行时间: {next_datetime.strftime('%Y-%m-%d %H:%M:%S')}）")
            time.sleep(wait_seconds)
            
        except KeyboardInterrupt:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 收到中断信号，停止调度器")
            print(f"总共执行了 {round_count} 轮")
            break
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 调度器异常: {e}")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 异常后等待10秒再继续...")
            time.sleep(10)

if __name__ == "__main__":
    sys.exit(main())
