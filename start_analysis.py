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
import importlib.util
from datetime import datetime, time as dt_time

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

def run_analysis(code=None, refresh=False, refresh_filter=False, custom_only=False, no_filter=False, source='hot_rank'):
    """执行量化分析 - 直接导入模块调用，避免新窗口"""
    try:
        # 动态导入 quant_analysis copy.py 模块（因为文件名包含空格）
        script_dir = os.path.dirname(os.path.abspath(__file__))
        module_path = os.path.join(script_dir, "quant_analysis copy.py")
        
        spec = importlib.util.spec_from_file_location("quant_analysis_copy", module_path)
        if spec is None or spec.loader is None:
            print(f"❌ 无法加载模块: {module_path}")
            return False
        
        quant_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(quant_module)
        
        # 创建 QuantAnalysis 实例
        analyzer = quant_module.QuantAnalysis()
        
        # 设置股票源
        analyzer.stock_source = source
        
        # 如果需要强制刷新缓存，删除缓存文件
        if refresh:
            if os.path.exists(analyzer.hot_stocks_cache_file):
                os.remove(analyzer.hot_stocks_cache_file)
                print("🔄 已删除热门股票缓存，将重新获取...")
        
        # 如果需要强制刷新筛选缓存，删除筛选缓存文件
        if refresh_filter:
            if os.path.exists(analyzer.price_cyq_filter_cache_file):
                os.remove(analyzer.price_cyq_filter_cache_file)
                print("🔄 已删除股价和筹码筛选缓存，将重新获取...")
            
            # 如果数据源是 hot_rank，也删除热门排行榜缓存
            if analyzer.stock_source == 'hot_rank':
                if os.path.exists(analyzer.hot_stocks_cache_file):
                    try:
                        import json
                        with open(analyzer.hot_stocks_cache_file, 'r', encoding='utf-8') as f:
                            cache_data = json.load(f)
                            cache_source = cache_data.get('source', 'zt')
                            if cache_source == 'hot_rank':
                                os.remove(analyzer.hot_stocks_cache_file)
                                print("🔄 已删除热门排行榜缓存，将重新获取...")
                    except:
                        pass
            
            analyzer.refresh_filter_cache = True
        else:
            analyzer.refresh_filter_cache = False
        
        # 执行分析
        if code:
            # 分析单个股票
            result = analyzer.analyze_single_stock(code)
            if result:
                print(f"\n🎯 分析完成！股票 {result['symbol']} ({result['name']}) 得分: {result['score']:.2f}")
                return True
            else:
                print(f"❌ 分析失败")
                return False
        else:
            # 默认分析股票
            if custom_only:
                print("🔍 量化分析系统 - 只分析自定义股票")
                analyzer.run_analysis(custom_only=True)
            elif no_filter:
                print("🔍 量化分析系统 - 直接获取tick数据模式（跳过筛选）")
                analyzer.run_analysis(no_filter=True)
            else:
                print("🔍 量化分析系统 - 分析热门股票 + 自定义股票")
                analyzer.run_analysis(custom_only=False)
            return True
            
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断分析")
        return False
    except Exception as e:
        print(f"❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def delete_custom_stock(code):
    """删除自定义股票"""
    try:
        # 导入QuantAnalysis类来操作自定义股票
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        # 动态导入模块（因为文件名有空格）
        import importlib.util
        spec = importlib.util.spec_from_file_location("quant_analysis", "quant_analysis copy.py")
        quant_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(quant_module)
        
        analyzer = quant_module.QuantAnalysis()
        
        # 确保股票代码格式正确
        if not code.startswith(('SH', 'SZ')):
            if code.startswith('6'):
                full_code = f'SH{code}'
            elif code.startswith('0') or code.startswith('3'):
                full_code = f'SZ{code}'
            else:
                full_code = code
        else:
            full_code = code
        
        # 查找并删除股票（支持两种格式匹配）
        original_count = len(analyzer.custom_stocks)
        analyzer.custom_stocks = [stock for stock in analyzer.custom_stocks 
                                 if stock['代码'] != code and stock['代码'] != full_code]
        
        if len(analyzer.custom_stocks) < original_count:
            # 保存更新后的列表
            analyzer.save_custom_stocks()
            print(f"✅ 成功删除股票 {code}")
            print(f"📊 当前自定义股票池: {len(analyzer.custom_stocks)}只")
            if analyzer.custom_stocks:
                print("剩余股票:")
                for stock in analyzer.custom_stocks:
                    print(f"  {stock['代码']} {stock['股票名称']}")
            return True
        else:
            print(f"❌ 未找到股票 {code}，可能不在自定义股票池中")
            return False
    except Exception as e:
        print(f"❌ 删除股票失败: {e}")
        return False

def list_custom_stocks():
    """列出所有自定义股票"""
    try:
        # 动态导入QuantAnalysis类
        import importlib.util
        spec = importlib.util.spec_from_file_location("quant_analysis_copy", "quant_analysis copy.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        analyzer = module.QuantAnalysis()
        custom_stocks = analyzer.load_custom_stocks()
        
        if not custom_stocks:
            print("=" * 60)
            print("⚠️ 自定义股票池为空")
            print("=" * 60)
            print("\n💡 使用以下命令添加股票：")
            print("   python3 start_analysis.py --code 000001")
            print("   python3 start_analysis.py --code 000001 002251 601360")
            return True
        
        print("=" * 60)
        print(f"📋 自定义股票池 (共{len(custom_stocks)}只)")
        print("=" * 60)
        
        # 直接显示股票列表（不进行实时查询）
        for i, stock in enumerate(custom_stocks, 1):
            code = stock.get('代码', 'N/A')
            name = stock.get('股票名称', 'N/A')
            price = stock.get('最新价', 'N/A')
            
            print(f"{i:3d}. {code:10s} {name:20s}", end='')
            if price != 'N/A':
                print(f" 价格:{price:6.2f}")
            else:
                print()
        
        print("=" * 60)
        print(f"💡 使用以下命令管理股票：")
        print(f"   添加单只: python3 start_analysis.py --code 000001")
        print(f"   添加多只: python3 start_analysis.py --code 000001 002251 601360")
        print(f"   删除单只: python3 start_analysis.py --delete 002251")
        print(f"   删除多只: python3 start_analysis.py --delete 002251 601360")
        print(f"   分析: python3 start_analysis.py --custom-only")
        print("=" * 60)
        
        return True
    except Exception as e:
        print(f"❌ 查看自定义股票失败: {e}")
        return False

def list_accumulated_zt_stocks():
    """列出所有累积的涨停股票"""
    try:
        import json
        import os
        
        cache_file = "accumulated_zt_stocks.json"
        
        if not os.path.exists(cache_file):
            print("=" * 80)
            print("⚠️ 累积涨停股票池为空")
            print("=" * 80)
            print("\n💡 累积涨停池会在程序运行时自动创建并更新")
            print("   每次获取昨日涨停池数据时，都会增量保存到此池中")
            return True
        
        with open(cache_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        stocks = data.get('stocks', [])
        last_update = data.get('last_update', 'N/A')
        total_count = data.get('total_count', len(stocks))
        
        if not stocks:
            print("=" * 80)
            print("⚠️ 累积涨停股票池为空")
            print("=" * 80)
            return True
        
        # 按涨停次数排序
        stocks_sorted = sorted(stocks, key=lambda x: x.get('涨停次数', 0), reverse=True)
        
        print("=" * 80)
        print(f"📋 累积涨停股票池 (共{total_count}只，最后更新: {last_update})")
        print("=" * 80)
        print(f"{'序号':<5} {'代码':<10} {'名称':<12} {'首次涨停':<12} {'最近涨停':<12} {'涨停次数':<8} {'最新价':<8}")
        print("-" * 80)
        
        for i, stock in enumerate(stocks_sorted, 1):
            code = stock.get('代码', 'N/A')
            name = stock.get('股票名称', 'N/A')
            first_date = stock.get('首次涨停日期', 'N/A')
            recent_date = stock.get('最近涨停日期', 'N/A')
            zt_count = stock.get('涨停次数', 0)
            price = stock.get('最新价', 0)
            
            print(f"{i:<5d} {code:<10s} {name:<12s} {first_date:<12s} {recent_date:<12s} {zt_count:<8d} {price:<8.2f}")
        
        print("=" * 80)
        print(f"📊 统计信息:")
        print(f"   - 总股票数: {total_count}只")
        print(f"   - 最后更新: {last_update}")
        print(f"   - 多次涨停: {len([s for s in stocks if s.get('涨停次数', 0) > 1])}只")
        print(f"   - 单次涨停: {len([s for s in stocks if s.get('涨停次数', 0) == 1])}只")
        print("=" * 80)
        print(f"💡 说明:")
        print(f"   - 此池每天自动增量更新，只增加不减少")
        print(f"   - 涨停次数统计从开始使用本系统起的累积次数")
        print(f"   - 可用于发现反复涨停的强势股")
        print("=" * 80)
        
        return True
    except Exception as e:
        print(f"❌ 查看累积涨停股票池失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """启动量化分析系统"""
    parser = argparse.ArgumentParser(description='量化分析系统启动器')
    parser.add_argument('--code', '-c', type=str, nargs='+', help='分析指定股票代码，支持多个 (例如: --code 000001 002251 601360)')
    parser.add_argument('--delete', '-d', type=str, nargs='+', help='删除指定股票代码，支持多个 (例如: --delete 002251 601360)')
    parser.add_argument('--list', '-l', action='store_true', help='查看自定义股票池')
    parser.add_argument('--list-zt', action='store_true', help='查看累积涨停股票池')
    parser.add_argument('--force', '-f', action='store_true', help='强制循环执行，忽略开市时间限制')
    parser.add_argument('--refresh', '-r', action='store_true', help='强制刷新热门股票缓存（重新调用API获取）')
    parser.add_argument('--refresh-filter', action='store_true', help='强制刷新股价和筹码筛选缓存（重新调用API获取）')
    parser.add_argument('--custom-only', action='store_true', help='只分析自定义股票，不分析热门股票')
    parser.add_argument('--no-filter', action='store_true', help='跳过筛选，直接获取所有股票的tick数据并排名')
    parser.add_argument('--source', '-s', type=str, choices=['ljqs', 'zt', 'hot_rank'], default='hot_rank',
                       help='热门股票源: hot_rank=热门排行榜(默认), zt=昨日涨停池, ljqs=量价齐升')
    
    args = parser.parse_args()
    
    if args.list:
        # 查看自定义股票池
        success = list_custom_stocks()
        return 0 if success else 1
    elif args.list_zt:
        # 查看累积涨停股票池
        success = list_accumulated_zt_stocks()
        return 0 if success else 1
    elif args.delete:
        # 删除自定义股票模式（支持多个股票）
        codes = args.delete if isinstance(args.delete, list) else [args.delete]
        
        print("=" * 60)
        if len(codes) == 1:
            print("删除自定义股票模式")
        else:
            print("批量删除自定义股票模式")
        print(f"删除股票: {', '.join(codes)}")
        print(f"股票数量: {len(codes)}只")
        print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # 删除每只股票
        success_count = 0
        failed_count = 0
        
        for i, code in enumerate(codes, 1):
            print(f"\n{'='*20} 删除第 {i}/{len(codes)} 只股票 {'='*20}")
            success = delete_custom_stock(code)
            if success:
                print(f"✅ 股票 {code} 删除成功")
                success_count += 1
            else:
                print(f"❌ 股票 {code} 删除失败")
                failed_count += 1
        
        # 汇总结果
        print(f"\n{'='*60}")
        print(f"📊 批量删除完成")
        print(f"   成功: {success_count}只")
        print(f"   失败: {failed_count}只")
        print(f"   总计: {len(codes)}只")
        print(f"{'='*60}")
        
        return 0 if failed_count == 0 else 1
    elif args.code:
        # 股票分析 - 单次执行模式（支持多个股票）
        codes = args.code if isinstance(args.code, list) else [args.code]
        
        print("=" * 60)
        if len(codes) == 1:
            print("单只股票分析模式")
        else:
            print("批量股票分析模式")
        print(f"分析股票: {', '.join(codes)}")
        print(f"股票数量: {len(codes)}只")
        print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("执行模式: 单次执行（分析完成后自动添加到热门股票池）")
        if args.refresh:
            print("缓存模式: 强制刷新热门股票缓存")
        print("=" * 60)
        
        # 分析每只股票
        success_count = 0
        failed_count = 0
        
        for i, code in enumerate(codes, 1):
            print(f"\n{'='*20} 分析第 {i}/{len(codes)} 只股票 {'='*20}")
            success = run_analysis(code, args.refresh, args.refresh_filter, no_filter=args.no_filter, source=args.source)
            if success:
                print(f"✅ 股票 {code} 分析完成")
                success_count += 1
            else:
                print(f"❌ 股票 {code} 分析失败")
                failed_count += 1
        
        # 汇总结果
        print(f"\n{'='*60}")
        print(f"📊 批量分析完成")
        print(f"   成功: {success_count}只")
        print(f"   失败: {failed_count}只")
        print(f"   总计: {len(codes)}只")
        print(f"{'='*60}")
        
        return 0 if failed_count == 0 else 1
    else:
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
        print("超时时间: 20分钟")
        if args.custom_only:
            print("分析模式: 只分析自定义股票")
        else:
            source_names = {'zt': '昨日涨停池', 'ljqs': '量价齐升', 'hot_rank': '热门排行榜'}
            source_name = source_names.get(args.source, '未知')
            print(f"分析模式: 热门股票({source_name}) + 自定义股票")
        if args.refresh:
            print("缓存模式: 强制刷新热门股票缓存")
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
                success = run_analysis(None, args.refresh, args.refresh_filter, args.custom_only, args.no_filter, args.source)  # 分析股票
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
                time.sleep(10)  # 异常时等待10秒再继续

if __name__ == "__main__":
    sys.exit(main())
