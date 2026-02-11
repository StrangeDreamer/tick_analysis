#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据收集脚本 - 用于回测和权重优化

使用方法：
1. 每天开市时间运行一次
2. 记录Top30股票的各项指标
3. 第二天记录T+1收益
4. 1-2个月后分析数据优化权重
"""

import json
import pandas as pd
from datetime import datetime, timedelta
import os
from quant_analysis import QuantAnalysis

class DataCollector:
    def __init__(self):
        self.data_file = "backtest_data.json"
        self.analyzer = QuantAnalysis()
    
    def collect_today_data(self):
        """收集今天的股票数据"""
        print(f"\n{'='*60}")
        print(f"📊 数据收集 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}\n")
        
        # 运行分析
        results = self.analyzer.analyze_stocks()
        
        if not results:
            print("❌ 没有分析结果")
            return
        
        # 准备今日数据
        today = datetime.now().strftime('%Y-%m-%d')
        today_data = {
            'date': today,
            'stocks': []
        }
        
        # 只保存Top30
        for symbol, data in results[:30]:
            stock_record = {
                'symbol': symbol,
                'name': data['name'],
                'score': data['score'],
                # 核心指标
                'relative_net_buy': data.get('relative_net_buy', 0),
                'total_volume': data.get('total_volume', 0),
                'pressure_ratio': data.get('pressure_ratio', 1.0),
                'large_buy_ratio': data.get('large_buy_ratio', 0),
                'large_sell_ratio': data.get('large_sell_ratio', 0),
                'active_buy_ratio': data.get('active_buy_ratio', 0.5),
                'momentum_ratio': data.get('momentum_ratio', 0),
                'closing_ratio': data.get('closing_ratio', 0),
                'momentum_acceleration': data.get('momentum_acceleration', 0),
                'sustainability': data.get('sustainability', 1.0),
                'excess_return': data.get('excess_return', 0),
                'kyle_lambda': data.get('kyle_lambda', 0),
                'effective_spread': data.get('effective_spread', 0),
                'buy_concentration': data.get('buy_concentration', 0),
                'wash_trade_ratio': data.get('wash_trade_ratio', 0),
                # 价格信息
                'current_price': data.get('current_price', 0),
                'intraday_change': data.get('intraday_change', 0),
                # T+1收益（待填）
                'T+1_return': None,
                'T+1_price': None
            }
            today_data['stocks'].append(stock_record)
        
        # 加载历史数据
        all_data = self._load_data()
        
        # 添加今日数据
        all_data.append(today_data)
        
        # 保存数据
        self._save_data(all_data)
        
        print(f"\n✅ 收集完成！共保存 {len(today_data['stocks'])} 只股票")
        print(f"📁 数据文件：{self.data_file}")
        print(f"📈 历史数据：{len(all_data)} 个交易日\n")
    
    def update_yesterday_returns(self):
        """更新昨天股票的T+1收益"""
        all_data = self._load_data()
        
        if len(all_data) < 2:
            print("⚠️ 历史数据不足，无法更新T+1收益")
            return
        
        yesterday_data = all_data[-2]  # 倒数第二天
        today_data = all_data[-1]  # 今天
        
        # 构建今日价格字典
        today_prices = {
            stock['symbol']: stock['current_price'] 
            for stock in today_data['stocks']
        }
        
        # 更新昨天的T+1收益
        updated_count = 0
        for stock in yesterday_data['stocks']:
            symbol = stock['symbol']
            if symbol in today_prices and stock['current_price'] > 0:
                yesterday_price = stock['current_price']
                today_price = today_prices[symbol]
                
                # 计算T+1收益率
                t1_return = ((today_price - yesterday_price) / yesterday_price) * 100
                
                stock['T+1_price'] = today_price
                stock['T+1_return'] = round(t1_return, 2)
                updated_count += 1
        
        # 保存更新后的数据
        self._save_data(all_data)
        
        print(f"✅ 更新完成！{yesterday_data['date']} 的 {updated_count} 只股票T+1收益已更新")
    
    def _load_data(self):
        """加载历史数据"""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return []
        return []
    
    def _save_data(self, data):
        """保存数据"""
        with open(self.data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def export_to_csv(self):
        """导出为CSV格式，方便分析"""
        all_data = self._load_data()
        
        if not all_data:
            print("❌ 没有数据可导出")
            return
        
        # 展开所有股票数据
        rows = []
        for day_data in all_data:
            date = day_data['date']
            for stock in day_data['stocks']:
                row = {
                    'date': date,
                    **stock
                }
                rows.append(row)
        
        # 转换为DataFrame
        df = pd.DataFrame(rows)
        
        # 保存为CSV
        csv_file = "backtest_data.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        print(f"✅ 已导出到 {csv_file}")
        print(f"📊 总计 {len(df)} 条记录")
        
        # 显示统计信息
        complete_data = df[df['T+1_return'].notna()]
        if len(complete_data) > 0:
            print(f"\n📈 已有T+1收益数据：{len(complete_data)} 条")
            print(f"   平均T+1收益：{complete_data['T+1_return'].mean():.2f}%")
            print(f"   最大T+1收益：{complete_data['T+1_return'].max():.2f}%")
            print(f"   最小T+1收益：{complete_data['T+1_return'].min():.2f}%")


def main():
    collector = DataCollector()
    
    print("\n" + "="*60)
    print("📊 数据收集脚本 V1.0")
    print("="*60)
    print("\n选择操作：")
    print("1. 收集今日数据")
    print("2. 更新昨日T+1收益")
    print("3. 导出数据到CSV")
    print("4. 全部执行（推荐）")
    
    choice = input("\n请输入选项 (1-4): ").strip()
    
    if choice == "1":
        collector.collect_today_data()
    elif choice == "2":
        collector.update_yesterday_returns()
    elif choice == "3":
        collector.export_to_csv()
    elif choice == "4":
        # 先更新昨日，再收集今日
        print("\n步骤1: 更新昨日T+1收益")
        collector.update_yesterday_returns()
        print("\n步骤2: 收集今日数据")
        collector.collect_today_data()
        print("\n步骤3: 导出到CSV")
        collector.export_to_csv()
    else:
        print("❌ 无效选项")


if __name__ == "__main__":
    main()
