#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化分析系统：热门股票分析
"""

import os
import warnings
import sys
import random
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

# 在导入其他库之前抑制所有警告（包括 urllib3 的 OpenSSL 警告）
warnings.filterwarnings('ignore')
# 特别抑制 urllib3 相关的警告
warnings.filterwarnings('ignore', message='.*urllib3.*')
warnings.filterwarnings('ignore', message='.*OpenSSL.*')
warnings.filterwarnings('ignore', category=UserWarning)

import akshare as ak

# 在导入 akshare（会导入 urllib3）后，再次禁用 urllib3 的所有警告
try:
    import urllib3
    urllib3.disable_warnings()
except (ImportError, AttributeError):
    pass

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json
import threading
import hashlib
import base64
import hmac
import time
import argparse

class QuantAnalysis:
    def __init__(self):
        self.hot_stocks = []
        self.tick_data = {}
        self.trade_directions = {}
        self.scores = {}
        self.max_workers = 5  # 降低并发数
        self.cache_file = "hot_stocks_cache.json"

    def _get_stock_name_by_code(self, code):
        """根据股票代码获取股票名称"""
        try:
            spot_df = ak.stock_zh_a_spot()
            if spot_df is not None and not spot_df.empty:
                stock_row = spot_df[spot_df['代码'] == code]
                if not stock_row.empty and '名称' in stock_row.columns:
                    return stock_row['名称'].iloc[0]
        except Exception:
            pass
        
        try:
            info_df = ak.stock_individual_info_em(symbol=code)
            if info_df is not None and not info_df.empty:
                name_row = info_df[info_df['item'] == '股票简称']
                if not name_row.empty:
                    stock_name = name_row['value'].iloc[0]
                    if stock_name and pd.notna(stock_name):
                        return str(stock_name).strip()
        except Exception:
            pass
        
        return f'股票{code}'

    def _fill_missing_stock_names(self, stocks):
        """填充股票列表中缺失的股票名称"""
        if not stocks:
            return stocks
        
        filled_count = 0
        for stock in stocks:
            code = stock.get('代码', '')
            if not code:
                continue
            
            pure_code = code[2:] if code.startswith(('SH', 'SZ')) else code
            stock_name = stock.get('股票名称', '')
            
            if not stock_name or stock_name == f'股票{pure_code}' or stock_name.startswith('股票'):
                new_name = self._get_stock_name_by_code(pure_code)
                if new_name and new_name != f'股票{pure_code}':
                    stock['股票名称'] = new_name
                    filled_count += 1
        
        if filled_count > 0:
            print(f"📝 已填充 {filled_count} 只股票的缺失名称")
        
        return stocks

    def get_hot_stocks(self):
        """获取当日最热的沪深主板非ST A股股票，带每日缓存"""
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    if cache_data.get('date') == today_str:
                        print("✅ 从缓存加载热门股票列表")
                        return cache_data.get('stocks', [])
            except (json.JSONDecodeError, IOError):
                print("⚠️ 缓存文件损坏，将重新获取")

        print("🔄 从API获取热门股票排行榜...")
        
        try:
            hot_rank_df = ak.stock_hot_rank_em()
        except Exception as e:
            print(f"❌ 获取热门股票排行榜失败: {e}")
            return []

        if hot_rank_df is None or hot_rank_df.empty:
            print("❌ 热门股票排行榜数据为空")
            return []

        is_main_board = hot_rank_df['代码'].str.startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605'))
        is_not_st = ~hot_rank_df['股票名称'].str.contains('ST')
        is_price_ok = (hot_rank_df['最新价'] >= 5) & (hot_rank_df['最新价'] <= 30)
        
        filtered_stocks_df = hot_rank_df[is_main_board & is_not_st & is_price_ok].copy()
        
        print(f"📊 筛选结果: {len(hot_rank_df)}只 → {len(filtered_stocks_df)}只")
        print(f"   - 沪深主板: ✓")
        print(f"   - 非ST股票: ✓")
        print(f"   - 价格5-30元: ✓")

        rejected_df = hot_rank_df[~(is_main_board & is_not_st & is_price_ok)]
        if not rejected_df.empty:
            print("\n🔍 被剔除股票随机抽样分析:")
            sample_size = min(5, len(rejected_df))
            for _, row in rejected_df.sample(n=sample_size).iterrows():
                reasons = []
                if not row['代码'].startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605')):
                    reasons.append("非主板")
                if 'ST' in row['股票名称']:
                    reasons.append("ST股")
                if not (5 <= row['最新价'] <= 30):
                    reasons.append(f"价格({row['最新价']:.2f}元)不符")
                
                print(f"  - {row['代码']} {row['股票名称']}: 被剔除，原因: {', '.join(reasons)}")
        
        final_stocks = filtered_stocks_df.to_dict('records')
        final_stocks = self._fill_missing_stock_names(final_stocks)
        
        if final_stocks:
            print(f"\n✅ 获取{len(final_stocks)}只热门股票")
            print("🔥 热门股票（热门排行榜）:")
            for stock in final_stocks[:10]:
                print(f"  {stock['代码']} {stock['股票名称']} 价格:{stock.get('最新价', 'N/A')} 涨跌幅:{stock.get('涨跌幅', 'N/A')}%")
            if len(final_stocks) > 10:
                print(f"  ... 还有 {len(final_stocks) - 10} 只股票")
            
            try:
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    json.dump({'date': today_str, 'stocks': final_stocks}, f, ensure_ascii=False, indent=4)
                print(f"💾 热门股票列表已缓存至 {self.cache_file}")
            except IOError as e:
                print(f"❌ 缓存热门股票列表失败: {e}")
        else:
            print("⚠️ 筛选后热门股票为空")
            
        return final_stocks

    def get_combined_stocks(self):
        """获取股票列表（仅热门股票）"""
        hot_stocks = self.get_hot_stocks()
        
        seen_symbols = set()
        unique_stocks = []
        for stock in hot_stocks:
            if stock['代码'] not in seen_symbols:
                unique_stocks.append(stock)
                seen_symbols.add(stock['代码'])
        
        print(f"✅ 共获取 {len(unique_stocks)} 只待分析股票")
        return unique_stocks

    def get_tick_data(self, symbol, date=None):
        """获取并处理股票的tick数据"""
        tick_symbol = symbol.lower() if symbol.startswith(('SH', 'SZ')) else (f'sh{symbol}' if symbol.startswith('6') else f'sz{symbol}')
        
        print(f"  获取 {symbol} ({tick_symbol}) 的tick数据...")
        
        try:
            tick_df = ak.stock_zh_a_tick_tx_js(symbol=tick_symbol)
        except Exception as e:
            raise e

        if tick_df is None or tick_df.empty:
            print(f"  ❌ {symbol} 未获取到tick数据")
            return None

        print(f"  成功获取 {len(tick_df)} 条原始tick数据")
        
        tick_df = tick_df.rename(columns={
            '成交时间': '时间', '成交价格': '成交价', '成交量': '成交量', 
            '性质': '买卖盘性质', '价格变动': '价格变动'
        })
        
        tick_df = tick_df[['时间', '成交价', '成交量', '买卖盘性质', '价格变动']]
        tick_df['时间'] = pd.to_datetime(tick_df['时间'])
        tick_df = tick_df.sort_values('时间')
        
        original_len = len(tick_df)
        tick_df = tick_df[tick_df['买卖盘性质'].isin(['买盘', '卖盘'])].copy()
        print(f"  过滤中性盘: {original_len}条 → {len(tick_df)}条")

        tick_df['成交量'] = tick_df['成交量'].astype(int)
        
        tick_df.loc[tick_df['成交量'] > 0, 'price_impact'] = tick_df['价格变动'] / tick_df['成交量']
        tick_df['price_impact'].fillna(0, inplace=True)

        original_len = len(tick_df)
        tick_df = tick_df[tick_df['成交量'] > 0].copy()
        if original_len > len(tick_df):
            print(f"  过滤成交量为0的记录: {original_len}条 → {len(tick_df)}条")
        
        if tick_df.empty:
            print(f"  ⚠️ {symbol} 过滤后数据为空，返回None")
            return None
        
        print(f"  最新5条Tick数据 for {symbol}:")
        for _, row in tick_df.tail(5).iterrows():
            print(f"    {row['时间'].strftime('%H:%M:%S')} - 价格: {row['成交价']:.2f}, 成交量: {row['成交量']}手, 性质: {row['买卖盘性质']}")

        return tick_df

    def get_tick_data_worker(self, symbol):
        """多线程工作函数：获取单只股票的tick数据"""
        return symbol, self.get_tick_data(symbol)

    def get_tick_data_batch(self, symbols, max_workers=5):
        print(f"🚀 开始多线程获取 {len(symbols)} 只股票的tick数据（{max_workers}个线程）...")
        tick_data_results = {}
        successful_count = 0
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {executor.submit(self.get_tick_data_worker, symbol): symbol for symbol in symbols}
            
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    _, tick_df = future.result(timeout=15)
                    if tick_df is not None:
                        tick_data_results[symbol] = tick_df
                        successful_count += 1
                    else:
                        failed_count += 1
                except TimeoutError:
                    print(f"  ❌ {symbol} 获取数据超时 (超过15秒)")
                    failed_count += 1
                except Exception as e:
                    print(f"  ❌ {symbol} 获取数据时发生错误: {e}")
                    failed_count += 1
        
        print(f"📊 批量获取完成: 成功 {successful_count} 只，失败 {failed_count} 只")
        return tick_data_results

    def analyze_trade_direction(self, tick_df, symbol):
        """分析股票的主动买卖性质"""
        if tick_df is None or tick_df.empty:
            return {}
        
        total_trades = len(tick_df)
        buy_mask = tick_df['买卖盘性质'] == '买盘'
        
        buy_count = buy_mask.sum()
        
        buy_volume = tick_df.loc[buy_mask, '成交量'].sum()
        sell_volume = tick_df.loc[~buy_mask, '成交量'].sum()
        total_volume = buy_volume + sell_volume
        
        return {
            'buy_ratio': buy_count / total_trades if total_trades > 0 else 0,
            'sell_ratio': (total_trades - buy_count) / total_trades if total_trades > 0 else 0,
            'net_buy_volume': buy_volume - sell_volume,
            'active_buy_ratio': buy_volume / total_volume if total_volume > 0 else 0.5,
            'active_sell_ratio': sell_volume / total_volume if total_volume > 0 else 0.5,
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'total_trades': total_trades
        }

    def calculate_score(self, trade_direction, afternoon_net_buy_volume, avg_abs_impact):
        """计算股票上涨概率得分 (V2 - 包含动量和智能价格冲击)"""
        
        # 1. 主动买入强度得分 (60%)
        active_buy_ratio = trade_direction.get('active_buy_ratio', 0.5)
        buy_sell_score = (active_buy_ratio - 0.5) * 2 * 60
        
        # 2. 净买入量得分 (20%)
        net_buy_volume = trade_direction.get('net_buy_volume', 0)
        total_volume = trade_direction.get('buy_volume', 0) + trade_direction.get('sell_volume', 0)
        net_buy_ratio = net_buy_volume / total_volume if total_volume > 0 else 0
        net_buy_score = np.clip(net_buy_ratio * 40, -20, 20)

        # 3. 平均价格冲击得分 (20%) - 智能调整
        impact_score = 20 - (avg_abs_impact / 0.05) * 40
        impact_score = np.clip(impact_score, -20, 20)
        
        if active_buy_ratio > 0.7 and impact_score < 0:
            impact_score /= 2
        elif active_buy_ratio < 0.5 and impact_score < 0:
            impact_score *= 1.5
        impact_score = np.clip(impact_score, -20, 20)

        # 4. 动量得分 (额外 +/-10分)
        momentum_score = 0
        full_day_net_buy = trade_direction.get('net_buy_volume', 0)
        if full_day_net_buy > 0 and afternoon_net_buy_volume > 0:
            afternoon_ratio = afternoon_net_buy_volume / full_day_net_buy
            if afternoon_ratio > 0.6:
                momentum_score = 10 * min((afternoon_ratio - 0.6) / 0.4, 1.0)
        elif full_day_net_buy > 0 and afternoon_net_buy_volume < 0:
            momentum_score = -10
            
        # 5. 共振奖励 (额外 +10分)
        resonance_bonus = 0
        if buy_sell_score > 50 and net_buy_score > 15:
            resonance_bonus = 10

        total_score = buy_sell_score + net_buy_score + impact_score + momentum_score + resonance_bonus
        
        return {
            'score': np.clip(total_score, -100, 100),
            'avg_abs_impact': avg_abs_impact
        }

    def analyze_stock_worker(self, stock, tick_df):
        """分析单个股票的工作函数（计算交易方向和得分）"""
        symbol = stock['代码']
        name = stock['股票名称']
        
        intraday_change = 0.0
        if not tick_df.empty:
            first_price = float(tick_df['成交价'].iloc[0])
            last_price = float(tick_df['成交价'].iloc[-1])
            if first_price > 0:
                intraday_change = ((last_price - first_price) / first_price) * 100
        
        trade_direction = self.analyze_trade_direction(tick_df, symbol)
        
        afternoon_start_time = pd.to_datetime('13:00:00').time()
        afternoon_ticks = tick_df[tick_df['时间'].dt.time >= afternoon_start_time]
        afternoon_trade_direction = self.analyze_trade_direction(afternoon_ticks, symbol)
        afternoon_net_buy_volume = afternoon_trade_direction.get('net_buy_volume', 0)
        
        avg_abs_impact = tick_df['price_impact'].abs().mean() if 'price_impact' in tick_df.columns else 0

        score_info = self.calculate_score(trade_direction, afternoon_net_buy_volume, avg_abs_impact)
        
        return (symbol, {
            'name': name, 
            'score': score_info['score'], 
            'avg_abs_impact': score_info['avg_abs_impact'],
            'trade_direction': trade_direction,
            'tick_df': tick_df, 
            'intraday_change': intraday_change
        })

    def analyze_stocks(self):
        """分析所有热门股票"""
        all_stocks = self.get_combined_stocks()
        
        if not all_stocks:
            print("❌ 没有股票需要分析")
            return []
        
        symbols = [stock['代码'] for stock in all_stocks]
        
        print(f"📊 步骤1/3: 批量获取 {len(symbols)} 只股票的Tick数据...")
        tick_data_results = self.get_tick_data_batch(symbols, max_workers=self.max_workers)
        
        valid_stocks = []
        stock_dict = {stock['代码']: stock for stock in all_stocks}
        for symbol, tick_df in tick_data_results.items():
            if tick_df is not None and not tick_df.empty:
                valid_stocks.append((stock_dict[symbol], tick_df))
            else:
                print(f"  ❌ {symbol} 无有效tick数据，跳过")
        
        if not valid_stocks:
            print("❌ 没有股票有有效的tick数据")
            return []
        
        print(f"✅ 步骤1完成: {len(valid_stocks)}/{len(symbols)} 只股票获取成功")
        
        print(f"📊 步骤2/3: 批量分析交易方向和计算得分...")
        analysis_results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.analyze_stock_worker, stock, tick_df) for stock, tick_df in valid_stocks]
            for future in as_completed(futures):
                try:
                    symbol, result = future.result()
                    if result:
                        analysis_results[symbol] = result
                        print(f"  ✅ {symbol} 分析完成，得分: {result['score']:.2f}")
                except Exception as e:
                    print(f"  ⚠️ 分析任务异常: {e}")
        
        print(f"✅ 步骤2完成: {len(analysis_results)} 只股票分析成功")
        
        for symbol, analysis in analysis_results.items():
            self.tick_data[symbol] = analysis['tick_df']
            self.trade_directions[symbol] = analysis['trade_direction']
            self.scores[symbol] = {
                'name': analysis['name'], 
                'score': analysis['score'],
                'avg_abs_impact': analysis['avg_abs_impact'],
                'trade_direction': analysis['trade_direction'],
                'intraday_change': analysis.get('intraday_change', 0.0)
            }
        
        sorted_stocks = sorted(self.scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        print(f"📊 步骤3/3: 筛选最终结果...")
        final_stocks = []
        for symbol, data in sorted_stocks:
            if data['trade_direction']['active_buy_ratio'] < 1.0 and data.get('intraday_change', 0.0) <= 6.0:
                final_stocks.append((symbol, data))
            else:
                print(f"  ❌ {symbol} {data['name']} 不符合最终条件，剔除 (主动买入强度: {data['trade_direction']['active_buy_ratio']:.1%}, 日内涨跌幅: {data.get('intraday_change', 0.0):.2f}%)")

        print(f"✅ 分析完成，最终筛选出 {len(final_stocks)} 只股票")
        return final_stocks

    def send_dingtalk_message(self, top_stocks):
        """发送钉钉消息"""
        webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=ae055118615b242c6fe43fc3273a228f316209f707d07e7ce39fc83f4270ed82"
        secret = "SECf2b2861525388e240846ad1e2beb3b93d3b5f0d2e6634e43176b593f050e77da"
        
        stocks_to_send = top_stocks[:50]
        if not stocks_to_send:
            print("⚠️ 没有股票可发送，不发送钉钉消息")
            return False
        
        print(f"📤 准备发送钉钉消息: {len(stocks_to_send)}只股票")
        
        text = f"# 📈 量化分析报告 - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        text += f"## 🏆 股票评分排序 (Top {len(stocks_to_send)})\n\n"
        
        for i, (symbol, data) in enumerate(stocks_to_send, 1):
            trade_direction = data['trade_direction']
            
            current_price = None
            if symbol in self.tick_data and not self.tick_data[symbol].empty:
                current_price = float(self.tick_data[symbol]['成交价'].iloc[-1])
            
            stock_price = f"{current_price:.2f}元" if current_price is not None else "N/A"
            
            intraday_change = data.get('intraday_change', 0.0)
            avg_abs_impact = data.get('avg_abs_impact', 0.0)
            
            text += f"""### {i}. {symbol} {data['name']}
- **得分**: {data['score']:.2f}
- **股价**: {stock_price}
- **日内涨跌幅**: {intraday_change:.2f}%
- **主动买入强度**: {trade_direction['active_buy_ratio']:.1%}
- **净买入量**: {trade_direction['net_buy_volume']:,.0f}
- **平均价格冲击**: {avg_abs_impact:.4f}

"""
        
        message = {"msgtype": "markdown", "markdown": {"title": "量化分析报告", "text": text}}
        
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{secret}"
        hmac_code = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
        sign = base64.b64encode(hmac_code).decode('utf-8')
        full_webhook_url = f"{webhook_url}&timestamp={timestamp}&sign={sign}"
        
        try:
            response = requests.post(full_webhook_url, json=message)
            if response.status_code == 200 and response.json().get("errcode") == 0:
                print("✅ 钉钉消息发送成功！")
                return True
            else:
                print(f"❌ 钉钉消息发送失败: {response.text}")
                return False
        except Exception as e:
            print(f"❌ 发送钉钉消息时出错: {e}")
            return False

    def run_analysis(self):
        """运行完整分析流程"""
        print("🔍 量化分析系统 - 开始分析热门股票")
        top_stocks = self.analyze_stocks()
        
        if not top_stocks:
            print("🤷 没有符合条件的股票可发送")
            return
        
        self.send_dingtalk_message(top_stocks)

def main():
    """主函数"""
    analyzer = QuantAnalysis()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
