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
import time

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
import argparse

class QuantAnalysis:
    def __init__(self):
        self.hot_stocks = []
        self.tick_data = {}
        self.trade_directions = {}
        self.scores = {}
        self.max_workers = 5
        self.hot_stocks_cache_file = "hot_stocks_cache.json"
        self.historical_metrics_cache_file = "historical_metrics_cache.json"

    def _get_market_performance(self):
        """获取大盘表现作为基准"""
        try:
            # 使用您指定的、正确的接口获取上证指数数据
            market_df = ak.stock_individual_spot_xq(symbol="SH000001")
            
            # 从返回的DataFrame中正确提取“涨幅”
            change_row = market_df[market_df['item'] == '涨幅']
            
            if not change_row.empty:
                market_change_pct = change_row['value'].iloc[0]
                print(f"📈 大盘基准 (上证指数): {market_change_pct:.2f}%")
                return float(market_change_pct)
            else:
                print("⚠️ 在返回数据中未找到'涨幅'项")
                return 0.0
        except Exception as e:
            print(f"⚠️ 无法获取大盘表现: {e}")
        return 0.0

    def _get_historical_data(self, symbol):
        """获取单个股票的历史数据用于计算ADV和ATR"""
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=40)).strftime('%Y%m%d')
            # 获取纯代码
            pure_code = symbol[2:]
            
            hist_df = ak.stock_zh_a_hist(symbol=pure_code, start_date=start_date, end_date=end_date, adjust="qfq")
            
            if hist_df is None or len(hist_df) < 21:
                return None

            # 计算ADV20 (20日平均成交量，单位：手)
            adv20 = hist_df['成交量'].rolling(window=20).mean().iloc[-1]
            
            # 计算ATR20 (20日平均真实波幅)
            high_low = hist_df['最高'] - hist_df['最低']
            high_prev_close = np.abs(hist_df['最高'] - hist_df['收盘'].shift())
            low_prev_close = np.abs(hist_df['最低'] - hist_df['收盘'].shift())
            
            tr = np.max(pd.DataFrame({'hl': high_low, 'hpc': high_prev_close, 'lpc': low_prev_close}), axis=1)
            atr20 = tr.rolling(window=20).mean().iloc[-1]
            
            return {'adv20': adv20, 'atr20': atr20}
        except Exception:
            return None

    def _get_historical_data_batch(self, symbols):
        """批量获取历史数据，带缓存"""
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        if os.path.exists(self.historical_metrics_cache_file):
            try:
                with open(self.historical_metrics_cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    if cache_data.get('date') == today_str:
                        print("✅ 从缓存加载历史参照指标 (ADV, ATR)")
                        return cache_data.get('metrics', {})
            except (json.JSONDecodeError, IOError):
                print("⚠️ 历史参照指标缓存文件损坏，将重新获取")

        print("🔄 批量获取历史参照指标 (ADV, ATR)...")
        historical_metrics = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_symbol = {executor.submit(self._get_historical_data, symbol): symbol for symbol in symbols}
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result(timeout=10)
                    if result:
                        historical_metrics[symbol] = result
                except Exception:
                    pass
        
        try:
            with open(self.historical_metrics_cache_file, 'w', encoding='utf-8') as f:
                json.dump({'date': today_str, 'metrics': historical_metrics}, f, ensure_ascii=False, indent=4)
            print(f"💾 历史参照指标已缓存至 {self.historical_metrics_cache_file}")
        except IOError as e:
            print(f"❌ 缓存历史参照指标失败: {e}")
            
        return historical_metrics

    def get_hot_stocks(self):
        """获取当日最热的沪深主板非ST A股股票，带每日缓存"""
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        if os.path.exists(self.hot_stocks_cache_file):
            try:
                with open(self.hot_stocks_cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    if cache_data.get('date') == today_str:
                        print("✅ 从缓存加载热门股票列表")
                        return cache_data.get('stocks', [])
            except (json.JSONDecodeError, IOError):
                print("⚠️ 热门股票缓存文件损坏，将重新获取")

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
        
        final_stocks = filtered_stocks_df.to_dict('records')
        
        if final_stocks:
            try:
                with open(self.hot_stocks_cache_file, 'w', encoding='utf-8') as f:
                    json.dump({'date': today_str, 'stocks': final_stocks}, f, ensure_ascii=False, indent=4)
                print(f"💾 热门股票列表已缓存至 {self.hot_stocks_cache_file}")
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
        
        try:
            tick_df = ak.stock_zh_a_tick_tx_js(symbol=tick_symbol)
        except Exception as e:
            raise e

        if tick_df is None or tick_df.empty:
            return None
        
        tick_df = tick_df.rename(columns={
            '成交时间': '时间', '成交价格': '成交价', '成交量': '成交量', 
            '性质': '买卖盘性质', '价格变动': '价格变动'
        })
        
        tick_df = tick_df[['时间', '成交价', '成交量', '买卖盘性质', '价格变动']]
        tick_df['时间'] = pd.to_datetime(tick_df['时间'])
        tick_df = tick_df.sort_values('时间')
        
        tick_df = tick_df[tick_df['买卖盘性质'].isin(['买盘', '卖盘'])].copy()
        tick_df['成交量'] = tick_df['成交量'].astype(int)
        
        tick_df.loc[tick_df['成交量'] > 0, 'price_impact'] = tick_df['价格变动'] / tick_df['成交量']
        tick_df['price_impact'].fillna(0, inplace=True)

        tick_df = tick_df[tick_df['成交量'] > 0].copy()
        
        if tick_df.empty:
            return None
        
        print(f"\n  最新5条Tick数据 for {symbol}:")
        for _, row in tick_df.tail(5).iterrows():
            print(f"    {row['时间'].strftime('%H:%M:%S')} - 价格: {row['成交价']:.2f}, 成交量: {row['成交量']}手, 性质: {row['买卖盘性质']}")

        return tick_df

    def get_tick_data_worker(self, symbol):
        """多线程工作函数：获取单只股票的tick数据"""
        return symbol, self.get_tick_data(symbol)

    def get_tick_data_batch(self, symbols, max_workers=5):
        print(f"🚀 开始多线程获取 {len(symbols)} 只股票的tick数据（{max_workers}个线程）...")
        tick_data_results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {executor.submit(self.get_tick_data_worker, symbol): symbol for symbol in symbols}
            
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    _, tick_df = future.result(timeout=15)
                    if tick_df is not None:
                        tick_data_results[symbol] = tick_df
                except TimeoutError:
                    print(f"  ❌ {symbol} 获取tick数据超时")
                except Exception as e:
                    print(f"  ❌ {symbol} 获取tick数据时发生错误: {e}")
        
        return tick_data_results

    def analyze_trade_direction(self, tick_df):
        """分析股票的主动买卖性质"""
        if tick_df is None or tick_df.empty: return {}
        
        buy_mask = tick_df['买卖盘性质'] == '买盘'
        buy_volume = tick_df.loc[buy_mask, '成交量'].sum()
        sell_volume = tick_df.loc[~buy_mask, '成交量'].sum()
        total_volume = buy_volume + sell_volume
        
        return {
            'net_buy_volume': buy_volume - sell_volume,
            'active_buy_ratio': buy_volume / total_volume if total_volume > 0 else 0.5,
        }

    def calculate_score(self, trade_direction, afternoon_net_buy_volume, avg_abs_impact, excess_return, adv20, atr20):
        """计算股票上涨概率得分 (V4 - 引入历史参照)"""
        
        active_buy_ratio = trade_direction.get('active_buy_ratio', 0.5)
        buy_sell_score = (active_buy_ratio - 0.5) * 2 * 60
        
        net_buy_volume = trade_direction.get('net_buy_volume', 0)
        net_buy_adv_ratio = (net_buy_volume / adv20) if adv20 > 0 else 0
        net_buy_score = np.clip(net_buy_adv_ratio / 0.1 * 20, -20, 20)

        impact_atr_ratio = (avg_abs_impact / atr20) if atr20 > 0 else 0
        impact_score = 20 - (impact_atr_ratio / 0.1) * 40
        impact_score = np.clip(impact_score, -20, 20)
        
        if active_buy_ratio > 0.7 and impact_score < 0: impact_score /= 2
        elif active_buy_ratio < 0.5 and impact_score < 0: impact_score *= 1.5
        impact_score = np.clip(impact_score, -20, 20)

        momentum_score = 0
        if net_buy_volume > 0 and afternoon_net_buy_volume > 0:
            afternoon_ratio = afternoon_net_buy_volume / net_buy_volume
            if afternoon_ratio > 0.6:
                momentum_score = 10 * min((afternoon_ratio - 0.6) / 0.4, 1.0)
        elif net_buy_volume > 0 and afternoon_net_buy_volume < 0:
            momentum_score = -10
            
        resonance_bonus = 10 if buy_sell_score > 50 and net_buy_score > 15 else 0
        alpha_score = np.clip(excess_return / 2 * 10, -10, 10)

        total_score = buy_sell_score + net_buy_score + impact_score + momentum_score + resonance_bonus + alpha_score
        
        return {
            'score': np.clip(total_score, -100, 100),
            'net_buy_adv_ratio': net_buy_adv_ratio,
            'impact_atr_ratio': impact_atr_ratio
        }

    def analyze_stock_worker(self, stock, tick_df, market_performance, historical_metrics):
        """分析单个股票的工作函数"""
        symbol = stock['代码']
        name = stock['股票名称']
        
        intraday_change = 0.0
        if not tick_df.empty:
            first_price = float(tick_df['成交价'].iloc[0])
            last_price = float(tick_df['成交价'].iloc[-1])
            if first_price > 0:
                intraday_change = ((last_price - first_price) / first_price) * 100
        
        excess_return = intraday_change - market_performance
        
        trade_direction = self.analyze_trade_direction(tick_df)
        
        afternoon_ticks = tick_df[tick_df['时间'].dt.time >= pd.to_datetime('13:00:00').time()]
        afternoon_trade_direction = self.analyze_trade_direction(afternoon_ticks)
        afternoon_net_buy_volume = afternoon_trade_direction.get('net_buy_volume', 0)
        
        avg_abs_impact = tick_df['price_impact'].abs().mean() if 'price_impact' in tick_df.columns else 0

        adv20 = historical_metrics.get('adv20', 0)
        atr20 = historical_metrics.get('atr20', 0)

        score_info = self.calculate_score(trade_direction, afternoon_net_buy_volume, avg_abs_impact, excess_return, adv20, atr20)
        
        return (symbol, {
            'name': name, 
            'score': score_info['score'], 
            'net_buy_adv_ratio': score_info['net_buy_adv_ratio'],
            'impact_atr_ratio': score_info['impact_atr_ratio'],
            'trade_direction': trade_direction,
            'intraday_change': intraday_change,
            'excess_return': excess_return
        })

    def analyze_stocks(self):
        """分析所有热门股票"""
        market_performance = self._get_market_performance()
        all_stocks = self.get_combined_stocks()
        
        if not all_stocks: return []
        
        symbols = [stock['代码'] for stock in all_stocks]
        historical_metrics = self._get_historical_data_batch(symbols)
        
        # 礼貌性延迟，防止API限流
        print("⏳ 礼貌性延迟5秒，防止触发API限流...")
        time.sleep(5)

        print(f"📊 步骤1/3: 批量获取 {len(symbols)} 只股票的Tick数据...")
        tick_data_results = self.get_tick_data_batch(symbols, max_workers=self.max_workers)
        
        valid_stocks = []
        stock_dict = {stock['代码']: stock for stock in all_stocks}
        for symbol, tick_df in tick_data_results.items():
            if tick_df is not None and not tick_df.empty and symbol in historical_metrics:
                valid_stocks.append((stock_dict[symbol], tick_df, historical_metrics[symbol]))
            else:
                print(f"  ❌ {symbol} 无有效tick数据或历史数据，跳过")
        
        if not valid_stocks: return []
        
        print(f"📊 步骤2/3: 批量分析交易方向和计算得分...")
        analysis_results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.analyze_stock_worker, stock, tick_df, market_performance, metrics) for stock, tick_df, metrics in valid_stocks]
            for future in as_completed(futures):
                try:
                    symbol, result = future.result()
                    if result:
                        analysis_results[symbol] = result
                        print(f"  ✅ {symbol} 分析完成，得分: {result['score']:.2f}")
                except Exception as e:
                    print(f"  ⚠️ 分析任务异常: {e}")
        
        for symbol, analysis in analysis_results.items():
            self.scores[symbol] = analysis
        
        sorted_stocks = sorted(self.scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        print(f"📊 步骤3/3: 筛选最终结果...")
        final_stocks = []
        for symbol, data in sorted_stocks:
            if data['trade_direction'].get('active_buy_ratio', 0) < 1.0 and data.get('intraday_change', 0.0) <= 6.0:
                final_stocks.append((symbol, data))
        
        print(f"✅ 分析完成，最终筛选出 {len(final_stocks)} 只股票")
        return final_stocks

    def send_dingtalk_message(self, top_stocks):
        """发送钉钉消息"""
        webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=ae055118615b242c6fe43fc3273a228f316209f707d07e7ce39fc83f4270ed82"
        secret = "SECf2b2861525388e240846ad1e2beb3b93d3b5f0d2e6634e43176b593f050e77da"
        
        stocks_to_send = top_stocks[:50]
        if not stocks_to_send: return False
        
        text = f"# 📈 量化分析报告 - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        text += f"## 🏆 股票评分排序 (Top {len(stocks_to_send)})\n\n"
        
        for i, (symbol, data) in enumerate(stocks_to_send, 1):
            trade_direction = data['trade_direction']
            
            text += f"""### {i}. {data['name']} ({symbol})
- **得分**: **{data['score']:.2f}**
- **日内涨跌**: {data.get('intraday_change', 0.0):.2f}% (超额: {data.get('excess_return', 0.0):.2f}%)
- **主动买入强度**: {trade_direction.get('active_buy_ratio', 0.0):.1%}
- **净买入占比 (vs ADV20)**: {data.get('net_buy_adv_ratio', 0.0):.2%}
- **价格冲击 (vs ATR20)**: {data.get('impact_atr_ratio', 0.0):.2%}
"""
        
        message = {"msgtype": "markdown", "markdown": {"title": "量化分析报告", "text": text}}
        
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{secret}"
        hmac_code = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
        sign = base64.b64encode(hmac_code).decode('utf-8')
        full_webhook_url = f"{webhook_url}&timestamp={timestamp}&sign={sign}"
        
        try:
            response = requests.post(full_webhook_url, json=message, timeout=10)
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
