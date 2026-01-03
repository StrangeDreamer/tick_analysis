#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化分析系统：热门股票分析
"""

import os
import warnings
import sys
import random

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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        self.max_workers = 10
        self.cache_file = "hot_stocks_cache.json"

    def get_accurate_previous_close(self, symbol):
        """通过实时接口获取准确的昨日收盘价（已考虑复权因子）"""
        try:
            clean_code = symbol.replace('SH', '').replace('SZ', '')
            df = ak.stock_zh_a_spot_em()
            row = df[df['代码'] == clean_code]
            if not row.empty:
                return float(row['昨收'].iloc[0])
        except:
            pass
        return None

    def _get_stock_name_by_code(self, code):
        """根据股票代码获取股票名称"""
        spot_df = ak.stock_zh_a_spot()
        if spot_df is not None and not spot_df.empty:
            stock_row = spot_df[spot_df['代码'] == code]
            if not stock_row.empty and '名称' in stock_row.columns:
                return stock_row['名称'].iloc[0]
        
        try:
            info_df = ak.stock_individual_info_em(symbol=code)
            if info_df is not None and not info_df.empty:
                name_row = info_df[info_df['item'] == '股票简称']
                if not name_row.empty:
                    stock_name = name_row['value'].iloc[0]
                    if stock_name and pd.notna(stock_name):
                        return str(stock_name).strip()
        except:
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

    def _get_single_stock_realtime_info(self, symbol):
        """获取单只股票的实时价格"""
        clean_symbol = symbol.replace('SH', '').replace('SZ', '')
        
        try:
            minute_symbol = f'sh{clean_symbol}' if clean_symbol.startswith('6') else f'sz{clean_symbol}'
            minute_df = ak.stock_zh_a_minute(symbol=minute_symbol, period='1', adjust='qfq')
            if minute_df is not None and not minute_df.empty and 'close' in minute_df.columns:
                return {'最新价': float(minute_df['close'].iloc[-1])}
        except:
            pass
        
        try:
            hist_df = ak.stock_zh_a_hist(symbol=clean_symbol, period='daily', adjust='qfq')
            if hist_df is not None and not hist_df.empty:
                return {'最新价': float(hist_df['收盘'].iloc[-1])}
        except:
            pass
        
        return {'最新价': 10.0}

    def get_stock_price_batch(self, stock_codes):
        """批量获取股票价格（使用实时行情接口，一次性获取所有股票）"""
        if not stock_codes:
            return {}, {}
        
        print(f"💰 开始获取 {len(stock_codes)} 只股票的价格（使用 ak.stock_zh_a_spot_em() 接口，一次性获取）...")
        
        price_data = {}
        previous_close_data = {}  # 上一交易日收盘价数据
        successful_count = 0
        failed_count = 0
        
        code_map = { (code[2:] if code.startswith(('SH', 'SZ')) else code): code for code in stock_codes }
        
        try:
            spot_df = ak.stock_zh_a_spot_em()
            
            if spot_df is not None and not spot_df.empty:
                for pure_code, full_code in code_map.items():
                    try:
                        stock_row = spot_df[spot_df['代码'] == pure_code]
                        
                        if not stock_row.empty:
                            price = None
                            price_keys = ['最新价', '现价', 'current_price', 'price']
                            for k in price_keys:
                                if k in stock_row.columns:
                                    try:
                                        price = float(stock_row[k].iloc[0])
                                        break
                                    except: continue
                            
                            previous_close = None
                            close_keys = ['昨收', 'pre_close', 'yesterday_close', '前收盘']
                            for k in close_keys:
                                if k in stock_row.columns:
                                    try:
                                        previous_close = float(stock_row[k].iloc[0])
                                        break
                                    except: continue
                            
                            if price is not None:
                                price_data[full_code] = price
                                previous_close_data[full_code] = previous_close if previous_close is not None else price
                                successful_count += 1
                            else:
                                failed_count += 1
                        else:
                            failed_count += 1
                    except Exception:
                        failed_count += 1
                
                print(f"📊 价格获取完成: 成功 {successful_count} 只，失败 {failed_count} 只")
            else:
                print(f"❌ 无法获取实时行情数据")
            return price_data, previous_close_data
        except Exception as e:
            print(f"❌ 获取价格数据失败: {e}")
            return {}, {}

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

        # 定义筛选条件
        is_main_board = hot_rank_df['代码'].str.startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605'))
        is_not_st = ~hot_rank_df['股票名称'].str.contains('ST')
        is_price_ok = (hot_rank_df['最新价'] >= 5) & (hot_rank_df['最新价'] <= 30)
        
        # 应用筛选
        filtered_stocks_df = hot_rank_df[is_main_board & is_not_st & is_price_ok].copy()
        
        print(f"📊 筛选结果: {len(hot_rank_df)}只 → {len(filtered_stocks_df)}只")
        print(f"   - 沪深主板: ✓")
        print(f"   - 非ST股票: ✓")
        print(f"   - 价格5-30元: ✓")

        # 找出被剔除的股票并分析原因
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
        
        # 转换为字典格式
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
        """获取股票的tick数据"""
        if symbol.startswith(('SH', 'SZ')):
            tick_symbol = symbol.lower()
        elif symbol.startswith('6'):
            tick_symbol = f'sh{symbol}'
        elif symbol.startswith(('0', '3')):
            tick_symbol = f'sz{symbol}'
        else:
            tick_symbol = symbol
        
        print(f"  获取 {symbol} ({tick_symbol}) 的tick数据...")
        
        try:
            tick_df = ak.stock_zh_a_tick_tx_js(symbol=tick_symbol)
        except Exception as e:
            print(f"  ❌ 获取 {symbol} tick数据时出错: {e}")
            return None

        if tick_df is None or tick_df.empty:
            print(f"  ❌ {symbol} 未获取到tick数据")
            return None

        print(f"  成功获取 {len(tick_df)} 条tick数据")
        
        tick_df = tick_df.rename(columns={
            '成交时间': '时间', '成交价格': '成交价', '价格变动': '价格变动',
            '成交量': '成交量', '成交金额': '成交额', '性质': '买卖盘性质'
        })
        
        tick_df['时间'] = pd.to_datetime(tick_df['时间'])
        tick_df = tick_df.sort_values('时间')
        
        tick_df['dp'] = tick_df['价格变动']
        tick_df['w1'] = np.tanh(np.abs(tick_df['dp']) / 0.01) * np.sign(tick_df['dp'])
        tick_df['meanV'] = tick_df['成交量'].rolling(20, min_periods=1).mean()
        tick_df['w2'] = np.minimum(1, tick_df['成交量'] / (3 * tick_df['meanV']))
        alpha = 2 / 6
        tick_df['prob'] = (tick_df['w1'] * tick_df['w2']).ewm(alpha=alpha, adjust=False).mean()
        tick_df['mf'] = tick_df['prob'] * tick_df['成交额']
        
        tick_df['买卖盘性质'] = np.where(tick_df['mf'] < 0, '卖盘', '买盘')
        tick_df['成交量'] = (np.abs(tick_df['mf']) / tick_df['成交价'] / 100).round().astype(int)
        tick_df['成交额'] = np.abs(tick_df['mf']).round().astype(int)
        
        original_len = len(tick_df)
        tick_df = tick_df[tick_df['成交量'] > 0].copy()
        if original_len > len(tick_df):
            print(f"  过滤无效数据: {original_len}条 → {len(tick_df)}条")
        
        if tick_df.empty:
            print(f"  ⚠️ {symbol} 过滤后数据为空，返回None")
            return None
        
        # 打印最新的5条tick数据
        print(f"  最新5条Tick数据 for {symbol}:")
        for _, row in tick_df.tail(5).iterrows():
            print(f"    {row['时间'].strftime('%H:%M:%S')} - 价格: {row['成交价']:.2f}, 成交量: {row['成交量']}手, 性质: {row['买卖盘性质']}")

        return tick_df[['时间', '成交价', '成交量', '成交额', '买卖盘性质', 'meanV', 'w2', 'prob', 'mf']]

    def get_tick_data_worker(self, symbol):
        """多线程工作函数：获取单只股票的tick数据"""
        return symbol, self.get_tick_data(symbol)

    def get_tick_data_batch(self, symbols, max_workers=10):
        print(f"🚀 开始多线程获取 {len(symbols)} 只股票的tick数据（{max_workers}个线程）...")
        tick_data_results = {}
        successful_count = 0
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {executor.submit(self.get_tick_data_worker, symbol): symbol for symbol in symbols}
            
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    _, tick_df = future.result()
                    if tick_df is not None:
                        tick_data_results[symbol] = tick_df
                        successful_count += 1
                    else:
                        failed_count += 1
                except Exception:
                    failed_count += 1
        
        print(f"📊 批量获取完成: 成功 {successful_count} 只，失败 {failed_count} 只")
        return tick_data_results

    def analyze_trade_direction(self, tick_df, symbol):
        """分析股票的主动买卖性质"""
        if tick_df is None or tick_df.empty:
            return {'buy_ratio': 0, 'sell_ratio': 0, 'net_buy_volume': 0, 'active_buy_ratio': 0, 'active_sell_ratio': 0, 'buy_volume': 0, 'sell_volume': 0, 'total_trades': 0}
        
        total_trades = len(tick_df)
        buy_mask = tick_df['买卖盘性质'] == '买盘'
        sell_mask = tick_df['买卖盘性质'] == '卖盘'
        
        buy_count = buy_mask.sum()
        sell_count = sell_mask.sum()
        
        buy_volume = tick_df.loc[buy_mask, '成交量'].sum()
        sell_volume = tick_df.loc[sell_mask, '成交量'].sum()
        total_volume = buy_volume + sell_volume
        
        return {
            'buy_ratio': buy_count / total_trades if total_trades > 0 else 0,
            'sell_ratio': sell_count / total_trades if total_trades > 0 else 0,
            'net_buy_volume': buy_volume - sell_volume,
            'active_buy_ratio': buy_volume / total_volume if total_volume > 0 else 0.5,
            'active_sell_ratio': sell_volume / total_volume if total_volume > 0 else 0.5,
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'total_trades': total_trades
        }

    def calculate_score(self, symbol, tick_df, trade_direction):
        """计算股票上涨概率得分"""
        if tick_df is None or tick_df.empty:
            return 0
            
        active_buy_ratio = trade_direction['active_buy_ratio']
        buy_sell_score = (active_buy_ratio - 0.5) * 2 * 70  # Scale to [-70, 70]
        
        net_buy_volume = trade_direction['net_buy_volume']
        avg_volume = tick_df['成交量'].mean()
        net_buy_score = 0
        if avg_volume > 0:
            net_buy_score = np.clip(net_buy_volume / (avg_volume * 10), -15, 15) * 2 # Scale to [-30, 30]
        
        score = buy_sell_score * 0.7 + net_buy_score * 0.3
        return score

    def analyze_stock_worker(self, stock, tick_df):
        """分析单个股票的工作函数（计算交易方向和得分）"""
        symbol = stock['代码']
        name = stock['股票名称']
        
        intraday_change = 0.0
        if tick_df is not None and not tick_df.empty:
            first_price = float(tick_df['成交价'].iloc[0])
            last_price = float(tick_df['成交价'].iloc[-1])
            if first_price > 0:
                intraday_change = ((last_price - first_price) / first_price) * 100
        
        trade_direction = self.analyze_trade_direction(tick_df, symbol)
        score = self.calculate_score(symbol, tick_df, trade_direction)
        
        return (symbol, {
            'name': name, 'score': score, 'trade_direction': trade_direction,
            'tick_df': tick_df, 'intraday_change': intraday_change
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
        
        # 合并和排序
        for symbol, analysis in analysis_results.items():
            self.tick_data[symbol] = analysis['tick_df']
            self.trade_directions[symbol] = analysis['trade_direction']
            self.scores[symbol] = {
                'name': analysis['name'], 'score': analysis['score'],
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
            
            text += f"""### {i}. {symbol} {data['name']}
- **得分**: {data['score']:.2f}
- **股价**: {stock_price}
- **日内涨跌幅**: {intraday_change:.2f}%
- **主动买入强度**: {trade_direction['active_buy_ratio']:.1%}
- **净买入量**: {trade_direction['net_buy_volume']:,.0f}

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
