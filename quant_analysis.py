#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化分析系统：热门股票分析 (模型 V7.4 - 优化对倒交易识别)
"""

import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import time
import itertools
import sys

# 在导入其他库之前抑制所有警告
warnings.filterwarnings('ignore')
import akshare as ak
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
import hashlib
import base64
import hmac

class QuantAnalysis:
    def __init__(self):
        self.max_workers = 10
        self.hot_stocks_cache_file = "hot_stocks_cache.json"
        self.historical_metrics_cache_file = "historical_metrics_cache.json"
        self.fund_flow_cache_file = "fund_flow_cache.json"

    def _get_market_performance(self):
        """获取大盘表现作为基准"""
        try:
            market_df = ak.stock_individual_spot_xq(symbol="SH000001")
            change_row = market_df[market_df['item'] == '涨幅']
            if not change_row.empty:
                market_change_pct = change_row['value'].iloc[0]
                print(f"📈 大盘基准 (上证指数): {market_change_pct:.2f}%")
                return float(market_change_pct)
        except Exception as e:
            print(f"⚠️ 无法获取大盘表现: {e}")
        return 0.0

    def _get_historical_data(self, symbol, thread_id=""):
        """获取单个股票的历史数据用于计算ADV和ATR"""
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=40)).strftime('%Y%m%d')
            pure_code = symbol[2:]
            hist_df = ak.stock_zh_a_hist(symbol=pure_code, start_date=start_date, end_date=end_date, adjust="qfq")
            if hist_df is None or len(hist_df) < 21: return None
            adv20 = hist_df['成交量'].rolling(window=20).mean().iloc[-1]
            high_low = hist_df['最高'] - hist_df['最低']
            high_prev_close = np.abs(hist_df['最高'] - hist_df['收盘'].shift())
            low_prev_close = np.abs(hist_df['最低'] - hist_df['收盘'].shift())
            tr = np.max(pd.DataFrame({'hl': high_low, 'hpc': high_prev_close, 'lpc': low_prev_close}), axis=1)
            atr20 = tr.rolling(window=20).mean().iloc[-1]
            return {'adv20': adv20, 'atr20': atr20}
        except Exception:
            return None

    def _get_fund_flow_with_history(self, symbol, thread_id=""):
        """获取单个股票的资金流数据（包括当天和历史）"""
        try:
            pure_code = symbol[2:]
            market = "sh" if symbol.startswith("SH") else "sz"
            
            flow_df = ak.stock_individual_fund_flow(stock=pure_code, market=market)
            
            if flow_df is None or flow_df.empty or len(flow_df) < 21:
                return None
            
            flow_df['日期'] = pd.to_datetime(flow_df['日期'])
            flow_df = flow_df.sort_values(by='日期').reset_index(drop=True)

            today_flow_row = flow_df.iloc[-1]
            today_main_inflow = today_flow_row['主力净流入-净额'] / 10000

            historical_flows = flow_df.iloc[-21:-1]
            if len(historical_flows) < 20: return None

            main_inflow_mean = historical_flows['主力净流入-净额'].mean() / 10000
            main_inflow_std = historical_flows['主力净流入-净额'].std() / 10000
            
            return {
                'today': today_main_inflow,
                'mean': main_inflow_mean, 
                'std': main_inflow_std if np.isfinite(main_inflow_std) and main_inflow_std > 0 else 1.0
            }
        except Exception:
            return None

    def _incremental_cache_batch_processor(self, symbols, cache_path, processor_func, entity_name):
        """通用增量更新缓存处理器"""
        today_str = datetime.now().strftime('%Y-%m-%d')
        cached_data = {}
        cache_filename = os.path.basename(cache_path)

        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_file_content = json.load(f)
                    if cache_file_content.get('date') == today_str:
                        cached_data = cache_file_content.get('data', {})
                        print(f"✅ 从缓存文件 '{cache_filename}' 加载 {entity_name}，共 {len(cached_data)} 条记录")
            except (json.JSONDecodeError, IOError):
                print(f"⚠️ {cache_filename} 缓存文件损坏，将重新获取")

        missing_symbols = [s for s in symbols if s not in cached_data]

        if not missing_symbols:
            print(f"✅ 所有 {entity_name} 数据均已在缓存中")
            return cached_data

        print(f"🔄 需为 {len(missing_symbols)}/{len(symbols)} 只股票获取 {entity_name}...")
        
        newly_fetched_data = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            f_to_s = {executor.submit(processor_func, s, f"T{i%self.max_workers+1} "): (s, i) for i, s in enumerate(missing_symbols)}
            for f in as_completed(f_to_s):
                s, i = f_to_s[f]
                try:
                    res = f.result(timeout=20)
                    if res:
                        newly_fetched_data[s] = res
                except TimeoutError:
                    print(f"  T{i%self.max_workers+1} {s}: ❌ 获取 {entity_name} 超时")
                except Exception:
                    pass

        if newly_fetched_data:
            print(f"🔄 获取到 {len(newly_fetched_data)} 条新的 {entity_name} 数据")
            cached_data.update(newly_fetched_data)
            try:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump({'date': today_str, 'data': cached_data}, f, ensure_ascii=False, indent=4)
                print(f"💾 {entity_name} 缓存已更新至 '{cache_filename}'，总计 {len(cached_data)} 条记录")
            except IOError as e:
                print(f"❌ 缓存 {entity_name} 失败: {e}")
        
        return cached_data

    def get_hot_stocks(self):
        """获取当日最热的沪深主板非ST A股股票，带每日缓存"""
        today_str = datetime.now().strftime('%Y-%m-%d')
        cache_path = self.hot_stocks_cache_file
        cache_filename = os.path.basename(cache_path)

        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    if cache_data.get('date') == today_str:
                        stocks = cache_data.get('stocks', [])
                        if stocks:  # 检查缓存是否为空
                            print(f"✅ 从缓存文件 '{cache_filename}' 加载热门股票列表，共 {len(stocks)} 条记录")
                            return stocks
                        else:
                            print(f"⚠️ 缓存的热门股列表为空，将重新从API获取")
            except (json.JSONDecodeError, IOError):
                print(f"⚠️ {cache_filename} 缓存文件损坏，将重新获取")

        print("🔄 从API获取热门股票排行榜...")
        hot_stock_codes = set()

        # Source 1: East Money
        try:
            hot_rank_df = ak.stock_hot_rank_em()
            if hot_rank_df is not None and not hot_rank_df.empty:
                hot_stock_codes.update(hot_rank_df['代码'].tolist())
                print(f"✅ 从东方财富获取 {len(hot_stock_codes)} 只热门股")
        except Exception as e:
            print(f"⚠️ 获取东方财富热门股失败: {e}")

        # Source 2: Baidu Search
        try:
            baidu_date = datetime.now().strftime('%Y%m%d')
            baidu_hot_df = ak.stock_hot_search_baidu(symbol="A股", date=baidu_date, time="今日")
            if baidu_hot_df is not None and not baidu_hot_df.empty:
                # The column is '股票代码'
                baidu_codes = baidu_hot_df['股票代码'].tolist()
                initial_count = len(hot_stock_codes)
                hot_stock_codes.update(baidu_codes)
                print(f"✅ 从百度热搜新增 {len(hot_stock_codes) - initial_count} 只热门股")
        except Exception as e:
            print(f"⚠️ 获取百度热搜股票失败: {e}")

        if not hot_stock_codes:
            print("❌ 未从任何来源获取到热门股")
            return []

        print(f"ℹ️ 合并后共 {len(hot_stock_codes)} 只热门股，开始进行筛选...")

        try:
            spot_df = ak.stock_zh_a_spot_em()
            spot_df['代码'] = spot_df['代码'].apply(lambda x: f"SH{x}" if x.startswith('6') else f"SZ{x}")

            # Filter the spot dataframe to only include our hot stocks
            filtered_df = spot_df[spot_df['代码'].isin(hot_stock_codes)].copy()

            is_main = filtered_df['代码'].str.startswith(('SZ00', 'SH60'))
            is_not_st = ~filtered_df['名称'].str.contains('ST')
            is_price_ok = (filtered_df['最新价'] >= 5) & (filtered_df['最新价'] <= 30)

            final_df = filtered_df[is_main & is_not_st & is_price_ok]

            # The column name for stock name is '名称' in spot_df
            final_df = final_df.rename(columns={'名称': '股票名称'})
            final_stocks = final_df[['代码', '股票名称']].to_dict('records')

            if final_stocks:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump({'date': today_str, 'stocks': final_stocks}, f, ensure_ascii=False, indent=4)
                print(f"💾 热门股票列表已缓存至 '{cache_filename}'，筛选后剩 {len(final_stocks)} 条")
            else:
                print("⚠️ 未获取到符合条件的热门股，不更新缓存")

            return final_stocks
        except Exception as e:
            print(f"❌ 获取实时行情进行筛选失败: {e}")
            return []

    def get_tick_data(self, symbol, thread_id=""):
        """获取并处理股票的tick数据，增加备用数据源"""
        tick_df, source = None, "未知"
        try: # 1. Primary: Tencent
            tick_df = ak.stock_zh_a_tick_tx_js(symbol=symbol.lower())
            if tick_df is None or tick_df.empty: raise ValueError("Tencent data is empty")
            source = "腾讯"
            tick_df = tick_df.rename(columns={'成交时间': '时间', '成交价格': '成交价', '性质': '买卖盘性质', '价格变动': '价格变动'})
        except Exception:
            try: # 2. Fallback: East Money
                tick_df = ak.stock_intraday_em(symbol=symbol[2:])
                if tick_df is None or tick_df.empty: raise ValueError("East Money data is empty")
                source = "东方财富"
                tick_df = tick_df.rename(columns={'性质': '买卖盘性质'})
                tick_df['价格变动'] = tick_df['成交价'].diff().fillna(0)
            except Exception: return None, source
        
        if not all(c in tick_df.columns for c in ['时间', '成交价', '成交量', '买卖盘性质', '价格变动']): return None, source
        tick_df = tick_df[['时间', '成交价', '成交量', '买卖盘性质', '价格变动']].copy()
        tick_df['时间'] = pd.to_datetime(tick_df['时间'])
        tick_df = tick_df.sort_values('时间').reset_index(drop=True)
        tick_df = tick_df[tick_df['买卖盘性质'].isin(['买盘', '卖盘'])].copy()
        tick_df['成交量'] = tick_df['成交量'].astype(int)
        tick_df = tick_df[tick_df['成交量'] > 0].copy()
        if tick_df.empty: return None, source
        tick_df.loc[:, 'price_impact'] = tick_df['价格变动'] / tick_df['成交量']
        tick_df['price_impact'].fillna(0, inplace=True)
        return tick_df, source

    def get_tick_data_batch(self, symbols):
        print(f"🚀 开始多线程获取 {len(symbols)} 只股票的tick数据...")
        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            f_to_s = {executor.submit(self.get_tick_data, s, f"T{i%self.max_workers+1} "): (s, i) for i, s in enumerate(symbols)}
            for f in as_completed(f_to_s):
                s, i = f_to_s[f]
                log_prefix = f"  T{i%self.max_workers+1} {s}:"
                try:
                    df, src = f.result(timeout=15)
                    if df is not None and not df.empty:
                        results[s] = df
                        print(f"{log_prefix} ✅ 获取Tick成功 (来源: {src})")
                    else:
                        print(f"{log_prefix} ❌ 获取Tick失败")
                except TimeoutError:
                    print(f"{log_prefix} ❌ 获取Tick超时")
                except Exception as e:
                    print(f"{log_prefix} ❌ 获取Tick异常: {e}")
        print(f"✅ Tick数据获取完成，成功 {len(results)}/{len(symbols)} 只")
        return results

    def _filter_wash_trades(self, tick_df, symbol, name):
        """
        识别并过滤3秒快照数据中的疑似对倒交易。
        该方法检查两种模式：
        1. 单笔Tick内的量价背离 (Intra-Tick Divergence): 巨大的成交量但价格无变化。
        2. 连续Tick间的脉冲抵消 (Inter-Tick Cancellation): 连续两笔巨量Tick，性质相反，且价格变动相互抵消。
        """
        if tick_df is None or len(tick_df) < 20: # 需要足够数据来计算基准
            return tick_df, 0

        df = tick_df.copy()
        total_volume = df['成交量'].sum()
        if total_volume == 0:
            return df, 0

        # --- 定义基准 ---
        rolling_window = 20
        volume_mean = df['成交量'].rolling(window=rolling_window, min_periods=5).mean().fillna(df['成交量'].mean())
        volume_std = df['成交量'].rolling(window=rolling_window, min_periods=5).std().fillna(df['成交量'].std())
        volume_spike_threshold = volume_mean + 2 * volume_std
        
        is_wash_trade = pd.Series(False, index=df.index)

        # --- 特征一：单笔Tick内的量价背离 ---
        is_spike = df['成交量'] > volume_spike_threshold * 2 # 对单笔要求更高
        is_no_price_change = df['价格变动'] == 0
        feature1_mask = is_spike & is_no_price_change
        is_wash_trade[feature1_mask] = True

        # --- 特征二：连续Tick间的脉冲抵消 ---
        for i in range(1, len(df)):
            # 如果当前或前一个已被标记，则跳过，避免重复判断
            if is_wash_trade.iloc[i] or is_wash_trade.iloc[i-1]:
                continue

            current_tick = df.iloc[i]
            previous_tick = df.iloc[i-1]

            # 条件1: 必须是连续的Tick (时间差约3秒)
            if (current_tick['时间'] - previous_tick['时间']) > pd.Timedelta(seconds=5):
                continue

            # 条件2: 两笔都是成交量脉冲
            is_current_spike = current_tick['成交量'] > volume_spike_threshold.iloc[i]
            is_previous_spike = previous_tick['成交量'] > volume_spike_threshold.iloc[i-1]
            if not (is_current_spike and is_previous_spike):
                continue

            # 条件3: 成交量相近 (例如，在15%的容忍度内)
            volume_diff_ratio = abs(current_tick['成交量'] - previous_tick['成交量']) / max(current_tick['成交量'], previous_tick['成交量'])
            if volume_diff_ratio > 0.15:
                continue

            # 条件4: 买卖性质相反
            if current_tick['买卖盘性质'] == previous_tick['买卖盘性质']:
                continue
            
            # 条件5: 价格变动几乎完全抵消
            net_price_change = current_tick['价格变动'] + previous_tick['价格变动']
            if abs(net_price_change) > 0.01: # 允许微小误差
                continue
            
            # 如果所有条件都满足，则标记为对倒
            is_wash_trade.iloc[i] = True
            is_wash_trade.iloc[i-1] = True

        wash_trade_volume = df.loc[is_wash_trade, '成交量'].sum()
        clean_df = df.loc[~is_wash_trade]
        
        wash_trade_ratio = wash_trade_volume / total_volume
        if wash_trade_ratio > 0.01:
            print(f"    - {symbol} ({name}): 识别到对倒嫌疑，成交量占比: {wash_trade_ratio:.2%}")
        else:
            print(f"    - {symbol} ({name}): 未识别到明显对倒嫌疑")

        return clean_df, wash_trade_ratio

    def analyze_trade_direction(self, tick_df):
        if tick_df is None or tick_df.empty: return {}
        buy_volume = tick_df.loc[tick_df['买卖盘性质'] == '买盘', '成交量'].sum()
        sell_volume = tick_df.loc[tick_df['买卖盘性质'] == '卖盘', '成交量'].sum()
        total_volume = buy_volume + sell_volume
        return {
            'net_buy_volume': buy_volume - sell_volume,
            'active_buy_ratio': buy_volume / total_volume if total_volume > 0 else 0.5,
        }

    def _calculate_score_v7_4(self, fund_flow_z_score, net_buy_adv_ratio, impact_atr_ratio, excess_return, afternoon_momentum_ratio, wash_trade_ratio):
        """(主模型) 计算股票上涨概率得分 (V7.4 - 优化对倒惩罚)"""
        fund_flow_score = np.clip(fund_flow_z_score * 25, -50, 50)
        net_buy_score = np.clip(net_buy_adv_ratio / 0.1 * 20, -20, 20)
        impact_score = 15 - (impact_atr_ratio / 0.1) * 30
        impact_score = np.clip(impact_score, -15, 15)
        
        momentum_score = 0
        if afternoon_momentum_ratio > 0.6:
            momentum_score = 10 * min((afternoon_momentum_ratio - 0.6) / 0.4, 1.0)
        elif afternoon_momentum_ratio < 0:
            momentum_score = -10
            
        alpha_score = np.clip(excess_return / 2 * 5, -5, 5)
        
        # 对倒交易惩罚项: 对倒比例越高，惩罚越重
        wash_trade_penalty = np.clip(wash_trade_ratio * 50, 0, 15)
        
        total_score = fund_flow_score + net_buy_score + impact_score + momentum_score + alpha_score - wash_trade_penalty
        return np.clip(total_score, -100, 100)

    def _calculate_score_v4_fallback(self, active_buy_ratio, net_buy_adv_ratio, impact_atr_ratio, excess_return, afternoon_momentum_ratio, wash_trade_ratio):
        """(备用模型) 计算股票上涨概率得分 (V4 - 增加对倒惩罚)"""
        buy_sell_score = (active_buy_ratio - 0.5) * 2 * 60
        net_buy_score = np.clip(net_buy_adv_ratio / 0.1 * 20, -20, 20)
        impact_score = 20 - (impact_atr_ratio / 0.1) * 40
        impact_score = np.clip(impact_score, -20, 20)
        
        momentum_score = 0
        if afternoon_momentum_ratio > 0.6:
            momentum_score = 10 * min((afternoon_momentum_ratio - 0.6) / 0.4, 1.0)
        elif afternoon_momentum_ratio < 0:
            momentum_score = -10
            
        alpha_score = np.clip(excess_return / 2 * 10, -10, 10)
        
        # 对倒交易惩罚项
        wash_trade_penalty = np.clip(wash_trade_ratio * 50, 0, 15)
        
        total_score = buy_sell_score + net_buy_score + impact_score + momentum_score + alpha_score - wash_trade_penalty
        return np.clip(total_score, -100, 100)

    def analyze_stock_worker(self, stock, tick_df, market_performance, hist_metrics, fund_flow_data, volume_ratio, current_price, change_pct):
        symbol = stock['代码']
        name = stock['股票名称']
        
        # 步骤1: 清洗Tick数据，识别对倒交易
        clean_tick_df, wash_trade_ratio = self._filter_wash_trades(tick_df, symbol, name)
        
        if clean_tick_df.empty:
            return None # 如果所有数据都是对倒，则跳过分析

        first_price = float(clean_tick_df['成交价'].iloc[0])
        last_price = float(clean_tick_df['成交价'].iloc[-1])
        intraday_change = ((last_price - first_price) / first_price) * 100 if first_price > 0 else 0
        excess_return = intraday_change - market_performance
        
        trade_direction = self.analyze_trade_direction(clean_tick_df)
        net_buy_volume = trade_direction.get('net_buy_volume', 0)
        
        afternoon_ticks = clean_tick_df[clean_tick_df['时间'].dt.time >= pd.to_datetime('13:00:00').time()]
        afternoon_net_buy_volume = self.analyze_trade_direction(afternoon_ticks).get('net_buy_volume', 0)
        afternoon_momentum_ratio = afternoon_net_buy_volume / net_buy_volume if net_buy_volume > 0 else 0

        avg_abs_impact = clean_tick_df['price_impact'].abs().mean()
        
        adv20 = hist_metrics.get('adv20', 0)
        atr20 = hist_metrics.get('atr20', 0)
        
        net_buy_adv_ratio = (net_buy_volume / adv20) if adv20 > 0 else 0
        impact_atr_ratio = (avg_abs_impact / atr20) if atr20 > 0 else 0
        
        model_version = "V4"
        fund_flow_z_score = 0
        
        if fund_flow_data:
            model_version = "V7.4"
            mean = fund_flow_data.get('mean', 0)
            std = fund_flow_data.get('std', 1)
            today_flow = fund_flow_data.get('today', 0)
            fund_flow_z_score = (today_flow - mean) / std
            score = self._calculate_score_v7_4(fund_flow_z_score, net_buy_adv_ratio, impact_atr_ratio, excess_return, afternoon_momentum_ratio, wash_trade_ratio)
        else:
            active_buy_ratio = trade_direction.get('active_buy_ratio', 0.5)
            score = self._calculate_score_v4_fallback(active_buy_ratio, net_buy_adv_ratio, impact_atr_ratio, excess_return, afternoon_momentum_ratio, wash_trade_ratio)

        return (symbol, {
            'name': name, 'score': score, 'model_version': model_version,
            'current_price': current_price,
            'change_pct': change_pct,
            'fund_flow_z_score': fund_flow_z_score,
            'net_buy_adv_ratio': net_buy_adv_ratio, 'impact_atr_ratio': impact_atr_ratio,
            'intraday_change': intraday_change, 'excess_return': excess_return,
            'active_buy_ratio': trade_direction.get('active_buy_ratio', 0.5),
            'volume_ratio': volume_ratio,
            'wash_trade_ratio': wash_trade_ratio
        })

    def _get_realtime_quotes_worker(self):
        """获取全市场实时行情的工作函数"""
        try:
            spot_df = ak.stock_zh_a_spot_em()
            spot_df['代码'] = spot_df['代码'].apply(lambda x: f"SH{x}" if x.startswith('6') else f"SZ{x}")
            volume_ratios = spot_df.set_index('代码')['量比'].to_dict()
            current_prices = spot_df.set_index('代码')['最新价'].to_dict()
            change_pcts = spot_df.set_index('代码')['涨跌幅'].to_dict()
            return volume_ratios, current_prices, change_pcts
        except Exception as e:
            print(f"\n❌ 获取实时行情失败: {e}")
            return {}, {}, {}

    def analyze_stocks(self):
        """分析所有热门股票 (V7.4流程)"""
        market_performance = self._get_market_performance()
        all_stocks = self.get_hot_stocks()
        if not all_stocks: return []
        
        symbols = [stock['代码'] for stock in all_stocks]
        
        print("\n📊 步骤 1/3: 批量获取历史和资金流数据...")
        historical_metrics = self._incremental_cache_batch_processor(symbols, self.historical_metrics_cache_file, self._get_historical_data, "历史行情")
        fund_flow_data = self._incremental_cache_batch_processor(symbols, self.fund_flow_cache_file, self._get_fund_flow_with_history, "资金流")
        
        print("\n📊 步骤 2/3: 并行获取Tick数据和实时行情...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            tick_future = executor.submit(self.get_tick_data_batch, symbols)
            realtime_future = executor.submit(self._get_realtime_quotes_worker)
            
            tick_data_results = tick_future.result()
            volume_ratios, current_prices, change_pcts = realtime_future.result()

        if volume_ratios:
            print(f"✅ 成功获取 {len(volume_ratios)} 只股票的实时行情")
        else:
            print("❌ 获取实时行情失败，将跳过量比筛选和价格显示")

        valid_stocks = []
        stock_dict = {s['代码']: s for s in all_stocks}
        for symbol, tick_df in tick_data_results.items():
            if symbol in historical_metrics:
                valid_stocks.append((
                    stock_dict[symbol], 
                    tick_df, 
                    historical_metrics[symbol], 
                    fund_flow_data.get(symbol), 
                    volume_ratios.get(symbol, 0),
                    current_prices.get(symbol, 0),
                    change_pcts.get(symbol, 0)
                ))
            else:
                print(f"  ⚠️ {symbol} ({stock_dict.get(symbol, {}).get('股票名称', '')}) 缺少必要的历史行情数据，跳过")
        
        if not valid_stocks: return []
        
        print("\n📊 步骤 3/3: 批量分析并计算得分...")
        analysis_results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.analyze_stock_worker, s, df, market_performance, hm, ffd, vr, cp, chg) for s, df, hm, ffd, vr, cp, chg in valid_stocks]
            for f in as_completed(futures):
                try:
                    res = f.result()
                    if res:
                        symbol, result = res
                        analysis_results[symbol] = result
                except Exception as e:
                    print(f"  ⚠️ 分析任务异常: {e}")
        
        sorted_stocks = sorted(analysis_results.items(), key=lambda x: x[1]['score'], reverse=True)
        
        print("\n🔬 最终结果列表 (仅排序，无筛选)...")
        final_stocks = list(sorted_stocks)
        
        print(f"\n✅ 分析完成，最终生成 {len(final_stocks)} 只股票的排序列表")
        return final_stocks

    def send_dingtalk_message(self, top_stocks):
        """发送钉钉消息 (V7.4格式)"""
        webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=ae055118615b242c6fe43fc3273a228f316209f707d07e7ce39fc83f4270ed82"
        secret = "SECf2b2861525388e240846ad1e2beb3b93d3b5f0d2e6634e43176b593f050e77da"
        
        stocks_to_send = top_stocks[:50]
        if not stocks_to_send: return False
        
        text = f"# 📈 量化分析报告 V7.4 - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        text += f"## 🏆 股票评分排序 (Top {len(stocks_to_send)})\n\n"
        
        for i, (symbol, data) in enumerate(stocks_to_send, 1):
            model_tag = f"({data['model_version']})"
            
            change_pct = data.get('change_pct', 0)
            price_str = f"¥{data.get('current_price', 0):.2f}"
            change_str = f"{'📈' if change_pct > 0 else '📉'} {change_pct:.2f}%"
            title_line = f"### {i}. {data['name']} ({symbol})\n- **{price_str}** ({change_str})\n"

            score_line = f"- **得分**: **{data['score']:.2f}** {model_tag}\n"
            
            if data['model_version'] == 'V7.4':
                z_score_line = f"- **资金流强度 (Z-score)**: **{data['fund_flow_z_score']:.2f}**\n"
            else:
                z_score_line = f"- **主动买入强度**: {data['active_buy_ratio']:.1%}\n"

            text += f"""{title_line}{score_line}- **量比**: {data.get('volume_ratio', 'N/A'):.2f}
- **对倒嫌疑**: {data.get('wash_trade_ratio', 0):.2%}
- **日内涨跌**: {data['intraday_change']:.2f}% (超额: {data['excess_return']:.2f}%)
- **净买入占比 (vs ADV20)**: {data['net_buy_adv_ratio']:.2%}
- **价格冲击 (vs ATR20)**: {data['impact_atr_ratio']:.2%}
"""
        
        message = {"msgtype": "markdown", "markdown": {"title": "量化分析报告 V7.4", "text": text}}
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
        print("🔍 量化分析系统 V7.4 - 开始分析热门股票")
        top_stocks = self.analyze_stocks()
        
        if not top_stocks:
            print("🤷 没有符合条件的股票可发送")
            return
        
        self.send_dingtalk_message(top_stocks)

    def test_single_stock(self, symbol):
        """诊断单只股票的数据获取流程"""
        print(f"\n🔬 开始诊断单只股票: {symbol}\n")
        
        print("  - 步骤1: 获取历史行情 (ADV/ATR)...")
        hist_data = self._get_historical_data(symbol)
        if hist_data:
            print(f"    ✅ 成功: {hist_data}")
        else:
            print("    ❌ 失败")

        print("\n  - 步骤2: 获取资金流 (今日+历史)...")
        fund_flow = self._get_fund_flow_with_history(symbol, thread_id="[诊断] ")
        if fund_flow:
            print(f"  [诊断] {symbol}: ✅ 资金流数据处理成功")
        else:
            print(f"  [诊断] {symbol}: ❌ 资金流数据处理失败")


        print("\n  - 步骤3: 获取今日Tick数据...")
        tick_data, source = self.get_tick_data(symbol)
        if tick_data is not None and not tick_data.empty:
            print(f"    ✅ 成功 (来源: {source}), 获取到 {len(tick_data)} 条记录")
            
            print("\n  - 步骤4: 过滤对倒交易...")
            # For testing, we need a dummy name. In real run, it's passed from stock object.
            clean_df, wash_ratio = self._filter_wash_trades(tick_data, symbol, "测试股票")
            print(f"    - 原始Tick数: {len(tick_data)}, 清洗后Tick数: {len(clean_df)}")
            print(f"    - 对倒嫌疑成交量占比: {wash_ratio:.2%}")

        else:
            print(f"    ❌ 失败 (尝试了 {source})")
        
        print("\n🔬 诊断结束")

def main():
    analyzer = QuantAnalysis()
    analyzer.run_analysis()
    
    # --- 单股诊断工具 ---
    # 1. 注释掉上面的 analyzer.run_analysis()
    # 2. 取消下面的注释
    # 3. 填入你想测试的股票代码
    # analyzer.test_single_stock("SZ002413")

if __name__ == "__main__":
    main()
