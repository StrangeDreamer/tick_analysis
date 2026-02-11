#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import time
import traceback

from collections import defaultdict

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
    def __init__(self, force_refresh=False):
        self.max_workers = min(os.cpu_count() + 4, 16)  # 优化线程数
        self.hot_stocks_cache_file = "hot_stocks_cache.json"
        self.tick_cache_dir = "tick_cache"
        self.chart_dir = "charts"
        self.force_refresh = force_refresh  # 是否强制刷新缓存

        # 确保缓存目录存在
        for directory in [self.tick_cache_dir, self.chart_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)

        # 初始化会话对象以重用连接
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

        # 初始化性能计数器
        self.perf_counters = defaultdict(float)
        self.start_time = time.time()

        # 初始化市场状态
        self.market_status = self._get_market_status()

        force_msg = "（强制刷新模式）" if force_refresh else ""
        print(f"🚀 量化分析系统 V8.4-Intraday 初始化完成{force_msg}，当前市场状态: {self.market_status}")

    def _log_performance(self, task_name, start_time):
        """记录任务执行时间"""
        elapsed = time.time() - start_time
        self.perf_counters[task_name] += elapsed
        return elapsed

    def _get_market_status(self):
        """获取当前市场状态"""
        now = datetime.now()
        weekday = now.weekday()

        # 周末
        if weekday >= 5:
            return "已休市(周末)"

        # 工作日判断交易时间
        current_time = now.time()
        morning_start = datetime.strptime("09:30:00", "%H:%M:%S").time()
        morning_end = datetime.strptime("11:30:00", "%H:%M:%S").time()
        afternoon_start = datetime.strptime("13:00:00", "%H:%M:%S").time()
        afternoon_end = datetime.strptime("15:00:00", "%H:%M:%S").time()

        if (morning_start <= current_time <= morning_end) or (afternoon_start <= current_time <= afternoon_end):
            return "交易中"
        elif current_time > afternoon_end:
            return "已收盘"
        elif current_time < morning_start:
            return "未开盘"
        else:
            return "午间休市"



    def _incremental_cache_batch_processor(self, symbols, cache_path, processor_func, entity_name):
        """增量处理数据并缓存结果"""
        task_start = time.time()
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
            self._log_performance(f"cache_process_{entity_name}", task_start)
            return cached_data

        print(f"🔄 需为 {len(missing_symbols)}/{len(symbols)} 只股票获取 {entity_name}...")

        newly_fetched_data = {}
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            f_to_s = {executor.submit(processor_func, s, f"T{i % self.max_workers + 1} "): (s, i) for i, s in
                      enumerate(missing_symbols)}
            for f in as_completed(f_to_s):
                s, i = f_to_s[f]
                try:
                    res = f.result(timeout=20)
                    if res:
                        newly_fetched_data[s] = res
                    else:
                        failed_count += 1
                except TimeoutError:
                    failed_count += 1
                except Exception as e:
                    failed_count += 1

        # 显示获取结果
        success_count = len(newly_fetched_data)
        if success_count > 0:
            print(f"✅ 成功获取 {success_count}/{len(missing_symbols)} 条新的 {entity_name} 数据", end="")
            if failed_count > 0:
                print(f"（{failed_count}只失败，已跳过）")
            else:
                print()
        elif failed_count > 0:
            print(f"⚠️ 全部 {failed_count} 只股票获取失败（收盘后API不稳定，已跳过）")
        
        if newly_fetched_data:
            cached_data.update(newly_fetched_data)
            try:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump({'date': today_str, 'data': cached_data}, f, ensure_ascii=False, indent=4)
                print(f"💾 {entity_name} 缓存已更新，总计 {len(cached_data)} 条记录")
            except IOError as e:
                print(f"❌ 缓存 {entity_name} 失败: {e}")

        self._log_performance(f"cache_process_{entity_name}", task_start)
        return cached_data

    def get_hot_stocks(self):
        """获取热门股票列表"""
        task_start = time.time()
        today_str = datetime.now().strftime('%Y-%m-%d')
        cache_path = self.hot_stocks_cache_file
        cache_filename = os.path.basename(cache_path)

        # 如果是强制刷新模式，跳过缓存检查
        if self.force_refresh:
            print("🔄 强制刷新模式：跳过缓存，直接从API获取热门股票...")
        elif os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    if cache_data.get('date') == today_str:
                        stocks = cache_data.get('stocks', [])
                        if stocks:
                            print(f"✅ 从缓存文件 '{cache_filename}' 加载热门股票列表，共 {len(stocks)} 条记录")
                            
                            # 打印缓存的股票列表
                            print("\n" + "="*70)
                            print("📋 已入选的热门股票列表（来自缓存）")
                            print("="*70)
                            for idx, stock in enumerate(stocks, 1):
                                code = stock['代码']
                                name = stock['股票名称']
                                print(f"  {idx:>3}. ✅ {code} {name}")
                            print("="*70 + "\n")
                            
                            self._log_performance("get_hot_stocks", task_start)
                            return stocks
                        else:
                            print(f"⚠️ 缓存的热门股列表为空，将重新从API获取")
            except (json.JSONDecodeError, IOError):
                print(f"⚠️ {cache_filename} 缓存文件损坏，将重新获取")

        if not self.force_refresh:
            print("🔄 获取热门股票排行榜...")
        else:
            print("🔄 正在从API获取最新热门股票排行榜...")
        
        # 获取东方财富热门股
        try:
            hot_rank_df = ak.stock_hot_rank_em()
            if hot_rank_df is None or hot_rank_df.empty:
                print("❌ 未获取到热门股票")
                self._log_performance("get_hot_stocks", task_start)
                return []
            
            print(f"✅ 获取到 {len(hot_rank_df)} 只热门股")
        except Exception as e:
            print(f"❌ 获取热门股票失败: {e}")
            self._log_performance("get_hot_stocks", task_start)
            return []

        # 筛选主板非ST股票
        try:
            all_qualified_stocks = []  # 所有符合条件的股票
            filtered_out = []
            
            print("\n" + "="*70)
            print("📋 热门股票筛选详情（全部100只）")
            print("="*70)
            
            # 处理所有100只股票
            for idx, row in hot_rank_df.iterrows():
                code = str(row['代码'])
                name = str(row.get('股票名称', row.get('名称', '')))
                rank = idx + 1
                
                # 获取股价和涨跌幅
                try:
                    price = float(row.get('最新价', 0))
                except (ValueError, TypeError):
                    price = 0
                
                try:
                    change_pct = float(row.get('涨跌幅', 0))
                except (ValueError, TypeError):
                    change_pct = 0
                
                # 判断筛选条件
                is_sh_main = code.startswith('SH60')
                is_sz_main = code.startswith('SZ00')
                is_st = 'ST' in name
                is_price_ok = 5 < price < 30  # 股价在5-30元之间
                is_change_ok = -3 < change_pct < 9  # 涨跌幅在-3%到9%之间
                
                # 主板：SH60xxxx（沪市主板）或 SZ00xxxx（深市主板）
                # 非ST：名称不包含"ST"
                # 股价：5元 < 股价 < 30元
                # 涨跌幅：-3% < 涨跌幅 < 9%
                if (is_sh_main or is_sz_main) and not is_st and is_price_ok and is_change_ok:
                    all_qualified_stocks.append({'代码': code, '股票名称': name})
                    print(f"  {rank:>3}. ✅ {code} {name:<12} ¥{price:>6.2f} {change_pct:>+6.2f}% - 入选")
                else:
                    # 记录筛选原因
                    reasons = []
                    if is_st:
                        reasons.append("ST股票")
                    if not is_sh_main and not is_sz_main:
                        if code.startswith('SH68') or code.startswith('SZ30'):
                            reasons.append("创业板/科创板")
                        elif code.startswith('BJ') or code.startswith('SZ20'):
                            reasons.append("北交所/新三板")
                        else:
                            reasons.append("非主板")
                    if not is_price_ok and (is_sh_main or is_sz_main) and not is_st:
                        if price <= 5:
                            reasons.append(f"股价过低¥{price:.2f}")
                        elif price >= 30:
                            reasons.append(f"股价过高¥{price:.2f}")
                        else:
                            reasons.append("股价异常")
                    if not is_change_ok and (is_sh_main or is_sz_main) and not is_st and is_price_ok:
                        if change_pct <= -3:
                            reasons.append(f"跌幅过大{change_pct:.2f}%")
                        elif change_pct >= 9:
                            reasons.append(f"涨幅过大{change_pct:.2f}%")
                    
                    reason_str = "、".join(reasons)
                    filtered_out.append({'代码': code, '名称': name, '原因': reason_str})
                    print(f"  {rank:>3}. ❌ {code} {name:<12} ¥{price:>6.2f} {change_pct:>+6.2f}% - 筛除（{reason_str}）")
            
            # 全部入选，不限制数量
            final_stocks = all_qualified_stocks
            
            print("="*70)
            print(f"✅ 最终入选：{len(final_stocks)} 只主板非ST股票")
            if filtered_out:
                print(f"❌ 筛除：{len(filtered_out)} 只股票")
            print("="*70 + "\n")
            
            if final_stocks:
                # 保存到缓存
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump({'date': today_str, 'stocks': final_stocks}, f, ensure_ascii=False, indent=4)
                self._log_performance("get_hot_stocks", task_start)
                return final_stocks
            else:
                print(f"⚠️ 筛选后无符合条件的股票")
                self._log_performance("get_hot_stocks", task_start)
                return []
                
        except Exception as e:
            print(f"❌ 筛选股票时出错: {e}")
            self._log_performance("get_hot_stocks", task_start)
            return []






    def get_tick_data(self, symbol, thread_id=""):
        """获取股票的Tick数据，始终从API获取最新数据"""
        task_start = time.time()

        tick_df, source = None, "未知"
        try:
            # 优先从腾讯获取
            tick_df = ak.stock_zh_a_tick_tx_js(symbol=symbol.lower())
            if tick_df is None or tick_df.empty: raise ValueError("Tencent data is empty")
            source = "腾讯"
            tick_df = tick_df.rename(
                columns={'成交时间': '时间', '成交价格': '成交价', '性质': '买卖盘性质', '价格变动': '价格变动'})
        except Exception:
            try:
                # 尝试从东方财富获取
                tick_df = ak.stock_intraday_em(symbol=symbol[2:])
                if tick_df is None or tick_df.empty: raise ValueError("East Money data is empty")
                source = "东方财富"
                tick_df = tick_df.rename(columns={'性质': '买卖盘性质'})
                tick_df['价格变动'] = tick_df['成交价'].diff().fillna(0)
            except Exception:
                # 两个来源都失败
                self._log_performance("get_tick_data", task_start)
                return None, source

        # 检查并处理数据
        if not all(c in tick_df.columns for c in ['时间', '成交价', '成交量', '买卖盘性质', '价格变动']):
            self._log_performance("get_tick_data", task_start)
            return None, source

        # 数据清洗和预处理
        tick_df = tick_df[['时间', '成交价', '成交量', '买卖盘性质', '价格变动']].copy()
        tick_df['时间'] = pd.to_datetime(tick_df['时间'])
        tick_df = tick_df.sort_values('时间').reset_index(drop=True)
        tick_df = tick_df[tick_df['买卖盘性质'].isin(['买盘', '卖盘'])].copy()
        tick_df['成交量'] = tick_df['成交量'].astype(int)
        tick_df = tick_df[tick_df['成交量'] > 0].copy()

        if tick_df.empty:
            self._log_performance("get_tick_data", task_start)
            return None, source
        # 打印最新的5条tick数据
        try:
            print(f"\n📊 {symbol} 最新 5 条 tick 数据 (来源: {source}):")
            latest_ticks = tick_df.sort_values('时间', ascending=False).head(5)
            for _, row in latest_ticks.iterrows():
                time_str = row['时间'].strftime('%H:%M:%S')
                price = row['成交价']
                volume = row['成交量']
                trade_type = row['买卖盘性质']
                price_change = row['价格变动']
                print(f"  {time_str} | 价格: {price:.2f} | 变动: {price_change:.3f} | 成交量: {volume} | {trade_type}")
        except Exception as e:
            print(f"  ⚠️ 打印tick数据时出错: {e}")
        # 计算价格冲击
        tick_df.loc[:, 'price_impact'] = tick_df['价格变动'] / tick_df['成交量']
        tick_df['price_impact'].fillna(0, inplace=True)

        # 计算时间间隔
        tick_df['time_diff'] = tick_df['时间'].diff().dt.total_seconds()
        tick_df['time_diff'] = tick_df['time_diff'].fillna(0)

        # 计算成交速率
        tick_df['volume_rate'] = tick_df['成交量'] / (tick_df['time_diff'] + 0.001)

        # 计算累计成交量
        tick_df['cum_volume'] = tick_df['成交量'].cumsum()

        # 计算累计价格变动
        tick_df['cum_price_change'] = tick_df['价格变动'].cumsum()

        # 计算VWAP
        tick_df['volume_price'] = tick_df['成交价'] * tick_df['成交量']
        tick_df['cum_volume_price'] = tick_df['volume_price'].cumsum()
        tick_df['vwap'] = tick_df['cum_volume_price'] / tick_df['cum_volume']

        # 计算移动平均价格
        tick_df['ma10'] = tick_df['成交价'].rolling(window=10).mean()

        # 可以选择性地保存当前数据作为历史参考，但不用于缓存
        today_str = datetime.now().strftime('%Y-%m-%d')
        history_file = os.path.join(self.tick_cache_dir, f"{symbol}_{today_str}_history.csv")
        try:
            tick_df.to_csv(history_file, index=False)
        except Exception:
            pass  # 忽略保存历史数据的错误

        self._log_performance("get_tick_data", task_start)
        return tick_df, source

    def get_tick_data_batch(self, symbols):
        """批量获取多只股票的Tick数据"""
        task_start = time.time()
        print(f"🚀 开始多线程获取 {len(symbols)} 只股票的tick数据...")
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            f_to_s = {executor.submit(self.get_tick_data, s, f"T{i % self.max_workers + 1} "): (s, i) for i, s in
                      enumerate(symbols)}
            for f in as_completed(f_to_s):
                s, i = f_to_s[f]
                log_prefix = f"  T{i % self.max_workers + 1} {s}:"
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
        self._log_performance("get_tick_data_batch", task_start)
        return results

    def _filter_wash_trades(self, tick_df, symbol, name):
        """增强版对倒交易识别算法"""
        task_start = time.time()
        if tick_df is None or len(tick_df) < 20:
            self._log_performance("filter_wash_trades", task_start)
            return tick_df, 0

        df = tick_df.copy()
        total_volume = df['成交量'].sum()
        if total_volume == 0:
            self._log_performance("filter_wash_trades", task_start)
            return df, 0

        # 基于滚动窗口计算成交量统计
        rolling_window = min(20, len(df) // 4)
        volume_mean = df['成交量'].rolling(window=rolling_window, min_periods=5).mean().fillna(df['成交量'].mean())
        volume_std = df['成交量'].rolling(window=rolling_window, min_periods=5).std().fillna(df['成交量'].std())
        volume_spike_threshold = volume_mean + 2 * volume_std

        # 初始化对倒交易标记
        is_wash_trade = pd.Series(False, index=df.index)

        # 特征1: 成交量异常但价格无变化
        is_spike = df['成交量'] > volume_spike_threshold * 2
        is_no_price_change = df['价格变动'].abs() < 0.001
        feature1_mask = is_spike & is_no_price_change
        is_wash_trade[feature1_mask] = True

        # 特征2: 连续的买卖对倒
        for i in range(1, len(df)):
            if is_wash_trade.iloc[i] or is_wash_trade.iloc[i - 1]:
                continue

            current_tick = df.iloc[i]
            previous_tick = df.iloc[i - 1]

            # 时间间隔过大则跳过
            if (current_tick['时间'] - previous_tick['时间']) > pd.Timedelta(seconds=5):
                continue

            # 检查成交量是否都很大
            is_current_spike = current_tick['成交量'] > volume_spike_threshold.iloc[i]
            is_previous_spike = previous_tick['成交量'] > volume_spike_threshold.iloc[i - 1]
            if not (is_current_spike and is_previous_spike):
                continue

            # 检查成交量是否接近
            volume_diff_ratio = abs(current_tick['成交量'] - previous_tick['成交量']) / max(current_tick['成交量'],
                                                                                            previous_tick['成交量'])
            if volume_diff_ratio > 0.15:
                continue

            # 检查买卖盘性质是否相反
            if current_tick['买卖盘性质'] == previous_tick['买卖盘性质']:
                continue

            # 检查价格变化是否接近于零
            net_price_change = current_tick['价格变动'] + previous_tick['价格变动']
            if abs(net_price_change) > 0.01:
                continue

            # 标记为对倒交易
            is_wash_trade.iloc[i] = True
            is_wash_trade.iloc[i - 1] = True

        # 特征3: 高频交易模式识别
        if 'time_diff' in df.columns:
            # 识别高频小额交易
            is_high_freq = df['time_diff'] < 0.5
            is_small_price_change = df['价格变动'].abs() < 0.001
            is_balanced_volume = (df['成交量'] > volume_mean * 0.5) & (df['成交量'] < volume_mean * 1.5)

            # 连续3个以上满足条件的可能是对倒
            high_freq_count = (is_high_freq & is_small_price_change & is_balanced_volume).rolling(window=3).sum()
            is_wash_trade[high_freq_count >= 3] = True

        # 特征4: 大单对倒模式
        # 检测短时间内大单买卖交替且价格几乎不变的情况
        if len(df) > 10:
            for i in range(5, len(df)):
                window = df.iloc[i - 5:i + 1]
                buy_sells = window['买卖盘性质'].tolist()

                # 检查是否有交替的买卖模式
                if '买盘' in buy_sells and '卖盘' in buy_sells and len(set(buy_sells)) > 1:
                    # 检查价格变动
                    price_range = window['成交价'].max() - window['成交价'].min()
                    avg_volume = window['成交量'].mean()

                    if price_range < 0.01 and avg_volume > volume_mean.iloc[i] * 1.5:
                        is_wash_trade.iloc[i - 5:i + 1] = True

        # 计算对倒交易占比
        wash_trade_volume = df.loc[is_wash_trade, '成交量'].sum()
        clean_df = df.loc[~is_wash_trade]

        wash_trade_ratio = wash_trade_volume / total_volume
        if wash_trade_ratio > 0.01:
            print(f"    - {symbol} ({name}): 识别到对倒嫌疑，成交量占比: {wash_trade_ratio:.2%}")
        else:
            print(f"    - {symbol} ({name}): 未识别到明显对倒嫌疑")

        self._log_performance("filter_wash_trades", task_start)
        return clean_df, wash_trade_ratio

    def analyze_trade_direction(self, tick_df):
        """分析交易方向和买卖力量对比"""
        task_start = time.time()
        if tick_df is None or tick_df.empty:
            self._log_performance("analyze_trade_direction", task_start)
            return {}

        # 基本买卖盘分析
        buy_volume = tick_df.loc[tick_df['买卖盘性质'] == '买盘', '成交量'].sum()
        sell_volume = tick_df.loc[tick_df['买卖盘性质'] == '卖盘', '成交量'].sum()
        total_volume = buy_volume + sell_volume

        # 计算买卖比率
        active_buy_ratio = buy_volume / total_volume if total_volume > 0 else 0.5

        # 计算净买入量
        net_buy_volume = buy_volume - sell_volume

        # 计算买卖盘价格冲击
        buy_impact = tick_df.loc[tick_df['买卖盘性质'] == '买盘', 'price_impact'].mean()
        sell_impact = tick_df.loc[tick_df['买卖盘性质'] == '卖盘', 'price_impact'].mean()

        # 计算买卖盘平均成交量
        avg_buy_size = tick_df.loc[tick_df['买卖盘性质'] == '买盘', '成交量'].mean()
        avg_sell_size = tick_df.loc[tick_df['买卖盘性质'] == '卖盘', '成交量'].mean()

        # 计算大单比例
        large_threshold = tick_df['成交量'].quantile(0.8)
        large_buy = tick_df[(tick_df['买卖盘性质'] == '买盘') & (tick_df['成交量'] > large_threshold)]['成交量'].sum()
        large_sell = tick_df[(tick_df['买卖盘性质'] == '卖盘') & (tick_df['成交量'] > large_threshold)]['成交量'].sum()
        large_buy_ratio = large_buy / buy_volume if buy_volume > 0 else 0
        large_sell_ratio = large_sell / sell_volume if sell_volume > 0 else 0

        # 分时段分析
        morning_df = tick_df[tick_df['时间'].dt.time < pd.to_datetime('11:30:00').time()]
        afternoon_df = tick_df[tick_df['时间'].dt.time >= pd.to_datetime('13:00:00').time()]

        morning_buy = morning_df.loc[morning_df['买卖盘性质'] == '买盘', '成交量'].sum()
        morning_sell = morning_df.loc[morning_df['买卖盘性质'] == '卖盘', '成交量'].sum()
        afternoon_buy = afternoon_df.loc[afternoon_df['买卖盘性质'] == '买盘', '成交量'].sum()
        afternoon_sell = afternoon_df.loc[afternoon_df['买卖盘性质'] == '卖盘', '成交量'].sum()

        morning_net = morning_buy - morning_sell
        afternoon_net = afternoon_buy - afternoon_sell

        # 计算动量比率
        momentum_ratio = afternoon_net / net_buy_volume if net_buy_volume != 0 else 0

        # 计算收盘前15分钟的买卖情况
        closing_time = pd.to_datetime('14:45:00').time()
        closing_df = tick_df[tick_df['时间'].dt.time >= closing_time]
        closing_buy = closing_df.loc[closing_df['买卖盘性质'] == '买盘', '成交量'].sum()
        closing_sell = closing_df.loc[closing_df['买卖盘性质'] == '卖盘', '成交量'].sum()
        closing_net = closing_buy - closing_sell
        closing_ratio = closing_net / net_buy_volume if net_buy_volume != 0 else 0

        # 计算买卖盘连续性
        buy_runs = self._calculate_runs(tick_df, '买盘')
        sell_runs = self._calculate_runs(tick_df, '卖盘')

        # 计算买卖盘集中度
        buy_concentration = self._calculate_concentration(tick_df, '买盘')
        sell_concentration = self._calculate_concentration(tick_df, '卖盘')

        # 计算买卖盘强度变化
        buy_strength_change = self._calculate_strength_change(tick_df, '买盘')
        sell_strength_change = self._calculate_strength_change(tick_df, '卖盘')

        result = {
            'net_buy_volume': net_buy_volume,
            'active_buy_ratio': active_buy_ratio,
            'buy_impact': buy_impact,
            'sell_impact': sell_impact,
            'avg_buy_size': avg_buy_size,
            'avg_sell_size': avg_sell_size,
            'large_buy_ratio': large_buy_ratio,
            'large_sell_ratio': large_sell_ratio,
            'morning_net': morning_net,
            'afternoon_net': afternoon_net,
            'momentum_ratio': momentum_ratio,
            'closing_net': closing_net,
            'closing_ratio': closing_ratio,
            'buy_runs': buy_runs,
            'sell_runs': sell_runs,
            'buy_concentration': buy_concentration,
            'sell_concentration': sell_concentration,
            'buy_strength_change': buy_strength_change,
            'sell_strength_change': sell_strength_change
        }

        self._log_performance("analyze_trade_direction", task_start)
        return result

    def _calculate_runs(self, tick_df, side):
        """计算买卖盘连续性"""
        if tick_df.empty:
            return 0

        side_df = tick_df[tick_df['买卖盘性质'] == side]
        if side_df.empty:
            return 0

        # 计算连续交易的最大长度
        side_df = side_df.sort_values('时间')
        side_df['time_diff'] = side_df['时间'].diff().dt.total_seconds()

        # 定义连续交易的时间阈值（例如5秒内）
        continuous_mask = side_df['time_diff'] < 5

        # 标记每个连续序列的开始
        run_starts = ~continuous_mask
        run_ids = run_starts.cumsum()

        # 计算每个连续序列的长度
        run_lengths = side_df.groupby(run_ids).size()

        # 返回最长连续序列的长度
        return run_lengths.max() if not run_lengths.empty else 1

    def _calculate_concentration(self, tick_df, side):
        """计算买卖盘集中度"""
        if tick_df.empty:
            return 0

        side_df = tick_df[tick_df['买卖盘性质'] == side]
        if side_df.empty or len(side_df) < 5:
            return 0

        # 将交易时间分成多个时间段
        side_df['hour'] = side_df['时间'].dt.hour
        side_df['minute_group'] = (side_df['时间'].dt.minute // 15)
        side_df['time_group'] = side_df['hour'].astype(str) + '_' + side_df['minute_group'].astype(str)

        # 计算每个时间段的成交量
        volume_by_time = side_df.groupby('time_group')['成交量'].sum()

        # 计算集中度（使用基尼系数或赫芬达尔指数）
        total_volume = volume_by_time.sum()
        if total_volume == 0:
            return 0

        # 计算赫芬达尔指数
        market_shares = (volume_by_time / total_volume)
        herfindahl_index = (market_shares ** 2).sum()

        return herfindahl_index

    def _calculate_strength_change(self, tick_df, side):
        """计算买卖盘强度变化"""
        if tick_df.empty:
            return 0

        side_df = tick_df[tick_df['买卖盘性质'] == side]
        if side_df.empty or len(side_df) < 10:
            return 0

        # 将数据分为前半部分和后半部分
        midpoint = len(side_df) // 2
        first_half = side_df.iloc[:midpoint]
        second_half = side_df.iloc[midpoint:]

        # 计算前后半部分的平均成交量
        first_half_avg = first_half['成交量'].mean()
        second_half_avg = second_half['成交量'].mean()

        # 计算强度变化率
        if first_half_avg == 0:
            return 0

        strength_change = (second_half_avg - first_half_avg) / first_half_avg
        return strength_change

    def analyze_microstructure(self, tick_df):
        """分析市场微观结构指标"""
        task_start = time.time()
        if tick_df is None or tick_df.empty:
            self._log_performance("analyze_microstructure", task_start)
            return {}

        # 计算价格冲击指标
        avg_abs_impact = tick_df['price_impact'].abs().mean()
        buy_impact = tick_df.loc[tick_df['买卖盘性质'] == '买盘', 'price_impact'].mean()
        sell_impact = tick_df.loc[tick_df['买卖盘性质'] == '卖盘', 'price_impact'].mean()
        impact_asymmetry = buy_impact - sell_impact

        # 计算Kyle's Lambda (价格冲击系数)
        # 使用回归方法估计价格变动与成交量的关系
        try:
            X = tick_df['成交量'].values.reshape(-1, 1)
            y = tick_df['价格变动'].values
            model = LinearRegression()
            model.fit(X, y)
            kyle_lambda = model.coef_[0]
        except:
            kyle_lambda = avg_abs_impact

        # 计算有效价差 (Effective Spread)
        # 使用价格冲击的两倍作为有效价差的估计
        effective_spread = 2 * avg_abs_impact

        # 计算成交量加权价格波动率
        vwap_price = np.sum(tick_df['成交价'] * tick_df['成交量']) / np.sum(tick_df['成交量'])
        vwap_volatility = np.sqrt(
            np.sum(((tick_df['成交价'] - vwap_price) ** 2) * tick_df['成交量']) / np.sum(tick_df['成交量']))

        # 计算交易活跃度
        if 'time_diff' in tick_df.columns:
            avg_time_between_trades = tick_df['time_diff'].mean()
            trade_intensity = 1 / (avg_time_between_trades + 0.001)
        else:
            avg_time_between_trades = 0
            trade_intensity = 0

        # 计算大单冲击
        large_threshold = tick_df['成交量'].quantile(0.8)
        large_trades = tick_df[tick_df['成交量'] > large_threshold]
        large_impact = large_trades['price_impact'].abs().mean() if not large_trades.empty else 0

        # 计算价格趋势
        if len(tick_df) > 1:
            first_price = tick_df['成交价'].iloc[0]
            last_price = tick_df['成交价'].iloc[-1]
            price_trend = (last_price - first_price) / first_price
        else:
            price_trend = 0

        # 计算成交量趋势
        if len(tick_df) > 20:
            volume_trend = tick_df['成交量'].rolling(window=10).mean().pct_change().mean()
        else:
            volume_trend = 0

        # 计算买卖压力比
        buy_pressure = tick_df[tick_df['买卖盘性质'] == '买盘']['price_impact'].abs().mean() if not tick_df[
            tick_df['买卖盘性质'] == '买盘'].empty else 0
        sell_pressure = tick_df[tick_df['买卖盘性质'] == '卖盘']['price_impact'].abs().mean() if not tick_df[
            tick_df['买卖盘性质'] == '卖盘'].empty else 0
        pressure_ratio = buy_pressure / sell_pressure if sell_pressure > 0 else 1.0

        # 计算价格弹性
        if len(tick_df) > 20:
            # 使用滚动窗口计算价格变动与成交量的比率
            tick_df['price_volume_ratio'] = tick_df['价格变动'].abs() / tick_df['成交量']
            price_elasticity = tick_df['price_volume_ratio'].mean()
        else:
            price_elasticity = 0

        # 计算价格反转
        if len(tick_df) > 20:
            # 计算价格变动的自相关性
            price_changes = tick_df['价格变动']
            price_autocorr = price_changes.autocorr(lag=1)
            price_reversal = -price_autocorr  # 负的自相关表示反转
        else:
            price_reversal = 0

        # 计算流动性指标
        if 'vwap' in tick_df.columns:
            # 计算价格偏离VWAP的程度
            tick_df['price_vwap_diff'] = (tick_df['成交价'] - tick_df['vwap']) / tick_df['vwap']
            liquidity_index = tick_df['price_vwap_diff'].abs().mean()
        else:
            liquidity_index = 0

        # 计算Amihud非流动性指标
        if len(tick_df) > 10:
            # 将数据分成多个时间段
            tick_df['minute'] = tick_df['时间'].dt.minute
            grouped = tick_df.groupby('minute')

            # 计算每个时间段的价格变动绝对值与成交量的比率
            amihud_values = []
            for _, group in grouped:
                if len(group) > 1:
                    price_change = abs(group['成交价'].iloc[-1] - group['成交价'].iloc[0])
                    volume = group['成交量'].sum()
                    if volume > 0:
                        amihud_values.append(price_change / volume)

            amihud_illiquidity = np.mean(amihud_values) if amihud_values else 0
        else:
            amihud_illiquidity = 0

        result = {
            'avg_abs_impact': avg_abs_impact,
            'impact_asymmetry': impact_asymmetry,
            'kyle_lambda': kyle_lambda,
            'effective_spread': effective_spread,
            'vwap_volatility': vwap_volatility,
            'trade_intensity': trade_intensity,
            'large_impact': large_impact,
            'price_trend': price_trend,
            'volume_trend': volume_trend,
            'pressure_ratio': pressure_ratio,
            'price_elasticity': price_elasticity,
            'price_reversal': price_reversal,
            'liquidity_index': liquidity_index,
            'amihud_illiquidity': amihud_illiquidity
        }

        self._log_performance("analyze_microstructure", task_start)
        return result

    def _evaluate_liquidity(self, total_volume, tick_count):
        """评估流动性充足度（日内交易关键）"""
        # 流动性评分：确保有足够的交易量和笔数
        if total_volume < 100000:  # 日成交量低于10万手
            return -20  # 严重流动性不足
        elif total_volume < 300000:  # 日成交量低于30万手
            return -10  # 中度流动性不足
        elif tick_count < 500:  # 成交笔数太少
            return -5  # 轻度流动性不足
        elif total_volume > 1000000:  # 成交量超过100万手
            return +5  # 流动性优秀
        else:
            return 0  # 流动性正常
    
    def _calculate_momentum_acceleration(self, tick_df):
        """计算动量加速度（捕捉日内爆发力）"""
        if len(tick_df) < 5:
            return 0
        
        try:
            # 将tick数据分成5个时段
            segment_size = len(tick_df) // 5
            if segment_size == 0:
                return 0
            
            segment_returns = []
            for i in range(5):
                start_idx = i * segment_size
                end_idx = start_idx + segment_size if i < 4 else len(tick_df)
                segment = tick_df.iloc[start_idx:end_idx]
                
                if len(segment) > 0:
                    first_price = segment['成交价'].iloc[0]
                    last_price = segment['成交价'].iloc[-1]
                    if first_price > 0:
                        ret = (last_price - first_price) / first_price
                        segment_returns.append(ret)
            
            if len(segment_returns) >= 3:
                # 计算加速度：后半段涨幅 - 前半段涨幅
                # 正值表示加速上涨，负值表示减速或加速下跌
                acceleration = (segment_returns[-1] - segment_returns[0])
                return acceleration
            
            return 0
        except Exception:
            return 0
    
    def _calculate_sustainability(self, tick_df):
        """计算上涨持续性（避免假突破）"""
        if len(tick_df) < 10:
            return 1.0
        
        try:
            price_changes = tick_df['价格变动'].values
            
            # 统计连续上涨和连续下跌的情况
            up_streaks = []
            down_streaks = []
            current_streak = 0
            
            for change in price_changes:
                if change > 0:
                    if current_streak >= 0:
                        current_streak += 1
                    else:
                        if current_streak < 0:
                            down_streaks.append(abs(current_streak))
                        current_streak = 1
                elif change < 0:
                    if current_streak <= 0:
                        current_streak -= 1
                    else:
                        if current_streak > 0:
                            up_streaks.append(current_streak)
                        current_streak = -1
            
            # 添加最后的streak
            if current_streak > 0:
                up_streaks.append(current_streak)
            elif current_streak < 0:
                down_streaks.append(abs(current_streak))
            
            # 计算平均持续性
            avg_up = np.mean(up_streaks) if len(up_streaks) > 0 else 0
            avg_down = np.mean(down_streaks) if len(down_streaks) > 0 else 1
            
            # 持续性比率：平均上涨持续 / 平均下跌持续
            sustainability = avg_up / avg_down if avg_down > 0 else 1.0
            return sustainability
        except Exception:
            return 1.0

    def _calculate_score_v8(self, metrics):
        """计算综合评分 - V8.4日内版（优化日内交易指标）"""
        task_start = time.time()

        # 提取指标
        relative_net_buy = metrics.get('relative_net_buy', 0)  # 相对净买入（新）
        total_volume = metrics.get('total_volume', 0)  # 总成交量（新）
        tick_count = metrics.get('tick_count', 0)  # tick笔数（新）
        momentum_acceleration = metrics.get('momentum_acceleration', 0)  # 动量加速度（新）
        sustainability = metrics.get('sustainability', 1.0)  # 上涨持续性（新）
        momentum_ratio = metrics.get('momentum_ratio', 0)
        closing_ratio = metrics.get('closing_ratio', 0)
        wash_trade_ratio = metrics.get('wash_trade_ratio', 0)
        pressure_ratio = metrics.get('pressure_ratio', 1.0)
        large_buy_ratio = metrics.get('large_buy_ratio', 0)
        large_sell_ratio = metrics.get('large_sell_ratio', 0)
        impact_asymmetry = metrics.get('impact_asymmetry', 0)
        volume_trend = metrics.get('volume_trend', 0)
        price_reversal = metrics.get('price_reversal', 0)
        buy_concentration = metrics.get('buy_concentration', 0)
        active_buy_ratio = metrics.get('active_buy_ratio', 0.5)

        # 流动性评分 (-20~+5分) - 日内交易必须关注流动性
        liquidity_score = self._evaluate_liquidity(total_volume, tick_count)

        # 相对净买入评分 (0-35分) - 使用相对值，大小盘公平
        # 相对净买入20%为满分基准
        net_buy_score = np.clip(relative_net_buy * 175, -35, 35)

        # 买卖压力比评分 (0-20分) - 权重提升
        pressure_score = 0
        if pressure_ratio > 1.2:
            pressure_score = min((pressure_ratio - 1.2) * 20, 20)
        elif pressure_ratio < 0.8:
            pressure_score = max((pressure_ratio - 0.8) * 20, -20)

        # 大单比例评分 (0-20分) - 权重提升
        large_trade_score = (large_buy_ratio - large_sell_ratio) * 40
        large_trade_score = np.clip(large_trade_score, -20, 20)

        # 动量评分 (0-15分)
        momentum_score = 0
        if momentum_ratio > 0.6:
            momentum_score = 15 * min((momentum_ratio - 0.6) / 0.4, 1.0)
        elif momentum_ratio < 0:
            momentum_score = -15

        # 收盘动量评分 (0-20分) - 日内交易重点关注尾盘
        closing_score = 0
        if closing_ratio > 0.2:
            closing_score = 20 * min((closing_ratio - 0.2) / 0.3, 1.0)
        elif closing_ratio < -0.2:
            closing_score = -20 * min((abs(closing_ratio) - 0.2) / 0.3, 1.0)

        # 动量加速度评分 (0-10分) - 新增：捕捉爆发力
        # 加速上涨（越涨越快）加分，减速或加速下跌扣分
        acceleration_score = np.clip(momentum_acceleration * 200, -10, 10)

        # 上涨持续性评分 (0-10分) - 新增：避免假突破
        # 持续性 > 1 表示上涨持续时间长于下跌，加分
        sustainability_score = np.clip((sustainability - 1) * 10, -10, 10)

        # 冲击不对称性评分 (0-10分) - 权重保持
        asymmetry_score = np.clip(impact_asymmetry * 200, -10, 10)

        # 成交量趋势评分 (0-10分) - 权重提升
        vol_trend_score = np.clip(volume_trend * 100, -10, 10)

        # 价格反转评分 (0-10分) - 权重提升
        reversal_score = np.clip(price_reversal * 20, -10, 10)

        # 买盘集中度评分 (0-15分) - 权重提升
        concentration_score = np.clip((buy_concentration - 0.2) * 45, -15, 15)

        # 主动买入比率评分 (0-15分) - 权重提升
        active_buy_score = np.clip((active_buy_ratio - 0.5) * 60, -15, 15)

        # 对倒交易惩罚 (0-10分) - 降低权重，避免误杀（准确率约70%）
        wash_trade_penalty = np.clip(wash_trade_ratio * 35, 0, 10)

        # 计算总分（V8.4日内版：100%纯tick，无大盘依赖）
        total_score = (
                net_buy_score + pressure_score + large_trade_score +
                momentum_score + closing_score +
                acceleration_score +  # 新增：动量加速度
                sustainability_score +  # 新增：上涨持续性
                asymmetry_score +
                vol_trend_score + reversal_score +
                concentration_score + active_buy_score +
                liquidity_score -  # 新增：流动性评分
                wash_trade_penalty
        )

        self._log_performance("calculate_score", task_start)
        return np.clip(total_score, -100, 100)


    def analyze_stock_worker(self, stock, tick_df):
        """分析单只股票的工作函数（纯tick数据分析）"""
        task_start = time.time()
        symbol = stock['代码']
        name = stock['股票名称']

        # 过滤对倒交易
        clean_tick_df, wash_trade_ratio = self._filter_wash_trades(tick_df, symbol, name)

        if clean_tick_df is None or clean_tick_df.empty:
            self._log_performance("analyze_stock_worker", task_start)
            return None

        # 从tick数据中提取价格和涨跌幅
        first_price = float(clean_tick_df['成交价'].iloc[0])
        last_price = float(clean_tick_df['成交价'].iloc[-1])
        current_price = last_price
        intraday_change = ((last_price - first_price) / first_price) * 100 if first_price > 0 else 0
        change_pct = intraday_change

        # 分析交易方向
        trade_direction = self.analyze_trade_direction(clean_tick_df)
        net_buy_volume = trade_direction.get('net_buy_volume', 0)

        # 计算相对净买入（日内关键指标）
        total_volume = float(clean_tick_df['成交量'].sum())
        tick_count = len(clean_tick_df)
        relative_net_buy = net_buy_volume / total_volume if total_volume > 0 else 0

        # 计算日内动量特征
        momentum_acceleration = self._calculate_momentum_acceleration(clean_tick_df)
        sustainability = self._calculate_sustainability(clean_tick_df)

        # 分析市场微观结构
        microstructure = self.analyze_microstructure(clean_tick_df)

        # 准备评分指标（V8.4日内优化版）
        metrics = {
            'net_buy_volume': net_buy_volume,
            'relative_net_buy': relative_net_buy,  # 新增：相对净买入
            'total_volume': total_volume,  # 新增：总成交量
            'tick_count': tick_count,  # 新增：tick笔数
            'momentum_acceleration': momentum_acceleration,  # 新增：动量加速度
            'sustainability': sustainability,  # 新增：上涨持续性
            'momentum_ratio': trade_direction.get('momentum_ratio', 0),
            'closing_ratio': trade_direction.get('closing_ratio', 0),
            'wash_trade_ratio': wash_trade_ratio,
            'active_buy_ratio': trade_direction.get('active_buy_ratio', 0.5),
            'large_buy_ratio': trade_direction.get('large_buy_ratio', 0),
            'large_sell_ratio': trade_direction.get('large_sell_ratio', 0),
            'pressure_ratio': microstructure.get('pressure_ratio', 1.0),
            'impact_asymmetry': microstructure.get('impact_asymmetry', 0),
            'volume_trend': microstructure.get('volume_trend', 0),
            'price_reversal': microstructure.get('price_reversal', 0),
            'buy_concentration': trade_direction.get('buy_concentration', 0)
        }

        # 计算纯tick评分
        score = self._calculate_score_v8(metrics)

        # 构建结果（V8.4日内版）
        result = {
            'name': name,
            'score': score,
            'model_version': "V8.4-Intraday",
            'current_price': current_price,
            'change_pct': change_pct,
            'intraday_change': intraday_change,
            'relative_net_buy': relative_net_buy,  # 新增
            'total_volume': total_volume,  # 新增
            'momentum_acceleration': momentum_acceleration,  # 新增
            'sustainability': sustainability,  # 新增
            'active_buy_ratio': trade_direction.get('active_buy_ratio', 0.5),
            'momentum_ratio': trade_direction.get('momentum_ratio', 0),
            'closing_ratio': trade_direction.get('closing_ratio', 0),
            'wash_trade_ratio': wash_trade_ratio,
            'pressure_ratio': microstructure.get('pressure_ratio', 1.0),
            'large_buy_ratio': trade_direction.get('large_buy_ratio', 0),
            'large_sell_ratio': trade_direction.get('large_sell_ratio', 0),
            'kyle_lambda': microstructure.get('kyle_lambda', 0),
            'effective_spread': microstructure.get('effective_spread', 0),
            'price_reversal': microstructure.get('price_reversal', 0),
            'buy_concentration': trade_direction.get('buy_concentration', 0)
        }

        self._log_performance("analyze_stock_worker", task_start)
        return (symbol, result)



    def analyze_stocks(self):
        """分析所有热门股票"""
        total_start = time.time()
        all_stocks = self.get_hot_stocks()
        if not all_stocks: return []

        symbols = [stock['代码'] for stock in all_stocks]

        print("\n📊 步骤 1/1: 获取Tick数据...")
        tick_data_results = self.get_tick_data_batch(symbols)

        valid_stocks = []
        stock_dict = {s['代码']: s for s in all_stocks}
        for symbol, tick_df in tick_data_results.items():
            valid_stocks.append((
                stock_dict[symbol],
                tick_df
            ))

        if not valid_stocks: return []

        print("\n📊 步骤 2/2: 批量分析并计算得分...")
        analysis_results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.analyze_stock_worker, s, df)
                       for s, df in valid_stocks]
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

        total_time = time.time() - total_start
        print(f"\n✅ 分析完成，最终生成 {len(final_stocks)} 只股票的排序列表，总耗时: {total_time:.2f}秒")

        # 打印性能统计
        print("\n⏱️ 性能统计:")
        for task, time_spent in sorted(self.perf_counters.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  - {task}: {time_spent:.2f}秒")

        return final_stocks

    def send_dingtalk_message(self, top_stocks):
        """发送钉钉消息"""
        webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=ae055118615b242c6fe43fc3273a228f316209f707d07e7ce39fc83f4270ed82"
        secret = "SECf2b2861525388e240846ad1e2beb3b93d3b5f0d2e6634e43176b593f050e77da"

        stocks_to_send = top_stocks[:30]
        if not stocks_to_send: return False

        text = f"# 📈 量化分析报告 V8.4-Intraday - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        text += f"## 🏆 股票评分排序 (Top {len(stocks_to_send)})\n\n"

        for i, (symbol, data) in enumerate(stocks_to_send, 1):
            model_tag = f"({data['model_version']})"

            change_pct = data.get('change_pct', 0)
            price_str = f"¥{data.get('current_price', 0):.2f}"
            change_str = f"{'📈' if change_pct > 0 else '📉'} {change_pct:.2f}%"
            title_line = f"### {i}. {data['name']} ({symbol})\n- **{price_str}** ({change_str})\n"

            score_line = f"- **得分**: **{data['score']:.2f}** {model_tag}\n"

            text += f"""{title_line}{score_line}- **买卖压力比**: {data.get('pressure_ratio', 1.0):.2f}
- **主动买入比率**: {data.get('active_buy_ratio', 0.5):.2%}
- **大单买入占比**: {data.get('large_buy_ratio', 0):.2%} vs 卖出 {data.get('large_sell_ratio', 0):.2%}
- **日内涨跌**: {data['intraday_change']:.2f}%
- **动量比率**: {data['momentum_ratio']:.2f} / 收盘: {data['closing_ratio']:.2f}
- **对倒嫌疑**: {data.get('wash_trade_ratio', 0):.2%}
- **Kyle's Lambda**: {data.get('kyle_lambda', 0):.6f}
"""

        message = {"msgtype": "markdown", "markdown": {"title": "量化分析报告 V8.4-Intraday", "text": text}}
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{secret}"
        hmac_code = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
        sign = base64.b64encode(hmac_code).decode('utf-8')
        full_webhook_url = f"{webhook_url}&timestamp={timestamp}&sign={sign}"

        try:
            response = self.session.post(full_webhook_url, json=message, timeout=10)
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
        print("🔍 量化分析系统 V8.4-Intraday - 开始分析热门股票")
        try:
            top_stocks = self.analyze_stocks()

            if not top_stocks:
                print("🤷 没有符合条件的股票可发送")
                return

            self.send_dingtalk_message(top_stocks)
        except Exception as e:
            print(f"❌ 分析过程中发生错误: {e}")
            traceback.print_exc()



def main():
    analyzer = QuantAnalysis()
    analyzer.run_analysis()


if __name__ == "__main__":
    main()
