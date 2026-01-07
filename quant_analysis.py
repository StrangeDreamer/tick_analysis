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
from sklearn.linear_model import LinearRegression


class QuantAnalysis:
    def __init__(self):
        self.max_workers = min(os.cpu_count() + 4, 16)  # 优化线程数
        self.hot_stocks_cache_file = "hot_stocks_cache.json"
        self.historical_metrics_cache_file = "historical_metrics_cache.json"
        self.fund_flow_cache_file = "fund_flow_cache.json"
        self.tick_cache_dir = "tick_cache"
        self.chart_dir = "charts"

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

        print(f"🚀 量化分析系统 V8.0 初始化完成，当前市场状态: {self.market_status}")

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

    def _get_market_performance(self):
        """获取大盘表现作为基准"""
        task_start = time.time()
        try:
            market_df = ak.stock_individual_spot_xq(symbol="SH000001")
            change_row = market_df[market_df['item'] == '涨幅']
            if not change_row.empty:
                market_change_pct = change_row['value'].iloc[0]
                print(f"📈 大盘基准 (上证指数): {market_change_pct:.2f}%")

                # 获取上证50、沪深300和创业板指数表现
                try:
                    sz50_df = ak.stock_individual_spot_xq(symbol="SH000016")
                    sz50_change = sz50_df[sz50_df['item'] == '涨幅']['value'].iloc[0]

                    hs300_df = ak.stock_individual_spot_xq(symbol="SH000300")
                    hs300_change = hs300_df[hs300_df['item'] == '涨幅']['value'].iloc[0]

                    cyb_df = ak.stock_individual_spot_xq(symbol="SZ399006")
                    cyb_change = cyb_df[cyb_df['item'] == '涨幅']['value'].iloc[0]

                    print(
                        f"📊 市场表现: 上证50 {sz50_change:.2f}% | 沪深300 {hs300_change:.2f}% | 创业板 {cyb_change:.2f}%")
                except Exception:
                    pass

                self._log_performance("get_market_perf", task_start)
                return float(market_change_pct)
        except Exception as e:
            print(f"⚠️ 无法获取大盘表现: {e}")
        self._log_performance("get_market_perf", task_start)
        return 0.0

    def _get_historical_data(self, symbol, thread_id=""):
        """获取历史数据，计算ADV20、ATR20等技术指标"""
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')  # 扩大历史数据范围
            pure_code = symbol[2:]
            hist_df = ak.stock_zh_a_hist(symbol=pure_code, start_date=start_date, end_date=end_date, adjust="qfq")
            if hist_df is None or len(hist_df) < 21: return None

            # 基础量价指标
            adv20 = hist_df['成交量'].rolling(window=20).mean().iloc[-1]

            # 计算ATR20
            high_low = hist_df['最高'] - hist_df['最低']
            high_prev_close = np.abs(hist_df['最高'] - hist_df['收盘'].shift())
            low_prev_close = np.abs(hist_df['最低'] - hist_df['收盘'].shift())
            tr = np.max(pd.DataFrame({'hl': high_low, 'hpc': high_prev_close, 'lpc': low_prev_close}), axis=1)
            atr20 = tr.rolling(window=20).mean().iloc[-1]

            # 计算波动率
            returns = hist_df['收盘'].pct_change()
            volatility = returns.rolling(window=20).std().iloc[-1] * np.sqrt(252)

            # 计算趋势强度
            sma5 = hist_df['收盘'].rolling(window=5).mean()
            sma20 = hist_df['收盘'].rolling(window=20).mean()
            trend_strength = (sma5.iloc[-1] / sma20.iloc[-1] - 1) * 100

            # 计算RSI
            delta = hist_df['收盘'].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain = gain.rolling(window=14).mean()
            avg_loss = loss.rolling(window=14).mean()
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs)).iloc[-1]

            # 计算MACD
            exp12 = hist_df['收盘'].ewm(span=12, adjust=False).mean()
            exp26 = hist_df['收盘'].ewm(span=26, adjust=False).mean()
            macd = exp12 - exp26
            signal = macd.ewm(span=9, adjust=False).mean()
            macd_hist = macd - signal
            macd_value = macd.iloc[-1]
            macd_signal = signal.iloc[-1]
            macd_hist_value = macd_hist.iloc[-1]

            # 计算布林带
            middle_band = hist_df['收盘'].rolling(window=20).mean()
            std_dev = hist_df['收盘'].rolling(window=20).std()
            upper_band = middle_band + (std_dev * 2)
            lower_band = middle_band - (std_dev * 2)
            bb_width = (upper_band - lower_band) / middle_band
            bb_width_value = bb_width.iloc[-1]

            # 计算相对强度(与大盘比较)
            try:
                market_df = ak.stock_zh_a_hist(symbol="000001", start_date=start_date, end_date=end_date, adjust="qfq",
                                               period="daily")
                if market_df is not None and len(market_df) >= len(hist_df):
                    stock_returns = hist_df['收盘'].pct_change().dropna()
                    market_returns = market_df['收盘'].pct_change().dropna()
                    # 确保长度一致
                    min_len = min(len(stock_returns), len(market_returns))
                    stock_returns = stock_returns[-min_len:]
                    market_returns = market_returns[-min_len:]

                    # 计算Beta和Alpha
                    if len(stock_returns) > 5:
                        beta = np.cov(stock_returns, market_returns)[0, 1] / np.var(market_returns)
                        alpha = (stock_returns.mean() - beta * market_returns.mean()) * 252  # 年化Alpha
                    else:
                        beta = 1.0
                        alpha = 0.0
                else:
                    beta = 1.0
                    alpha = 0.0
            except Exception:
                beta = 1.0
                alpha = 0.0

            # 计算成交量变化趋势
            volume_trend = hist_df['成交量'].pct_change().rolling(window=5).mean().iloc[-1]

            # 计算价格动量
            momentum_5d = (hist_df['收盘'].iloc[-1] / hist_df['收盘'].iloc[-6] - 1) * 100 if len(hist_df) >= 6 else 0
            momentum_10d = (hist_df['收盘'].iloc[-1] / hist_df['收盘'].iloc[-11] - 1) * 100 if len(hist_df) >= 11 else 0

            # 计算换手率平均值
            turnover_mean = hist_df['换手率'].rolling(window=20).mean().iloc[-1] if '换手率' in hist_df.columns else 0

            # 计算价格与成交量相关性
            if len(hist_df) >= 20:
                price_changes = hist_df['收盘'].pct_change().iloc[-20:]
                volume_changes = hist_df['成交量'].pct_change().iloc[-20:]
                price_volume_corr = price_changes.corr(volume_changes)
            else:
                price_volume_corr = 0

            return {
                'adv20': adv20,
                'atr20': atr20,
                'volatility': volatility,
                'trend_strength': trend_strength,
                'rsi': rsi,
                'macd': macd_value,
                'macd_signal': macd_signal,
                'macd_hist': macd_hist_value,
                'bb_width': bb_width_value,
                'beta': beta,
                'alpha': alpha,
                'volume_trend': volume_trend,
                'momentum_5d': momentum_5d,
                'momentum_10d': momentum_10d,
                'turnover_mean': turnover_mean,
                'price_volume_corr': price_volume_corr
            }
        except Exception as e:
            print(f"  ⚠️ 获取历史数据异常 ({symbol}): {e}")
            return None

    def _get_fund_flow_with_history(self, symbol, thread_id=""):
        """获取资金流数据，包括历史统计"""
        try:
            pure_code = symbol[2:]
            market = "sh" if symbol.startswith("SH") else "sz"

            flow_df = ak.stock_individual_fund_flow(stock=pure_code, market=market)

            if flow_df is None or flow_df.empty or len(flow_df) < 21:
                return None

            flow_df['日期'] = pd.to_datetime(flow_df['日期'])
            flow_df = flow_df.sort_values(by='日期').reset_index(drop=True)

            # 检查必要的列是否存在
            required_columns = ['主力净流入-净额', '超大单净流入-净额', '大单净流入-净额', '中单净流入-净额',
                                '小单净流入-净额']
            for col in required_columns:
                if col not in flow_df.columns:
                    print(f"  ⚠️ 资金流数据缺少列: {col}")
                    return None

            # 计算散户净流入-净额 (小单 + 中单)
            flow_df['散户净流入-净额'] = flow_df['小单净流入-净额'] + flow_df['中单净流入-净额']

            today_flow_row = flow_df.iloc[-1]
            today_main_inflow = today_flow_row['主力净流入-净额'] / 10000
            today_retail_inflow = today_flow_row['散户净流入-净额'] / 10000
            today_super_inflow = today_flow_row['超大单净流入-净额'] / 10000
            today_big_inflow = today_flow_row['大单净流入-净额'] / 10000
            today_mid_inflow = today_flow_row['中单净流入-净额'] / 10000
            today_small_inflow = today_flow_row['小单净流入-净额'] / 10000

            historical_flows = flow_df.iloc[-21:-1]
            if len(historical_flows) < 20: return None

            # 计算主力资金流统计
            main_inflow_mean = historical_flows['主力净流入-净额'].mean() / 10000
            main_inflow_std = historical_flows['主力净流入-净额'].std() / 10000

            # 计算超大单资金流统计
            super_inflow_mean = historical_flows['超大单净流入-净额'].mean() / 10000
            super_inflow_std = historical_flows['超大单净流入-净额'].std() / 10000

            # 计算大单资金流统计
            big_inflow_mean = historical_flows['大单净流入-净额'].mean() / 10000
            big_inflow_std = historical_flows['大单净流入-净额'].std() / 10000

            # 计算中单资金流统计
            mid_inflow_mean = historical_flows['中单净流入-净额'].mean() / 10000
            mid_inflow_std = historical_flows['中单净流入-净额'].std() / 10000

            # 计算小单资金流统计
            small_inflow_mean = historical_flows['小单净流入-净额'].mean() / 10000
            small_inflow_std = historical_flows['小单净流入-净额'].std() / 10000

            # 计算散户资金流统计
            retail_inflow_mean = historical_flows['散户净流入-净额'].mean() / 10000
            retail_inflow_std = historical_flows['散户净流入-净额'].std() / 10000

            # 计算资金流趋势
            if len(historical_flows) >= 5:
                recent_flows = historical_flows['主力净流入-净额'].values[-5:] / 10000
                flow_trend = np.polyfit(range(len(recent_flows)), recent_flows, 1)[0]
            else:
                flow_trend = 0

            # 计算资金流连续性
            if len(historical_flows) >= 3:
                recent_signs = np.sign(historical_flows['主力净流入-净额'].values[-3:])
                flow_consistency = 1 if np.all(recent_signs > 0) else (-1 if np.all(recent_signs < 0) else 0)
            else:
                flow_consistency = 0

            return {
                'today_main': today_main_inflow,
                'today_retail': today_retail_inflow,
                'today_super': today_super_inflow,
                'today_big': today_big_inflow,
                'today_mid': today_mid_inflow,
                'today_small': today_small_inflow,
                'main_mean': main_inflow_mean,
                'main_std': main_inflow_std if np.isfinite(main_inflow_std) and main_inflow_std > 0 else 1.0,
                'super_mean': super_inflow_mean,
                'super_std': super_inflow_std if np.isfinite(super_inflow_std) and super_inflow_std > 0 else 1.0,
                'big_mean': big_inflow_mean,
                'big_std': big_inflow_std if np.isfinite(big_inflow_std) and big_inflow_std > 0 else 1.0,
                'mid_mean': mid_inflow_mean,
                'mid_std': mid_inflow_std if np.isfinite(mid_inflow_std) and mid_inflow_std > 0 else 1.0,
                'small_mean': small_inflow_mean,
                'small_std': small_inflow_std if np.isfinite(small_inflow_std) and small_inflow_std > 0 else 1.0,
                'retail_mean': retail_inflow_mean,
                'retail_std': retail_inflow_std if np.isfinite(retail_inflow_std) and retail_inflow_std > 0 else 1.0,
                'flow_trend': flow_trend,
                'flow_consistency': flow_consistency
            }
        except Exception as e:
            print(f"  ⚠️ 获取资金流异常 ({symbol}): {e}")
            return None

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
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            f_to_s = {executor.submit(processor_func, s, f"T{i % self.max_workers + 1} "): (s, i) for i, s in
                      enumerate(missing_symbols)}
            for f in as_completed(f_to_s):
                s, i = f_to_s[f]
                try:
                    res = f.result(timeout=20)
                    if res:
                        newly_fetched_data[s] = res
                except TimeoutError:
                    print(f"  T{i % self.max_workers + 1} {s}: ❌ 获取 {entity_name} 超时")
                except Exception as e:
                    print(f"  T{i % self.max_workers + 1} {s}: ❌ 获取 {entity_name} 异常: {str(e)[:50]}...")

        if newly_fetched_data:
            print(f"🔄 获取到 {len(newly_fetched_data)} 条新的 {entity_name} 数据")
            cached_data.update(newly_fetched_data)
            try:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump({'date': today_str, 'data': cached_data}, f, ensure_ascii=False, indent=4)
                print(f"💾 {entity_name} 缓存已更新至 '{cache_filename}'，总计 {len(cached_data)} 条记录")
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

        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    if cache_data.get('date') == today_str:
                        stocks = cache_data.get('stocks', [])
                        if stocks:
                            print(f"✅ 从缓存文件 '{cache_filename}' 加载热门股票列表，共 {len(stocks)} 条记录")
                            self._log_performance("get_hot_stocks", task_start)
                            return stocks
                        else:
                            print(f"⚠️ 缓存的热门股列表为空，将重新从API获取")
            except (json.JSONDecodeError, IOError):
                print(f"⚠️ {cache_filename} 缓存文件损坏，将重新获取")

        print("🔄 从API获取热门股票排行榜...")
        hot_stock_codes = set()

        # 获取东方财富热门股
        try:
            hot_rank_df = ak.stock_hot_rank_em()
            if hot_rank_df is not None and not hot_rank_df.empty:
                hot_stock_codes.update(hot_rank_df['代码'].tolist())
                print(f"✅ 从东方财富获取 {len(hot_stock_codes)} 只热门股")
        except Exception as e:
            print(f"⚠️ 获取东方财富热门股失败: {e}")

        # 获取百度热搜股票
        try:
            baidu_date = datetime.now().strftime('%Y%m%d')
            baidu_hot_df = ak.stock_hot_search_baidu(symbol="A股", date=baidu_date, time="今日")
            if baidu_hot_df is not None and not baidu_hot_df.empty:
                baidu_codes = baidu_hot_df['股票代码'].tolist()
                initial_count = len(hot_stock_codes)
                hot_stock_codes.update(baidu_codes)
                print(f"✅ 从百度热搜新增 {len(hot_stock_codes) - initial_count} 只热门股")
        except Exception as e:
            print(f"⚠️ 获取百度热搜股票失败: {e}")

        # 获取雪球热门股票
        try:
            xq_hot_df = ak.stock_hot_rank_detail_xq(symbol="最热门")
            if xq_hot_df is not None and not xq_hot_df.empty:
                xq_codes = xq_hot_df['股票代码'].tolist()
                initial_count = len(hot_stock_codes)
                hot_stock_codes.update(xq_codes)
                print(f"✅ 从雪球热门新增 {len(hot_stock_codes) - initial_count} 只热门股")
        except Exception as e:
            print(f"⚠️ 获取雪球热门股票失败: {e}")

        # 获取龙虎榜股票
        try:
            lhb_df = ak.stock_lhb_em()
            if lhb_df is not None and not lhb_df.empty:
                lhb_codes = lhb_df['代码'].tolist()
                initial_count = len(hot_stock_codes)
                hot_stock_codes.update(lhb_codes)
                print(f"✅ 从龙虎榜新增 {len(hot_stock_codes) - initial_count} 只热门股")
        except Exception as e:
            print(f"⚠️ 获取龙虎榜股票失败: {e}")

        if not hot_stock_codes:
            print("❌ 未从任何来源获取到热门股")
            self._log_performance("get_hot_stocks", task_start)
            return []

        print(f"ℹ️ 合并后共 {len(hot_stock_codes)} 只热门股，开始进行筛选...")

        try:
            # 获取实时行情进行筛选
            spot_df = ak.stock_zh_a_spot_em()
            spot_df['代码'] = spot_df['代码'].apply(lambda x: f"SH{x}" if x.startswith('6') else f"SZ{x}")

            filtered_df = spot_df[spot_df['代码'].isin(hot_stock_codes)].copy()

            # 筛选条件
            is_main = filtered_df['代码'].str.startswith(('SZ00', 'SH60'))  # 主板
            is_not_st = ~filtered_df['名称'].str.contains('ST')  # 非ST
            is_price_ok = (filtered_df['最新价'] >= 5) & (filtered_df['最新价'] <= 30)  # 价格区间
            is_volume_ok = filtered_df['成交量'] > 100000  # 成交量要足够
            is_turnover_ok = filtered_df['换手率'] > 1.0  # 换手率要足够

            # 应用筛选条件
            final_df = filtered_df[is_main & is_not_st & is_price_ok & is_volume_ok & is_turnover_ok]

            # 重命名并提取结果
            final_df = final_df.rename(columns={'名称': '股票名称'})
            final_stocks = final_df[['代码', '股票名称']].to_dict('records')

            if final_stocks:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump({'date': today_str, 'stocks': final_stocks}, f, ensure_ascii=False, indent=4)
                print(f"💾 热门股票列表已缓存至 '{cache_filename}'，筛选后剩 {len(final_stocks)} 条")
            else:
                print("⚠️ 未获取到符合条件的热门股，不更新缓存")

            self._log_performance("get_hot_stocks", task_start)
            return final_stocks
        except Exception as e:
            print(f"❌ 获取实时行情进行筛选失败: {e}")
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

    def _calculate_score_v8(self, metrics):
        """计算综合评分 - V8版本"""
        task_start = time.time()

        # 提取指标
        fund_flow_z_score = metrics.get('fund_flow_z_score', 0)
        super_flow_z_score = metrics.get('super_flow_z_score', 0)
        flow_consistency = metrics.get('flow_consistency', 0)
        net_buy_adv_ratio = metrics.get('net_buy_adv_ratio', 0)
        impact_atr_ratio = metrics.get('impact_atr_ratio', 0)
        excess_return = metrics.get('excess_return', 0)
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
        rsi = metrics.get('rsi', 50)

        # 资金流评分 (0-40分)
        fund_flow_score = np.clip(fund_flow_z_score * 20, -30, 30)
        super_flow_score = np.clip(super_flow_z_score * 10, -10, 10)
        flow_consistency_score = flow_consistency * 5  # -5到5分

        # 净买入评分 (0-20分)
        net_buy_score = np.clip(net_buy_adv_ratio / 0.1 * 20, -20, 20)

        # 价格冲击评分 (0-15分)
        impact_score = 15 - (impact_atr_ratio / 0.1) * 30
        impact_score = np.clip(impact_score, -15, 15)

        # 买卖压力比评分 (0-10分)
        pressure_score = 0
        if pressure_ratio > 1.2:
            pressure_score = min((pressure_ratio - 1.2) * 10, 10)
        elif pressure_ratio < 0.8:
            pressure_score = max((pressure_ratio - 0.8) * 10, -10)

        # 大单比例评分 (0-10分)
        large_trade_score = (large_buy_ratio - large_sell_ratio) * 20
        large_trade_score = np.clip(large_trade_score, -10, 10)

        # 动量评分 (0-15分)
        momentum_score = 0
        if momentum_ratio > 0.6:
            momentum_score = 10 * min((momentum_ratio - 0.6) / 0.4, 1.0)
        elif momentum_ratio < 0:
            momentum_score = -10

        # 收盘动量评分 (0-5分)
        closing_score = 0
        if closing_ratio > 0.2:
            closing_score = 5 * min((closing_ratio - 0.2) / 0.3, 1.0)
        elif closing_ratio < -0.2:
            closing_score = -5 * min((abs(closing_ratio) - 0.2) / 0.3, 1.0)

        # 超额收益评分 (0-5分)
        alpha_score = np.clip(excess_return / 2 * 5, -5, 5)

        # 冲击不对称性评分 (0-5分)
        asymmetry_score = np.clip(impact_asymmetry * 100, -5, 5)

        # 成交量趋势评分 (0-5分)
        vol_trend_score = np.clip(volume_trend * 50, -5, 5)

        # 价格反转评分 (0-5分)
        reversal_score = np.clip(price_reversal * 10, -5, 5)

        # 买盘集中度评分 (0-5分)
        concentration_score = np.clip((buy_concentration - 0.2) * 20, -5, 5)

        # RSI评分 (0-5分)
        rsi_score = 0
        if rsi > 70:
            rsi_score = -5 * min((rsi - 70) / 15, 1.0)  # 过热惩罚
        elif rsi < 30:
            rsi_score = 5 * min((30 - rsi) / 15, 1.0)  # 超跌奖励

        # 对倒交易惩罚 (0-15分)
        wash_trade_penalty = np.clip(wash_trade_ratio * 50, 0, 15)

        # 计算总分
        total_score = (
                fund_flow_score + super_flow_score + flow_consistency_score +
                net_buy_score + impact_score +
                pressure_score + large_trade_score +
                momentum_score + closing_score +
                alpha_score + asymmetry_score +
                vol_trend_score + reversal_score +
                concentration_score + rsi_score -
                wash_trade_penalty
        )

        self._log_performance("calculate_score", task_start)
        return np.clip(total_score, -100, 100)


    def analyze_stock_worker(self, stock, tick_df, market_performance, hist_metrics, fund_flow_data, volume_ratio,
                             current_price, change_pct, turnover_rate):
        """分析单只股票的工作函数"""
        task_start = time.time()
        symbol = stock['代码']
        name = stock['股票名称']

        # 过滤对倒交易
        clean_tick_df, wash_trade_ratio = self._filter_wash_trades(tick_df, symbol, name)

        if clean_tick_df is None or clean_tick_df.empty:
            self._log_performance("analyze_stock_worker", task_start)
            return None

        # 计算日内价格变化
        first_price = float(clean_tick_df['成交价'].iloc[0])
        last_price = float(clean_tick_df['成交价'].iloc[-1])
        intraday_change = ((last_price - first_price) / first_price) * 100 if first_price > 0 else 0
        excess_return = intraday_change - market_performance

        # 分析交易方向
        trade_direction = self.analyze_trade_direction(clean_tick_df)
        net_buy_volume = trade_direction.get('net_buy_volume', 0)

        # 分析市场微观结构
        microstructure = self.analyze_microstructure(clean_tick_df)

        # 获取历史指标
        adv20 = hist_metrics.get('adv20', 0)
        atr20 = hist_metrics.get('atr20', 0)
        volatility = hist_metrics.get('volatility', 0)
        trend_strength = hist_metrics.get('trend_strength', 0)
        rsi = hist_metrics.get('rsi', 50)
        macd = hist_metrics.get('macd', 0)
        macd_signal = hist_metrics.get('macd_signal', 0)
        bb_width = hist_metrics.get('bb_width', 0)
        beta = hist_metrics.get('beta', 1.0)
        alpha = hist_metrics.get('alpha', 0)
        momentum_5d = hist_metrics.get('momentum_5d', 0)
        momentum_10d = hist_metrics.get('momentum_10d', 0)
        price_volume_corr = hist_metrics.get('price_volume_corr', 0)

        # 计算关键比率
        net_buy_adv_ratio = (net_buy_volume / adv20) if adv20 > 0 else 0
        impact_atr_ratio = (microstructure.get('avg_abs_impact', 0) / atr20) if atr20 > 0 else 0

        # 准备评分指标
        metrics = {
            'net_buy_adv_ratio': net_buy_adv_ratio,
            'impact_atr_ratio': impact_atr_ratio,
            'excess_return': excess_return,
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
            'buy_concentration': trade_direction.get('buy_concentration', 0),
            'volatility': volatility,
            'trend_strength': trend_strength,
            'rsi': rsi,
            'macd': macd,
            'macd_signal': macd_signal,
            'bb_width': bb_width,
            'beta': beta,
            'alpha': alpha,
            'momentum_5d': momentum_5d,
            'momentum_10d': momentum_10d,
            'price_volume_corr': price_volume_corr,
            'turnover_rate': turnover_rate
        }

        # 添加资金流指标
        if fund_flow_data:
            # 主力资金流Z-score
            main_mean = fund_flow_data.get('main_mean', 0)
            main_std = fund_flow_data.get('main_std', 1)
            today_main = fund_flow_data.get('today_main', 0)
            fund_flow_z_score = (today_main - main_mean) / main_std

            # 超大单资金流Z-score
            super_mean = fund_flow_data.get('super_mean', 0)
            super_std = fund_flow_data.get('super_std', 1)
            today_super = fund_flow_data.get('today_super', 0)
            super_flow_z_score = (today_super - super_mean) / super_std

            # 资金流一致性
            flow_consistency = fund_flow_data.get('flow_consistency', 0)

            metrics['fund_flow_z_score'] = fund_flow_z_score
            metrics['super_flow_z_score'] = super_flow_z_score
            metrics['flow_consistency'] = flow_consistency

        # 计算V8评分
        score = self._calculate_score_v8(metrics)

        # 构建结果
        result = {
            'name': name,
            'score': score,
            'model_version': "V8",
            'current_price': current_price,
            'change_pct': change_pct,
            'turnover_rate': turnover_rate,
            'fund_flow_z_score': metrics.get('fund_flow_z_score', 0),
            'super_flow_z_score': metrics.get('super_flow_z_score', 0),
            'flow_consistency': metrics.get('flow_consistency', 0),
            'net_buy_adv_ratio': net_buy_adv_ratio,
            'impact_atr_ratio': impact_atr_ratio,
            'intraday_change': intraday_change,
            'excess_return': excess_return,
            'active_buy_ratio': trade_direction.get('active_buy_ratio', 0.5),
            'momentum_ratio': trade_direction.get('momentum_ratio', 0),
            'closing_ratio': trade_direction.get('closing_ratio', 0),
            'volume_ratio': volume_ratio,
            'wash_trade_ratio': wash_trade_ratio,
            'pressure_ratio': microstructure.get('pressure_ratio', 1.0),
            'large_buy_ratio': trade_direction.get('large_buy_ratio', 0),
            'large_sell_ratio': trade_direction.get('large_sell_ratio', 0),
            'kyle_lambda': microstructure.get('kyle_lambda', 0),
            'effective_spread': microstructure.get('effective_spread', 0),
            'volatility': volatility,
            'rsi': rsi,
            'trend_strength': trend_strength,
            'macd': macd,
            'macd_signal': macd_signal,
            'price_reversal': microstructure.get('price_reversal', 0),
            'buy_concentration': trade_direction.get('buy_concentration', 0),
            'beta': beta,
            'alpha': alpha
        }

        self._log_performance("analyze_stock_worker", task_start)
        return (symbol, result)



    def _get_realtime_quotes_worker(self):
        """获取实时行情数据"""
        task_start = time.time()
        try:
            spot_df = ak.stock_zh_a_spot_em()
            spot_df['代码'] = spot_df['代码'].apply(lambda x: f"SH{x}" if x.startswith('6') else f"SZ{x}")
            volume_ratios = spot_df.set_index('代码')['量比'].to_dict()
            current_prices = spot_df.set_index('代码')['最新价'].to_dict()
            change_pcts = spot_df.set_index('代码')['涨跌幅'].to_dict()
            turnover_rates = spot_df.set_index('代码')['换手率'].to_dict()
            self._log_performance("get_realtime_quotes", task_start)
            return volume_ratios, current_prices, change_pcts, turnover_rates
        except Exception as e:
            print(f"\n❌ 获取实时行情失败: {e}")
            self._log_performance("get_realtime_quotes", task_start)
            return {}, {}, {}, {}

    def analyze_stocks(self):
        """分析所有热门股票"""
        total_start = time.time()
        market_performance = self._get_market_performance()
        all_stocks = self.get_hot_stocks()
        if not all_stocks: return []

        symbols = [stock['代码'] for stock in all_stocks]

        print("\n📊 步骤 1/3: 批量获取历史和资金流数据...")
        historical_metrics = self._incremental_cache_batch_processor(symbols, self.historical_metrics_cache_file,
                                                                     self._get_historical_data, "历史行情")
        fund_flow_data = self._incremental_cache_batch_processor(symbols, self.fund_flow_cache_file,
                                                                 self._get_fund_flow_with_history, "资金流")

        print("\n📊 步骤 2/3: 并行获取Tick数据和实时行情...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            tick_future = executor.submit(self.get_tick_data_batch, symbols)
            realtime_future = executor.submit(self._get_realtime_quotes_worker)

            tick_data_results = tick_future.result()
            volume_ratios, current_prices, change_pcts, turnover_rates = realtime_future.result()

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
                    change_pcts.get(symbol, 0),
                    turnover_rates.get(symbol, 0)
                ))
            else:
                print(f"  ⚠️ {symbol} ({stock_dict.get(symbol, {}).get('股票名称', '')}) 缺少必要的历史行情数据，跳过")

        if not valid_stocks: return []

        print("\n📊 步骤 3/3: 批量分析并计算得分...")
        analysis_results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.analyze_stock_worker, s, df, market_performance, hm, ffd, vr, cp, chg, tr)
                       for s, df, hm, ffd, vr, cp, chg, tr in valid_stocks]
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

        text = f"# 📈 量化分析报告 V8.0 - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        text += f"## 🏆 股票评分排序 (Top {len(stocks_to_send)})\n\n"

        for i, (symbol, data) in enumerate(stocks_to_send, 1):
            model_tag = f"({data['model_version']})"

            change_pct = data.get('change_pct', 0)
            price_str = f"¥{data.get('current_price', 0):.2f}"
            change_str = f"{'📈' if change_pct > 0 else '📉'} {change_pct:.2f}%"
            turnover_str = f"换手: {data.get('turnover_rate', 0):.2f}%"
            title_line = f"### {i}. {data['name']} ({symbol})\n- **{price_str}** ({change_str}) | {turnover_str}\n"

            score_line = f"- **得分**: **{data['score']:.2f}** {model_tag}\n"

            z_score_line = f"- **资金流强度 (Z-score)**: 主力 **{data['fund_flow_z_score']:.2f}** / 超大单 **{data['super_flow_z_score']:.2f}**\n"

            text += f"""{title_line}{score_line}{z_score_line}- **量比**: {data.get('volume_ratio', 'N/A'):.2f}
- **买卖压力比**: {data.get('pressure_ratio', 1.0):.2f}
- **大单买入占比**: {data.get('large_buy_ratio', 0):.2%} vs 卖出 {data.get('large_sell_ratio', 0):.2%}
- **日内涨跌**: {data['intraday_change']:.2f}% (超额: {data['excess_return']:.2f}%)
- **净买入占比 (vs ADV20)**: {data['net_buy_adv_ratio']:.2%}
- **动量比率**: {data['momentum_ratio']:.2f} / 收盘: {data['closing_ratio']:.2f}
- **技术指标**: RSI {data.get('rsi', 0):.1f} | Beta {data.get('beta', 0):.2f}
- **对倒嫌疑**: {data.get('wash_trade_ratio', 0):.2%}
"""

        message = {"msgtype": "markdown", "markdown": {"title": "量化分析报告 V8.0", "text": text}}
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
        print("🔍 量化分析系统 V8.0 - 开始分析热门股票")
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
