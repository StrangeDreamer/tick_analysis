#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化分析系统：热门股票分析、主力拆单识别
"""

import os
import warnings
import sys

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
        self.custom_stocks = []  # 存储用户自定义分析的股票
        self.custom_stocks_file = "custom_stocks.json"  # 自定义股票存储文件
        self.hot_stocks_cache_file = "hot_stocks_cache.json"  # 热门股票缓存文件
        self.price_cyq_filter_cache_file = "price_cyq_filter_cache.json"  # 股价和筹码筛选缓存文件
        self.custom_stocks = self.load_custom_stocks()  # 加载自定义股票
        self.refresh_filter_cache = False  # 是否强制刷新筛选缓存
        
        # 并发配置（保留，仅用于控制线程数，默认10）
        self.max_workers = 10
    
    def _retry(self, func, *args, retries=3, base_delay=2, jitter=0.5, **kwargs):
        """带指数退避与抖动的通用重试包装"""
        import random, time
        attempt = 0
        last_err = None
        while attempt < retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_err = e
                wait = (base_delay * (2 ** attempt)) + random.uniform(0, jitter)
                print(f"⏳ 调用失败，{wait:.1f}s后重试... ({attempt+1}/{retries}) 错误: {e}")
                time.sleep(wait)
                attempt += 1
        raise last_err
    
    
    def _get_stock_name_by_code(self, code):
        """根据股票代码获取股票名称
        
        策略（按优先级）：
        1. 优先从缓存的市场数据中查找（如果已加载）
        2. 尝试使用东财个股信息接口（ak.stock_individual_info_em）
        3. 尝试使用新浪实时行情接口（ak.stock_zh_a_spot）
        4. 尝试使用麦瑞API接口（api.mairuiapi.com）
        5. 如果都没有，返回默认名称
        """
        # 方法1: 从缓存的市场数据中查找（如果已加载）
        if hasattr(self, '_market_data_cache') and self._market_data_cache is not None:
            try:
                stock_row = self._market_data_cache[self._market_data_cache['代码'] == code]
                if not stock_row.empty and '名称' in stock_row.columns:
                    stock_name = stock_row['名称'].iloc[0]
                    print(f"  ✅ 从缓存获取股票名称: {stock_name}")
                    return stock_name
            except Exception as e:
                pass  # 缓存中没有，继续尝试其他方法
        
        # 方法2: 尝试使用东财个股信息接口（最可靠）
        try:
            info_df = ak.stock_individual_info_em(symbol=code)
            if info_df is not None and not info_df.empty:
                # 查找'股票简称'这一行
                name_row = info_df[info_df['item'] == '股票简称']
                if not name_row.empty:
                    stock_name = name_row['value'].iloc[0]
                    if stock_name and pd.notna(stock_name):
                        stock_name = str(stock_name).strip()
                        print(f"  ✅ 从东财API获取股票名称: {stock_name}")
                        # 将获取到的名称添加到缓存，避免下次重复调用API
                        if not hasattr(self, '_stock_names_cache'):
                            self._stock_names_cache = {}
                        self._stock_names_cache[code] = stock_name
                        return stock_name
        except Exception as e:
            # API被限制，跳过
            pass
        
        # 方法3: 尝试使用实时行情接口（可能被限制，但尝试一下）
        try:
            spot_df = ak.stock_zh_a_spot()
            if spot_df is not None and not spot_df.empty:
                stock_row = spot_df[spot_df['代码'] == code]
                if not stock_row.empty and '名称' in stock_row.columns:
                    stock_name = stock_row['名称'].iloc[0]
                    print(f"  ✅ 从新浪API获取股票名称: {stock_name}")
                    # 将获取到的名称添加到缓存，避免下次重复调用API
                    if not hasattr(self, '_stock_names_cache'):
                        self._stock_names_cache = {}
                    self._stock_names_cache[code] = stock_name
                    return stock_name
        except Exception as e:
            # API被限制，跳过
            pass
        
        # 方法4: 尝试使用麦瑞API接口（备用方案）
        try:
            import requests
            # 转换股票代码格式（002513 -> 002513.SZ）
            if code.startswith('6'):
                api_code = f'{code}.SH'
            else:
                api_code = f'{code}.SZ'
            
            license = '0150F12C-5051-4482-9A43-66E14094B66C'
            url = f'http://api.mairuiapi.com/hsstock/instrument/{api_code}/{license}'
            
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and 'name' in data:
                    stock_name = data['name']
                    if stock_name and pd.notna(stock_name):
                        stock_name = str(stock_name).strip()
                        print(f"  ✅ 从麦瑞API获取股票名称: {stock_name}")
                        # 将获取到的名称添加到缓存，避免下次重复调用API
                        if not hasattr(self, '_stock_names_cache'):
                            self._stock_names_cache = {}
                        self._stock_names_cache[code] = stock_name
                        return stock_name
        except Exception as e:
            # API失败，跳过
            pass
        
        # 检查是否有运行时缓存（之前通过API获取的）
        if hasattr(self, '_stock_names_cache') and code in self._stock_names_cache:
            return self._stock_names_cache[code]
        
        # 如果都没有，返回默认名称
        print(f"  ⚠️ 未找到股票 {code} 的名称，使用默认名称")
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
            
            # 提取纯代码（去掉SH/SZ前缀）
            if code.startswith('SH') or code.startswith('SZ'):
                pure_code = code[2:]
            else:
                pure_code = code
            
            # 检查股票名称是否为空或默认值
            stock_name = stock.get('股票名称', '')
            if not stock_name or stock_name == f'股票{pure_code}' or stock_name.startswith('股票'):
                # 尝试获取股票名称
                new_name = self._get_stock_name_by_code(pure_code)
                if new_name and new_name != f'股票{pure_code}':
                    stock['股票名称'] = new_name
                    filled_count += 1
        
        if filled_count > 0:
            print(f"📝 已填充 {filled_count} 只股票的缺失名称")
        
        return stocks
    
    def _get_single_stock_realtime_info(self, symbol):
        """获取单只股票的实时价格
        
        只返回价格，不计算涨跌幅
        """
        try:
            # 去掉SH/SZ前缀（如果有）
            clean_symbol = symbol.replace('SH', '').replace('SZ', '')
            
            # 方法1: 使用 ak.stock_zh_a_minute() 接口获取实时价格
            try:
                # 转换股票代码格式
                if clean_symbol.startswith('6'):
                    minute_symbol = f'sh{clean_symbol}'
                elif clean_symbol.startswith(('0', '3')):
                    minute_symbol = f'sz{clean_symbol}'
                else:
                    minute_symbol = f'sh{clean_symbol}'  # 默认沪市
                
                # 获取分钟级数据
                minute_df = ak.stock_zh_a_minute(symbol=minute_symbol, period='1', adjust='qfq')
                
                # 详细检查数据有效性
                if minute_df is not None and hasattr(minute_df, 'empty') and not minute_df.empty:
                    if len(minute_df) > 0 and 'close' in minute_df.columns:
                        # 获取最新价格
                        latest_price = float(minute_df['close'].iloc[-1])
                        
                        return {
                            '最新价': latest_price
                        }
                    else:
                        print(f"  ⚠️ {symbol} 分钟级数据无效: 长度={len(minute_df) if minute_df is not None else 0}, 列={minute_df.columns.tolist() if minute_df is not None else 'N/A'}")
                else:
                    print(f"  ⚠️ {symbol} 分钟级数据为空")
            except Exception as e:
                print(f"  ⚠️ {symbol} 分钟级接口失败: {e}")
            
            # 方法2: 使用历史数据接口作为备用
            try:
                hist_df = ak.stock_zh_a_hist(symbol=clean_symbol, period='daily', adjust='qfq')
                if hist_df is not None and not hist_df.empty and len(hist_df) > 0:
                    latest_price = float(hist_df['收盘'].iloc[-1])
                    
                    return {
                        '最新价': latest_price
                    }
            except Exception as e:
                print(f"  ⚠️ {symbol} 历史数据接口失败: {e}")
            
        except Exception as e:
            print(f"❌ {symbol} 获取实时信息失败: {e}")
        
        # 如果所有方法都失败，返回默认值
        print(f"⚠️ {symbol} 所有接口失败，返回默认值: 最新价=10.0")
        return {
            '最新价': 10.0
        }

    # TuShare 相关功能已移除
    
    def load_custom_stocks(self):
        """从文件加载自定义股票"""
        try:
            if os.path.exists(self.custom_stocks_file):
                with open(self.custom_stocks_file, 'r', encoding='utf-8') as f:
                    stocks = json.load(f)
                    print(f"📂 加载了{len(stocks)}只自定义股票")
                    return stocks
            else:
                return []
        except Exception as e:
            print(f"⚠️ 加载自定义股票失败: {e}")
            return []
    
    def load_hot_stocks_cache(self, source=None):
        """从缓存文件加载热门股票
        
        Args:
            source: 数据源类型 ('zt', 'ljqs', 'hot_rank')，如果指定则只返回匹配数据源的缓存
        """
        try:
            if os.path.exists(self.hot_stocks_cache_file):
                with open(self.hot_stocks_cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    cache_date = cache_data.get('date', '')
                    cache_stocks = cache_data.get('stocks', [])
                    cache_source = cache_data.get('source', 'zt')  # 兼容旧缓存，默认为zt
                    
                    # 检查是否是今天的缓存
                    today = datetime.now().strftime('%Y-%m-%d')
                    if cache_date == today and cache_stocks:
                        # 如果指定了数据源，检查缓存的数据源是否匹配
                        if source is not None and cache_source != source:
                            source_names = {'zt': '昨日涨停池', 'ljqs': '量价齐升', 'hot_rank': '热门排行榜'}
                            print(f"⚠️ 缓存数据源不匹配 (缓存: {source_names.get(cache_source, cache_source)}, 需要: {source_names.get(source, source)})，需要重新获取")
                            return None
                        
                        source_name = {'zt': '昨日涨停池', 'ljqs': '量价齐升', 'hot_rank': '热门排行榜'}.get(cache_source, '未知')
                        print(f"📦 使用今日缓存的热门股票: {len(cache_stocks)}只 (数据源: {source_name}, 缓存时间: {cache_date})")
                        return cache_stocks
                    else:
                        print(f"⚠️ 缓存已过期 (缓存日期: {cache_date}, 今日: {today})，需要重新获取")
                        return None
            else:
                print("⚠️ 热门股票缓存文件不存在，需要重新获取")
                return None
        except Exception as e:
            print(f"⚠️ 加载热门股票缓存失败: {e}，需要重新获取")
            return None
    
    def save_hot_stocks_cache(self, hot_stocks, source=None):
        """保存热门股票到缓存文件
        
        Args:
            hot_stocks: 热门股票列表
            source: 数据源类型 ('zt', 'ljqs', 'hot_rank')，用于区分不同数据源的缓存
        """
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            cache_data = {
                'date': today,
                'stocks': hot_stocks,
                'source': source or getattr(self, 'stock_source', 'hot_rank'),  # 保存数据源信息
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            with open(self.hot_stocks_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            source_name = {'zt': '昨日涨停池', 'ljqs': '量价齐升', 'hot_rank': '热门排行榜'}.get(cache_data['source'], '未知')
            print(f"💾 热门股票已缓存: {len(hot_stocks)}只 (数据源: {source_name}, 日期: {today})")
        except Exception as e:
            print(f"⚠️ 保存热门股票缓存失败: {e}")
    
    def load_price_cyq_filter_cache(self):
        """从缓存加载股价和筹码筛选结果和上一交易日收盘价"""
        try:
            if not os.path.exists(self.price_cyq_filter_cache_file):
                return None, None
            
            with open(self.price_cyq_filter_cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 检查缓存日期是否是今天
            today = datetime.now().strftime('%Y-%m-%d')
            cache_date = cache_data.get('date')
            
            if cache_date == today:
                print(f"✅ 使用股价和筹码筛选缓存 (日期: {cache_date})")
                filtered_stocks = cache_data.get('filtered_stocks', [])
                previous_close_prices = cache_data.get('previous_close_prices', {})
                
                # 如果缓存中有上一交易日收盘价，加载到实例变量
                if previous_close_prices:
                    if not hasattr(self, 'previous_close_prices'):
                        self.previous_close_prices = {}
                    self.previous_close_prices.update(previous_close_prices)
                    print(f"✅ 已从缓存加载上一交易日收盘价: {len(previous_close_prices)}只股票")
                
                return filtered_stocks, previous_close_prices
            else:
                print(f"⚠️ 股价和筹码筛选缓存已过期 (缓存日期: {cache_date}, 今日: {today})")
                return None, None
        except Exception as e:
            print(f"⚠️ 加载股价和筹码筛选缓存失败: {e}")
            return None, None
    
    def save_price_cyq_filter_cache(self, filtered_stocks, previous_close_prices=None):
        """保存股价和筹码筛选结果和上一交易日收盘价到缓存"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            # 如果没有传入上一交易日收盘价，尝试从实例变量获取
            if previous_close_prices is None and hasattr(self, 'previous_close_prices'):
                previous_close_prices = self.previous_close_prices
            
            cache_data = {
                'date': today,
                'filtered_stocks': filtered_stocks,
                'previous_close_prices': previous_close_prices or {},
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            with open(self.price_cyq_filter_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 股价和筹码筛选结果已缓存: {len(filtered_stocks)}只股票 (日期: {today})")
            if previous_close_prices:
                print(f"💾 上一交易日收盘价已缓存: {len(previous_close_prices)}只股票")
        except Exception as e:
            print(f"⚠️ 保存股价和筹码筛选缓存失败: {e}")
    
    def get_stock_price_batch(self, stock_codes):
        """批量获取股票价格和换手率（使用日线数据接口）"""
        if not stock_codes:
            return {}, {}, {}
        
        print(f"💰 开始获取 {len(stock_codes)} 只股票的价格和换手率（使用 ak.stock_zh_a_daily() 接口，多线程）...")
        
        price_data = {}
        turnover_data = {}  # 换手率数据
        previous_close_data = {}  # 上一交易日收盘价数据
        successful_count = 0
        failed_count = 0
        
        # 提取纯数字代码（去除SH/SZ前缀）用于匹配
        code_map = {}
        for code in stock_codes:
            if code.startswith('SH') or code.startswith('SZ'):
                pure_code = code[2:]
            else:
                pure_code = code
            code_map[pure_code] = code
        
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import datetime as dt
            
            # 获取当前日期和最近2天的日期
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
            
            def get_price_from_daily(code_info):
                """从日线数据获取价格和换手率"""
                pure_code, full_code = code_info
                try:
                    # 转换代码格式（sz000001 或 sh600000）
                    if full_code.startswith('SH'):
                        symbol = f'sh{pure_code}'
                    elif full_code.startswith('SZ'):
                        symbol = f'sz{pure_code}'
                    else:
                        if pure_code.startswith('6'):
                            symbol = f'sh{pure_code}'
                        else:
                            symbol = f'sz{pure_code}'
                    
                    # 添加延迟，避免触发反爬虫
                    import random
                    import time
                    time.sleep(random.uniform(0.1, 0.3))
                    
                    # 获取日线数据（使用不复权，获取真实价格）
                    try:
                        df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust="")
                    except KeyError as e:
                        # 某些股票的API返回数据中可能缺少date字段，导致akshare内部报错
                        if 'date' in str(e):
                            print(f'  ⚠️ {full_code} ak.stock_zh_a_daily() 调用失败: API返回数据缺少date字段，可能该股票数据异常')
                        else:
                            print(f'  ⚠️ {full_code} ak.stock_zh_a_daily() 调用失败: KeyError: {e}')
                        return (full_code, None, None, None)
                    except Exception as e:
                        print(f'  ⚠️ {full_code} ak.stock_zh_a_daily() 调用失败: {type(e).__name__}: {e}')
                        return (full_code, None, None, None)
                    
                    df_available = df is not None and not df.empty
                    if df_available:
                        # 调试信息：打印原始数据
                        if full_code == 'SZ002333':
                            print(f"  🔍 调试 SZ002333 stock_zh_a_daily 原始返回数据:")
                            print(f"    - DataFrame 形状: {df.shape}")
                            print(f"    - 列名: {df.columns.tolist()}")
                            print(f"    - 原始数据 (前5条):")
                            print(df.head().to_string())
                            print(f"    - 原始数据 (后5条):")
                            print(df.tail().to_string())
                        
                        # 获取日期列名（尝试多种可能的列名）
                        date_col = None
                        available_cols = df.columns.tolist()
                        for col_name in ['date', '日期', '交易日期', 'Date', 'DATE']:
                            if col_name in df.columns:
                                date_col = col_name
                                break
                        
                        if date_col is None:
                            print(f'  ⚠️ {full_code} 无法找到日期列！')
                            print(f'     尝试的列名: [date, 日期, 交易日期, Date, DATE]')
                            print(f'     实际可用列: {available_cols}')
                            return (full_code, None, None, None)
                        
                        # 确保按日期排序（升序，最新的在最后）
                        try:
                            df = df.sort_values(date_col).reset_index(drop=True)
                        except KeyError as sort_error:
                            print(f'  ⚠️ {full_code} 排序失败: KeyError: {sort_error}, date_col={date_col}, 可用列: {df.columns.tolist()}')
                            return (full_code, None, None, None)
                        
                        # 获取最新一条数据的收盘价和换手率（最新交易日）
                        latest = df.iloc[-1]
                        # 价格字段兼容：close / 收盘 / 收盘价
                        price_keys = ['close', '收盘', '收盘价']
                        for k in price_keys:
                            if k in latest:
                                latest_price = float(latest[k])
                                break
                        else:
                            raise KeyError('close/收盘/收盘价 字段不存在')
                        
                        # 换手率字段兼容：turnover / turnover_rate / 换手率（可能为百分比字符串）
                        turnover_val = None
                        for k in ('turnover', 'turnover_rate', '换手率'):
                            if k in latest:
                                turnover_val = latest[k]
                                break
                        if turnover_val is not None:
                            if isinstance(turnover_val, str):
                                turnover_val = turnover_val.replace('%','')
                                latest_turnover = float(turnover_val) / 100.0
                            else:
                                latest_turnover = float(turnover_val)
                        else:
                            latest_turnover = None
                        
                        # 获取上一交易日的收盘价：根据日期字段找到上一个交易日
                        # stock_zh_a_daily 返回的 date 字段已经是 '1991-04-03' 格式的字符串
                        today = dt.datetime.now().strftime('%Y-%m-%d')
                        current_time = dt.datetime.now().time()
                        
                        # 判断是否已经开盘（9:25 集合竞价开始，可以取上一个交易日的数据）
                        is_market_open = current_time >= dt.time(9, 25)
                        
                        # 确保日期列是字符串格式（接口返回的可能是 datetime.date 类型，需要统一转换为字符串）
                        # 先检查日期列的实际类型，然后相应处理
                        try:
                            # 检查日期列的第一个值类型
                            first_date_val = df[date_col].iloc[0] if len(df) > 0 else None
                            if first_date_val is not None:
                                # 导入 datetime.date 用于类型检查
                                from datetime import date as date_type
                                # 如果是 datetime.date 类型，先转换为字符串再转换为 datetime，最后转换为字符串格式
                                if isinstance(first_date_val, date_type):
                                    df[date_col] = pd.to_datetime(df[date_col].astype(str)).dt.strftime('%Y-%m-%d')
                                elif isinstance(first_date_val, str):
                                    # 如果已经是字符串，直接转换
                                    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                                else:
                                    # 其他类型，尝试直接转换
                                    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                            else:
                                # 如果没有数据，跳过转换
                                pass
                        except KeyError as date_error:
                            print(f'  ⚠️ {full_code} 日期列转换失败: KeyError: {date_error}, date_col={date_col}, 可用列: {df.columns.tolist()}')
                            return (full_code, None, None, None)
                        except Exception as date_error:
                            print(f'  ⚠️ {full_code} 日期列转换失败: {type(date_error).__name__}: {date_error}, date_col={date_col}')
                            print(f'     日期列类型: {df[date_col].dtype if date_col in df.columns else "N/A"}, 第一个值类型: {type(df[date_col].iloc[0]) if date_col in df.columns and len(df) > 0 else "N/A"}')
                            return (full_code, None, None, None)
                        
                        # 如果还没开盘，需要找到 date < 今天 的最大日期（真正的上一个交易日）
                        # 如果已经开盘，可以取 date = 今天-1 的数据（上一个交易日）
                        if not is_market_open:
                            # 还没开盘：直接取第0条作为上一个交易日（真正的上一个交易日收盘价）
                            # 因为11-05的数据可能还没有收盘，或者数据不准确
                            # 所以直接取第0条（2025-11-04，收盘价6.15）
                            if len(df) > 0:
                                previous_trading_day_row = df.iloc[0]
                                previous_close = float(previous_trading_day_row['close'] if 'close' in previous_trading_day_row else (previous_trading_day_row['收盘'] if '收盘' in previous_trading_day_row else previous_trading_day_row['收盘价']))
                                previous_date = str(previous_trading_day_row[date_col])
                            else:
                                # 如果没有数据，使用最新一条的收盘价
                                previous_close = latest_price
                                previous_date = str(df[date_col].iloc[-1]) if len(df) > 0 else today
                        else:
                            # 已经开盘：可以取最新一条或倒数第二条（取决于是否有今天的数据）
                            latest_date = str(df[date_col].iloc[-1])
                            if latest_date == today and len(df) >= 2:
                                # 今天的数据存在，取倒数第二条作为上一交易日
                                previous_trading_day_row = df.iloc[-2]
                                previous_close = float(previous_trading_day_row['close'] if 'close' in previous_trading_day_row else (previous_trading_day_row['收盘'] if '收盘' in previous_trading_day_row else previous_trading_day_row['收盘价']))
                                previous_date = str(previous_trading_day_row[date_col])
                            elif latest_date < today:
                                # 最新一条的日期小于今天，说明最新一条就是上一个交易日
                                previous_close = latest_price
                                previous_date = latest_date
                            else:
                                # 如果只有一条数据或日期异常，使用最新一条的收盘价
                                previous_close = latest_price
                                previous_date = latest_date
                        
                        # 调试信息：打印日期和价格
                        if full_code == 'SZ002333':
                            print(f"  🔍 调试 SZ002333 昨日收盘价获取:")
                            print(f"    - 今天日期: {today}")
                            print(f"    - 当前时间: {current_time}, 是否已开盘: {is_market_open}")
                            print(f"    - 数据条数: {len(df)}")
                            print(f"    - 转换后的日期列:")
                            for idx, row in df.iterrows():
                                date_val = row[date_col]
                                close_val = row.get('close', row.get('收盘', row.get('收盘价', 'N/A')))
                                is_less_than_today = date_val < today
                                print(f"      第{idx}条: 日期={date_val} (类型={type(date_val).__name__}), 收盘价={close_val}, <今天: {is_less_than_today}")
                            if not is_market_open:
                                print(f"    - 未开盘模式：直接取第0条作为上一个交易日:")
                                if len(df) > 0:
                                    row0 = df.iloc[0]
                                    date_val = row0[date_col]
                                    close_val = row0.get('close', row0.get('收盘', row0.get('收盘价', 'N/A')))
                                    print(f"      第0条: 日期={date_val}, 收盘价={close_val} <-- 使用这条")
                            print(f"    - 最新一条日期: {str(df[date_col].iloc[-1])}, 收盘价: {latest_price:.2f}")
                            print(f"    - 上一交易日日期: {previous_date}, 收盘价: {previous_close:.2f}")
                            print(f"    - 使用上一交易日收盘价: {previous_close:.2f}")
                        
                        return (full_code, latest_price, latest_turnover, previous_close)
                    else:
                        return (full_code, None, None, None)
                except KeyError as e:
                    print(f'  ⚠️ {full_code} 日线解析失败: KeyError: {e}')
                    # 尝试获取可用列名（如果df存在）
                    try:
                        if df_available and df is not None:
                            print(f'     可用列: {df.columns.tolist()}')
                        else:
                            print(f'     数据为空或无效')
                    except:
                        pass
                    return (full_code, None, None, None)
                except Exception as e:
                    print(f'  ⚠️ {full_code} 日线解析失败: {type(e).__name__}: {e}')
                    return (full_code, None, None, None)
            
            # 使用多线程批量获取（限制并发数，避免封禁）
            total_count = len(code_map)
            completed_count = 0
            print(f"   总股票数: {total_count} 只，使用 {min(5, total_count)} 个线程并发获取...")
            
            # 计算进度打印间隔（每10只或每10%，取较小值）
            progress_interval = min(10, max(1, total_count // 10))
            
            with ThreadPoolExecutor(max_workers=min(5, len(code_map))) as executor:
                futures = [executor.submit(get_price_from_daily, (pure_code, full_code)) 
                          for pure_code, full_code in code_map.items()]
                
                for future in as_completed(futures):
                    try:
                        code, price, turnover, previous_close = future.result()
                        completed_count += 1
                        
                        if price is not None:
                            price_data[code] = price
                            if turnover is not None:
                                turnover_data[code] = turnover
                            if previous_close is not None:
                                previous_close_data[code] = previous_close
                            successful_count += 1
                        else:
                            failed_count += 1
                        
                        # 每完成一定数量或全部完成时打印进度
                        if completed_count % progress_interval == 0 or completed_count == total_count:
                            progress_percent = (completed_count / total_count) * 100
                            print(f"   进度: {completed_count}/{total_count} ({progress_percent:.1f}%) - 成功: {successful_count}, 失败: {failed_count}")
                    except Exception as e:
                        completed_count += 1
                        failed_count += 1
                        if completed_count % progress_interval == 0 or completed_count == total_count:
                            progress_percent = (completed_count / total_count) * 100
                            print(f"   进度: {completed_count}/{total_count} ({progress_percent:.1f}%) - 成功: {successful_count}, 失败: {failed_count}")
                
                print(f"📊 价格和换手率获取完成: 成功 {successful_count} 只，失败 {failed_count} 只")
                print(f"📊 昨日收盘价获取完成: {len(previous_close_data)} 只股票")
                if len(previous_close_data) > 0:
                    sample_codes = list(previous_close_data.keys())[:3]
                    for sample_code in sample_codes:
                        print(f"  示例: {sample_code} 昨日收盘价 = {previous_close_data[sample_code]:.2f}元")
                return price_data, turnover_data, previous_close_data
        except Exception as e:
            print(f"⚠️ ak.stock_zh_a_daily() 接口失败: {e}")
            return price_data, turnover_data, previous_close_data
    
    def get_cyq_data_worker(self, stock_code):
        """筹码分布数据获取的工作函数（单个股票）"""
        try:
            # 提取纯数字代码（去除SH/SZ前缀）
            if stock_code.startswith('SH') or stock_code.startswith('SZ'):
                pure_code = stock_code[2:]
            else:
                pure_code = stock_code
            
            # 添加随机延迟，避免触发反爬虫
            import random
            import time
            time.sleep(random.uniform(0.1, 0.3))  # 并发模式下的延迟
            
            # 获取筹码分布数据（后复权）
            df = ak.stock_cyq_em(symbol=pure_code, adjust='hfq')
            
            if df is not None and not df.empty:
                # 获取最新一条数据
                latest = df.iloc[-1]
                
                cyq_info = {
                    '获利比例': float(latest['获利比例']),
                    '平均成本': float(latest['平均成本']),
                    '90集中度': float(latest['90集中度']),
                    '90成本_低': float(latest['90成本-低']),
                    '90成本_高': float(latest['90成本-高'])
                }
                
                return (stock_code, cyq_info)
            else:
                return (stock_code, None)
                
        except Exception as e:
            return (stock_code, None)
    
    def get_cyq_data_batch(self, stock_codes):
        """批量获取筹码分布数据（多线程并发）"""
        if not stock_codes:
            return {}
        
        print(f"🔍 开始获取 {len(stock_codes)} 只股票的筹码分布数据（{self.max_workers}线程）...")
        
        cyq_data = {}
        successful_count = 0
        failed_count = 0
        failed_stocks = []
        total_count = len(stock_codes)
        processed_count = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 创建future到代码的映射
            future_to_code = {}
            for code in stock_codes:
                future = executor.submit(self.get_cyq_data_worker, code)
                future_to_code[future] = code
            
            # 收集结果
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    code_result, info = future.result()
                    processed_count += 1
                    progress = (processed_count / total_count) * 100
                    
                    if info is not None:
                        cyq_data[code_result] = info
                        successful_count += 1
                        # 每10只股票打印一次进度
                        if processed_count % 10 == 0 or processed_count == total_count:
                            print(f"  📈 进度: {processed_count}/{total_count} ({progress:.1f}%) | 成功: {successful_count} | 失败: {failed_count}")
                    else:
                        failed_count += 1
                        failed_stocks.append(code_result)
                        # 每10只股票打印一次进度
                        if processed_count % 10 == 0 or processed_count == total_count:
                            print(f"  📈 进度: {processed_count}/{total_count} ({progress:.1f}%) | 成功: {successful_count} | 失败: {failed_count}")
                except Exception as e:
                    processed_count += 1
                    failed_count += 1
                    failed_stocks.append(code)
                    progress = (processed_count / total_count) * 100
                    # 每10只股票打印一次进度
                    if processed_count % 10 == 0 or processed_count == total_count:
                        print(f"  📈 进度: {processed_count}/{total_count} ({progress:.1f}%) | 成功: {successful_count} | 失败: {failed_count}")
        
        print(f"📊 筹码数据获取完成: 成功 {successful_count} 只，失败 {failed_count} 只")
        if failed_stocks and len(failed_stocks) <= 10:
            print(f"  ⚠️ 失败的股票: {', '.join(failed_stocks)}")
        elif failed_stocks:
            print(f"  ⚠️ 失败的股票（前10只）: {', '.join(failed_stocks[:10])} ... 还有 {len(failed_stocks) - 10} 只")
        
        # 打印一些成功获取的示例（前5只）
        if cyq_data:
            sample_stocks = list(cyq_data.items())[:5]
            print(f"  ✅ 成功示例（前5只）:")
            for code, info in sample_stocks:
                profit_ratio = info.get('获利比例', 0)
                concentration = info.get('90集中度', 0)
                print(f"     {code}: 获利盘 {profit_ratio:.1%}, 集中度 {concentration:.3f}")
        
        return cyq_data
    
    def save_custom_stocks(self):
        """保存自定义股票到文件"""
        try:
            # 转换numpy类型为Python原生类型
            serializable_stocks = []
            for stock in self.custom_stocks:
                serializable_stock = {}
                for key, value in stock.items():
                    if hasattr(value, 'item'):  # numpy类型
                        serializable_stock[key] = value.item()
                    elif isinstance(value, dict):  # 嵌套字典
                        serializable_dict = {}
                        for k, v in value.items():
                            if hasattr(v, 'item'):
                                serializable_dict[k] = v.item()
                            else:
                                serializable_dict[k] = v
                        serializable_stock[key] = serializable_dict
                    else:
                        serializable_stock[key] = value
                serializable_stocks.append(serializable_stock)
            
            with open(self.custom_stocks_file, 'w', encoding='utf-8') as f:
                json.dump(serializable_stocks, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 保存自定义股票失败: {e}")
    
    def add_custom_stock(self, symbol, stock_name, score, trade_direction, stock_info=None):
        """将自定义分析的股票添加到热门股票池"""
        # 检查是否已存在
        existing_stock = None
        for stock in self.custom_stocks:
            if stock['代码'] == symbol:
                existing_stock = stock
                break
        
        # 使用实时信息或默认值
        if stock_info:
            latest_price = stock_info.get('最新价', 10.0)
            price_change = stock_info.get('涨跌幅', 0.0)
        else:
            latest_price = 10.0
            price_change = 0.0
        
        if existing_stock:
            # 更新现有股票信息（不保存得分，每次重新计算）
            existing_stock['最新价'] = latest_price
            existing_stock['涨跌幅'] = price_change
            print(f"📝 更新自定义股票池: {symbol} ({stock_name}) 价格:{latest_price} 涨跌幅:{price_change:.2f}%")
        else:
            # 添加新股票（不保存得分，每次重新计算）
            self.custom_stocks.append({
                '代码': symbol,
                '股票名称': stock_name,
                '最新价': latest_price,
                '涨跌幅': price_change
            })
            print(f"➕ 添加到自定义股票池: {symbol} ({stock_name}) 价格:{latest_price} 涨跌幅:{price_change:.2f}%")
        
        # 保存到文件
        self.save_custom_stocks()
        
    def get_yesterday_zt_stocks(self):
        """获取昨日涨停股票"""
        from datetime import datetime, timedelta
        
        # 获取今天的日期（接口会自动返回前一日涨停数据）
        today = datetime.now()
        
        # 如果今天是周末，往前推到上周五
        # 周一（0）传入，获取上周五的涨停
        # 周六（5）传入周五日期，获取周四的涨停
        # 周日（6）传入周五日期，获取周四的涨停
        if today.weekday() == 5:  # 周六
            today = today - timedelta(days=1)  # 往前推到周五
        elif today.weekday() == 6:  # 周日
            today = today - timedelta(days=2)  # 往前推到周五
        
        date_str = today.strftime('%Y%m%d')
        
        try:
            print(f"🔄 获取昨日涨停池（查询日期: {date_str}）...")
            zt_df = ak.stock_zt_pool_previous_em(date=date_str)
            
            if zt_df is not None and not zt_df.empty:
                print(f"✅ 获取到 {len(zt_df)} 只昨日涨停股票")
                
                # 添加股票代码前缀（SH/SZ）
                def add_prefix(code):
                    code = str(code).zfill(6)  # 补齐6位
                    if code.startswith('6'):
                        return f'SH{code}'
                    elif code.startswith(('0', '3')):
                        return f'SZ{code}'
                    else:
                        return code
                
                zt_df['代码'] = zt_df['代码'].apply(add_prefix)
            
                # 重命名列以保持一致性
                zt_df = zt_df.rename(columns={
                    '名称': '股票名称',
                    '最新价': '最新价'
                })
                
                # 添加涨跌幅列（昨日涨停池的涨跌幅是今日的）
                if '涨跌幅' not in zt_df.columns:
                    zt_df['涨跌幅'] = 0.0
                
                # 筛选条件：沪深主板、非ST的股票（保存到累积池前先筛选）
                filtered_zt_df = zt_df[
                    (zt_df['代码'].str.startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605'))) &  # 沪深主板
                    (~zt_df['股票名称'].str.contains('ST'))                                                          # 非ST股票
                ].copy()
                
                print(f"📊 筛选后保存到累积池: {len(zt_df)}只 → {len(filtered_zt_df)}只（沪深主板、非ST）")
                
                # 增量保存到累积池（只保存筛选后的股票）
                today_date_str = datetime.now().strftime('%Y-%m-%d')
                self.update_accumulated_zt_stocks(filtered_zt_df, today_date_str)
                
                return zt_df
            else:
                print("❌ 昨日涨停池数据为空")
                return None
        except Exception as e:
            print(f"❌ 获取昨日涨停池失败: {e}")
            return None
    
    def load_accumulated_zt_stocks(self):
        """加载累积的涨停股票池"""
        cache_file = "accumulated_zt_stocks.json"
        
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"📂 加载累积涨停股票池: {len(data.get('stocks', []))}只股票")
                    return data
            else:
                print("📂 累积涨停股票池文件不存在，将创建新文件")
                return {"stocks": [], "last_update": None}
        except Exception as e:
            print(f"❌ 加载累积涨停股票池失败: {e}")
            return {"stocks": [], "last_update": None}
    
    def save_accumulated_zt_stocks(self, accumulated_data):
        """保存累积的涨停股票池"""
        cache_file = "accumulated_zt_stocks.json"
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(accumulated_data, f, ensure_ascii=False, indent=2)
            print(f"💾 保存累积涨停股票池: {len(accumulated_data.get('stocks', []))}只股票")
        except Exception as e:
            print(f"❌ 保存累积涨停股票池失败: {e}")
    
    def update_accumulated_zt_stocks(self, new_zt_df, date_str):
        """增量更新累积的涨停股票池（只增加不减少）"""
        # 加载现有数据
        accumulated_data = self.load_accumulated_zt_stocks()
        existing_stocks_list = accumulated_data.get('stocks', [])
        
        # 筛选现有数据：只保留符合条件的股票（沪深主板、非ST）
        filtered_existing_stocks = []
        for stock in existing_stocks_list:
            code = stock.get('代码', '')
            name = stock.get('股票名称', '')
            # 筛选条件：沪深主板、非ST股票
            if (code.startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605')) and
                'ST' not in name):
                filtered_existing_stocks.append(stock)
        
        # 如果筛选后数量减少，说明有历史数据不符合条件
        if len(filtered_existing_stocks) < len(existing_stocks_list):
            removed_count = len(existing_stocks_list) - len(filtered_existing_stocks)
            print(f"📊 清理历史数据: 移除{removed_count}只不符合条件的股票（非主板或ST股票）")
        
        existing_stocks = {stock['代码']: stock for stock in filtered_existing_stocks}
        
        # 添加新股票
        new_count = 0
        updated_count = 0
        for _, row in new_zt_df.iterrows():
            stock_code = row['代码']
            if stock_code not in existing_stocks:
                # 新股票，添加到池中
                stock_info = {
                    '代码': stock_code,
                    '股票名称': row['股票名称'],
                    '首次涨停日期': date_str,
                    '最近涨停日期': date_str,
                    '最新价': float(row.get('最新价', 0)),
                    '涨停次数': 1
                }
                existing_stocks[stock_code] = stock_info
                new_count += 1
            else:
                # 已存在的股票，更新涨停次数和日期
                existing_stocks[stock_code]['涨停次数'] = existing_stocks[stock_code].get('涨停次数', 1) + 1
                existing_stocks[stock_code]['最近涨停日期'] = date_str
                existing_stocks[stock_code]['最新价'] = float(row.get('最新价', 0))
                updated_count += 1
        
        # 保存更新后的数据
        accumulated_data['stocks'] = list(existing_stocks.values())
        accumulated_data['last_update'] = date_str
        accumulated_data['total_count'] = len(existing_stocks)
        
        self.save_accumulated_zt_stocks(accumulated_data)
        
        print(f"📊 累积涨停股票池统计:")
        print(f"   - 总股票数: {len(existing_stocks)}只")
        print(f"   - 本次新增: {new_count}只")
        print(f"   - 本次更新: {updated_count}只")
        print(f"   - 更新日期: {date_str}")
    
    def get_hot_stocks(self, source='hot_rank'):
        """获取当日最热的沪深主板非ST A股股票
        
        Args:
            source: 热门股票源
                - 'hot_rank': 热门股票排行榜（默认）
                - 'zt': 昨日涨停池
                - 'ljqs': 量价齐升排行
        """
        # 先尝试从缓存加载（根据数据源匹配缓存）
        # 注意：只有在没有设置强制刷新时，才使用缓存
        if not (hasattr(self, 'refresh_filter_cache') and self.refresh_filter_cache):
            cached_stocks = self.load_hot_stocks_cache(source=source)
            if cached_stocks is not None:
                # 使用缓存的热门股票（只返回热门股票，不包含自定义股票）
                print(f"✅ 使用缓存: {len(cached_stocks)}只热门股票 + {len(self.custom_stocks)}只自定义股票 = 共{len(cached_stocks) + len(self.custom_stocks)}只股票")
                
                # 显示热门股票
                print("🔥 热门股票 (缓存):")
                for stock in cached_stocks[:10]:  # 只显示前10只
                    print(f"  {stock['代码']} {stock['股票名称']} 价格:{stock['最新价']} 涨跌幅:{stock['涨跌幅']:.2f}%")
                if len(cached_stocks) > 10:
                    print(f"  ... 还有 {len(cached_stocks) - 10} 只股票")
                
                # 显示自定义股票
                if self.custom_stocks:
                    print("⭐ 自定义股票:")
                    for stock in self.custom_stocks:
                        print(f"  {stock['代码']} {stock['股票名称']}")
                
                return cached_stocks  # 只返回热门股票，不包含自定义股票
        
        # 缓存不存在或已过期，或强制刷新，从API获取
        if source == 'zt':
            print("🔄 从API获取昨日涨停股票...")
            # 使用昨日涨停池
            zt_df = self.get_yesterday_zt_stocks()
            
            if zt_df is not None and not zt_df.empty:
                # 过滤条件：沪深主板、非ST的股票
                filtered_stocks = zt_df[
                    (zt_df['代码'].str.startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605'))) &  # 沪深主板
                    (~zt_df['股票名称'].str.contains('ST'))                                                          # 非ST股票
                ].copy()
                
                print(f"📊 筛选结果: {len(zt_df)}只 → {len(filtered_stocks)}只")
                print(f"   - 沪深主板: ✓")
                print(f"   - 非ST股票: ✓")
                
                # 转换为字典格式
                temp_stocks = []
                for _, stock in filtered_stocks.iterrows():
                    temp_stocks.append({
                        '代码': stock['代码'],
                        '股票名称': stock['股票名称'],
                        '最新价': stock['最新价'],
                        '涨跌幅': stock['涨跌幅']
                    })
                
                # 填充缺失的股票名称
                temp_stocks = self._fill_missing_stock_names(temp_stocks)
                
                # 直接使用筛选后的股票
                final_stocks = temp_stocks
                
                # 保存热门股票到缓存（标记数据源为zt）
                self.save_hot_stocks_cache(final_stocks, source='zt')
                
                # 显示筛选结果
                if len(final_stocks) > 0:
                    print(f"✅ 获取{len(final_stocks)}只热门股票 + {len(self.custom_stocks)}只自定义股票 = 共{len(final_stocks) + len(self.custom_stocks)}只股票")
                    
                    # 显示热门股票
                    print("🔥 热门股票（昨日涨停池）:")
                    for stock in final_stocks[:10]:
                        print(f"  {stock['代码']} {stock['股票名称']} 价格:{stock['最新价']} 涨跌幅:{stock['涨跌幅']:.2f}%")
                    if len(final_stocks) > 10:
                        print(f"  ... 还有 {len(final_stocks) - 10} 只股票")
                else:
                    print(f"⚠️ 筛选后热门股票为空（初始{len(temp_stocks)}只）")
                    print(f"   可能原因：")
                    print(f"   1. 基础筛选后没有符合条件的股票")
                    print(f"✅ 只获取{len(self.custom_stocks)}只自定义股票")
                
                # 显示自定义股票
                if self.custom_stocks:
                    print("⭐ 自定义股票:")
                    for stock in self.custom_stocks:
                        print(f"  {stock['代码']} {stock['股票名称']}")
                
                return final_stocks  # 只返回热门股票，不包含自定义股票
            else:
                print("❌ 昨日涨停池获取失败，尝试兜底接口（量价齐升）")
                source = 'ljqs'  # 降级到量价齐升
        
        # 使用热门股票排行榜
        if source == 'hot_rank':
            # 如果设置了强制刷新筛选缓存，也强制刷新热门排行榜缓存
            if hasattr(self, 'refresh_filter_cache') and self.refresh_filter_cache:
                print("🔄 强制刷新筛选缓存，同时刷新热门排行榜缓存...")
                # 删除热门股票缓存文件（如果是hot_rank数据源）
                if os.path.exists(self.hot_stocks_cache_file):
                    try:
                        with open(self.hot_stocks_cache_file, 'r', encoding='utf-8') as f:
                            cache_data = json.load(f)
                            cache_source = cache_data.get('source', 'zt')
                            if cache_source == 'hot_rank':
                                os.remove(self.hot_stocks_cache_file)
                                print("🔄 已删除热门排行榜缓存，将重新获取...")
                    except:
                        pass
            # 注意：缓存检查已在函数开始处统一处理，这里直接调用API
            
            print("🔄 从API获取热门股票排行榜...")
        
            hot_rank_df = ak.stock_hot_rank_em()
        
            if hot_rank_df is not None and not hot_rank_df.empty:
                # 过滤条件：沪深主板、非ST的股票
                filtered_stocks = hot_rank_df[
                    (hot_rank_df['代码'].str.startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605'))) &  # 沪深主板
                    (~hot_rank_df['股票名称'].str.contains('ST'))                                                          # 非ST股票
                ].copy()
                print(f"📊 筛选结果: {len(hot_rank_df)}只 → {len(filtered_stocks)}只")
                print(f"   - 沪深主板: ✓")
                print(f"   - 非ST股票: ✓")
                
                # 转换为字典格式
                temp_stocks = []
                for _, stock in filtered_stocks.iterrows():
                    temp_stocks.append({
                        '代码': stock['代码'],
                        '股票名称': stock['股票名称'],
                        '最新价': stock['最新价'],
                        '涨跌幅': stock['涨跌幅']
                    })
                
                # 填充缺失的股票名称
                temp_stocks = self._fill_missing_stock_names(temp_stocks)
                
                # 直接使用筛选后的股票
                final_stocks = temp_stocks
                
                # 保存热门股票到缓存（标记数据源为hot_rank）
                self.save_hot_stocks_cache(final_stocks, source='hot_rank')
                
                # 显示筛选结果
                if len(final_stocks) > 0:
                    print(f"✅ 获取{len(final_stocks)}只热门股票 + {len(self.custom_stocks)}只自定义股票 = 共{len(final_stocks) + len(self.custom_stocks)}只股票")
                    
                    # 显示热门股票
                    print("🔥 热门股票（热门排行榜）:")
                    for stock in final_stocks[:10]:
                        print(f"  {stock['代码']} {stock['股票名称']} 价格:{stock['最新价']} 涨跌幅:{stock['涨跌幅']:.2f}%")
                    if len(final_stocks) > 10:
                        print(f"  ... 还有 {len(final_stocks) - 10} 只股票")
                else:
                    print(f"⚠️ 筛选后热门股票为空（初始{len(temp_stocks)}只）")
                    print(f"   可能原因：")
                    print(f"   1. 基础筛选后没有符合条件的股票")
                    print(f"✅ 只获取{len(self.custom_stocks)}只自定义股票")
                
                # 显示自定义股票
                if self.custom_stocks:
                    print("⭐ 自定义股票:")
                    for stock in self.custom_stocks:
                        print(f"  {stock['代码']} {stock['股票名称']}")
                
                    return final_stocks  # 只返回热门股票，不包含自定义股票
            else:
                print("❌ 热门股票排行榜数据为空，尝试兜底接口（量价齐升）")
                source = 'ljqs'  # 降级到量价齐升
            
        
        # 兜底使用量价齐升
        if source == 'ljqs':
            print("🔄 从API获取量价齐升股票...")
            try:
                # 兜底方法: 使用量价齐升排行（同花顺）
                ljqs_df = ak.stock_rank_ljqs_ths()
                
                if ljqs_df is not None and not ljqs_df.empty:
                    print(f"✅ 获取到 {len(ljqs_df)} 只量价齐升股票")
                
                # 添加股票代码前缀（SH/SZ）
                def add_prefix(code):
                    code = str(code).zfill(6)  # 补齐6位
                    if code.startswith('6'):
                        return f'SH{code}'
                    elif code.startswith(('0', '3')):
                        return f'SZ{code}'
                    else:
                        return code
                ljqs_df['代码'] = ljqs_df['股票代码'].apply(add_prefix)
                
                # 重命名列以保持一致性
                ljqs_df = ljqs_df.rename(columns={
                    '股票简称': '股票名称'
                })
                
                # 过滤条件：量价齐升>=2天、沪深主板、非ST的股票、股价在5-30元之间、阶段涨幅<=9.8%、换手率>=5%
                filtered_stocks = ljqs_df[
                    (ljqs_df['量价齐升天数'] >= 2) &                                                                # 至少2天量价齐升
                    (ljqs_df['代码'].str.startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605'))) &  # 沪深主板
                    (~ljqs_df['股票名称'].str.contains('ST')) &                                                      # 非ST股票
                    (ljqs_df['最新价'] >= 5) &                                                                       # 价格>=5元
                    (ljqs_df['最新价'] <= 30) &                                                                      # 价格<=30元
                    (ljqs_df['阶段涨幅'] <= 9.8)                                                                    # 涨幅<=9.8%
                ].copy()
                
                # 按量价齐升天数和阶段涨幅排序
                filtered_stocks = filtered_stocks.sort_values(['量价齐升天数', '阶段涨幅'], ascending=[False, False])
                
                print(f"📊 筛选结果: {len(ljqs_df)}只 → {len(filtered_stocks)}只")
                print(f"   - 量价齐升≥2天: ✓")
                print(f"   - 沪深主板: ✓")
                print(f"   - 非ST股票: ✓")
                print(f"   - 价格5-30元: ✓")
                print(f"   - 阶段涨幅≤9.8%: ✓")
                
                # 转换为字典格式（使用阶段涨幅作为涨跌幅）
                temp_stocks = []
                for _, stock in filtered_stocks.iterrows():
                    temp_stocks.append({
                        '代码': stock['代码'],
                        '股票名称': stock['股票名称'],
                        '最新价': stock['最新价'],
                        '涨跌幅': stock['阶段涨幅']
                    })
                
                # 填充缺失的股票名称
                temp_stocks = self._fill_missing_stock_names(temp_stocks)
                
                # 直接使用筛选后的股票
                final_stocks = temp_stocks
                
                # 保存热门股票到缓存（标记数据源为ljqs）
                self.save_hot_stocks_cache(final_stocks, source='ljqs')
                
                # 显示筛选结果
                if len(final_stocks) > 0:
                    print(f"✅ 获取{len(final_stocks)}只热门股票 + {len(self.custom_stocks)}只自定义股票 = 共{len(final_stocks) + len(self.custom_stocks)}只股票")
                    
                    # 显示热门股票
                    print("🔥 热门股票（量价齐升）:")
                    for stock in final_stocks[:10]:  # 只显示前10只
                        print(f"  {stock['代码']} {stock['股票名称']} 价格:{stock['最新价']} 涨跌幅:{stock['涨跌幅']:.2f}%")
                    if len(final_stocks) > 10:
                        print(f"  ... 还有 {len(final_stocks) - 10} 只股票")
                else:
                    print(f"⚠️ 筛选后热门股票为空（初始{len(temp_stocks)}只）")
                    print(f"   可能原因：")
                    print(f"   1. 基础筛选后没有符合条件的股票")
                    print(f"✅ 只获取{len(self.custom_stocks)}只自定义股票")
                
                # 显示自定义股票（只显示代码和名称）
                if self.custom_stocks:
                    print("⭐ 自定义股票:")
                    for stock in self.custom_stocks:
                        print(f"  {stock['代码']} {stock['股票名称']}")
                
                return final_stocks  # 只返回热门股票，不包含自定义股票
            except Exception as e:
                print(f"❌ 获取量价齐升股票失败: {e}")
                print("🔄 尝试使用兜底接口: ak.stock_hot_deal_xq")
                
                try:
                    # 方法2: 使用兜底接口 - 雪球热门成交
                    hot_follow_df = ak.stock_hot_deal_xq(symbol="最热门")
                    
                    if hot_follow_df is not None and not hot_follow_df.empty:
                        print(f"✅ 兜底接口成功，获取到 {len(hot_follow_df)} 条数据")
            
                # 重命名列以保持一致性
                        hot_follow_df = hot_follow_df.rename(columns={
                            '股票代码': '代码',
                            '股票简称': '股票名称',
                            '最新价': '最新价'
                        })
                    
                        # 过滤条件：沪深主板、非ST的股票、股价在5-30元之间
                        filtered_stocks = hot_follow_df[
                            (hot_follow_df['代码'].str.startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605'))) &
                            (~hot_follow_df['股票名称'].str.contains('ST')) &
                            (hot_follow_df['最新价'] >= 5) &
                            (hot_follow_df['最新价'] <= 30)
                        ].copy()
                    
                        # 按关注度排序，取前50只
                        filtered_stocks = filtered_stocks.sort_values('关注', ascending=False).head(50)
                    
                        # 添加涨跌幅列（兜底接口没有涨跌幅，设为0）
                        filtered_stocks['涨跌幅'] = 0.0
                    
                        print(f"📊 兜底接口筛选结果: {len(filtered_stocks)} 只股票")
                    
                        # 转换为字典格式
                        temp_stocks = pd.DataFrame(filtered_stocks)[['代码', '股票名称', '最新价', '涨跌幅']].to_dict('records')
                    
                        # 填充缺失的股票名称
                        temp_stocks = self._fill_missing_stock_names(temp_stocks)
                    
                        # 直接使用筛选后的股票
                        final_stocks = temp_stocks
                    
                        # 保存热门股票到缓存（标记数据源为ljqs）
                        self.save_hot_stocks_cache(final_stocks, source='ljqs')
                    
                        # 显示筛选结果
                        if len(final_stocks) > 0:
                            print(f"✅ 兜底接口获取{len(final_stocks)}只热门股票 + {len(self.custom_stocks)}只自定义股票 = 共{len(final_stocks) + len(self.custom_stocks)}只股票")
                            
                            # 显示热门股票
                            print("🔥 热门股票 (兜底接口):")
                            for stock in final_stocks:
                                print(f"  {stock['代码']} {stock['股票名称']} 价格:{stock['最新价']} 涨跌幅:{stock['涨跌幅']:.2f}%")
                        else:
                            print(f"⚠️ 筛选后热门股票为空（初始{len(temp_stocks)}只 → 筹码筛选后0只）")
                            print(f"   可能原因：")
                            print(f"   1. 所有股票都被筹码筛选过滤（获利盘>70%或集中度>0.11）")
                            print(f"✅ 只获取{len(self.custom_stocks)}只自定义股票")
                    
                        # 显示自定义股票（只显示代码和名称）
                        if self.custom_stocks:
                            print("⭐ 自定义股票:")
                            for stock in self.custom_stocks:
                                print(f"  {stock['代码']} {stock['股票名称']}")
                    
                        return final_stocks  # 只返回热门股票，不包含自定义股票
                    # 兜底接口为空时，不在此处返回，由外层异常处理决定
                except Exception as e2:
                    print(f"❌ 兜底接口也失败: {e2}")
            return []
    
    
    def get_combined_stocks(self):
        """获取合并后的股票列表（热门股票 + 自定义股票）"""
        # 重新加载自定义股票（支持多线程动态更新）
        self.custom_stocks = self.load_custom_stocks()
        
        # 尝试获取热门股票，如果失败则使用空列表
        try:
            # 使用用户选择的股票源
            source = getattr(self, 'stock_source', 'hot_rank')
            hot_stocks = self.get_hot_stocks(source=source)
        except Exception as e:
            print(f"⚠️ 热门股票获取失败，跳过热门股票: {e}")
            hot_stocks = []
        
        # 合并热门股票和自定义股票
        all_stocks = hot_stocks + self.custom_stocks
        
        # 去重（以代码为准）
        seen_symbols = set()
        unique_stocks = []
        duplicate_count = 0
        for stock in all_stocks:
            if stock['代码'] not in seen_symbols:
                unique_stocks.append(stock)
                seen_symbols.add(stock['代码'])
            else:
                duplicate_count += 1
        
        # 显示股票统计信息
        hot_count = len(hot_stocks)
        custom_count = len(self.custom_stocks)
        total_count = len(unique_stocks)
        total_before_dedup = hot_count + custom_count
        
        if hot_count > 0:
            if duplicate_count > 0:
                print(f"✅ 获取{hot_count}只热门股票 + {custom_count}只自定义股票 = 共{total_before_dedup}只（去重{duplicate_count}只） = 实际{total_count}只股票")
            else:
                print(f"✅ 获取{hot_count}只热门股票 + {custom_count}只自定义股票 = 共{total_count}只股票")
        else:
            print(f"⚠️ 热门股票为空，只使用自定义股票")
            print(f"   可能原因：")
            print(f"   1. 缓存已过期或不存在，API获取失败")
            print(f"   2. 基础筛选后没有符合条件的股票")
            print(f"   3. 今日不是交易日，无法获取昨日涨停池")
            print(f"✅ 获取{custom_count}只自定义股票 = 共{total_count}只股票")
        
        return unique_stocks
    
    
    def get_tick_data(self, symbol, date=None):
        """获取股票的tick数据"""
    
        if symbol.startswith('SZ'):
            tick_symbol = symbol.lower()
        elif symbol.startswith('SH'):
            tick_symbol = symbol.lower()
        else:
            if symbol.startswith('6'):
                tick_symbol = f'sh{symbol}'
            elif symbol.startswith(('0', '3')):
                tick_symbol = f'sz{symbol}'
            else:
                tick_symbol = symbol
        
        print(f"  获取 {symbol} ({tick_symbol}) 的tick数据...")
        
        # 使用AKShare的stock_zh_a_tick_tx_js函数
        
        tick_df = ak.stock_zh_a_tick_tx_js(symbol=tick_symbol)
        
            
        if tick_df is not None and not tick_df.empty:
            print(f"  成功获取 {len(tick_df)} 条tick数据")
                
                # 重命名列以保持一致性
            tick_df = tick_df.rename(columns={
                '成交时间': '时间',
                '成交价格': '成交价',
                '价格变动': '价格变动',
                '成交量': '成交量',
                '成交金额': '成交额',
                '性质': '买卖盘性质'
                })
                
                # 转换时间格式
            tick_df['时间'] = pd.to_datetime(tick_df['时间'])
                    
                    # 按时间排序
            tick_df = tick_df.sort_values('时间')
                    
                    # 计算资金流向相关指标
                # 1. 使用API返回的价格变动（元）
            tick_df['dp'] = tick_df['价格变动']

                    # 2. 价变权重
            tick_df['w1'] = np.tanh(np.abs(tick_df['dp']) / 0.01) * np.sign(tick_df['dp'])

                    # 3. 量权重（20 笔滚动）
            tick_df['meanV'] = tick_df['成交量'].rolling(20, min_periods=1).mean()
            tick_df['w2'] = np.minimum(1, tick_df['成交量'] / (3 * tick_df['meanV']))

                    # 4. 指数平滑方向强度
            alpha = 2 / 6
            tick_df['prob'] = (tick_df['w1'] * tick_df['w2']).ewm(alpha=alpha, adjust=False).mean()

                    # 5. 资金流向（元）
            tick_df['mf'] = tick_df['prob'] * tick_df['成交额']
                    
                    # 6. 基于mf重新计算买卖盘性质和成交量
                # 如果mf是负数，就是主动流出（卖盘）；如果mf接近0，就是中性盘
            def classify_trade_type(mf):
                if mf < 0:
                    return '卖盘'
                else:
                    return '买盘'
            
            tick_df['买卖盘性质'] = tick_df['mf'].apply(classify_trade_type)
                
            # 重新计算成交量：|mf|/成交价，转换为手数（1手=100股）
            tick_df['成交量'] = (np.abs(tick_df['mf']) / tick_df['成交价'] / 100).round().astype(int)
            # 重新计算成交金额：|mf|
            tick_df['成交额'] = np.abs(tick_df['mf']).round().astype(int)
            # 过滤掉成交量为0的记录（无效数据）
            original_len = len(tick_df)
            tick_df = tick_df[tick_df['成交量'] > 0].copy()
            filtered_len = len(tick_df)
            if original_len > filtered_len:
                print(f"  过滤无效数据: {original_len}条 → {filtered_len}条 (移除{original_len - filtered_len}条成交量为0的记录)")
            
            # 检查过滤后是否还有数据
            if len(tick_df) == 0:
                print(f"  ⚠️ {symbol} 过滤后数据为空，返回None")
                return None
            
            # 打印最早的2条+最新的3条tick数据
            if len(tick_df) > 5:
                print(f"  📊 最早的2条tick数据:")
                print(tick_df.head(2))
                print(f"  📊 最新的3条tick数据:")
                print(tick_df.tail(3))
            else:
                print(f"  📊 所有tick数据（共{len(tick_df)}条）:")
                print(tick_df)
                
                # 返回处理后的数据
            try:
                result = tick_df[['时间', '成交价', '成交量', '成交额', '买卖盘性质', 'meanV', 'w2', 'prob', 'mf']]
                print(f"  ✅ {symbol} 成功处理并返回 {len(result)} 条数据")
                return result
            except KeyError as e:
                print(f"  ❌ {symbol} 提取列失败: {e}, 可用列: {tick_df.columns.tolist()}")
                return None
        else:
            # API返回了None或空数据
            if tick_df is None:
                print(f"  ⚠️ {symbol} API返回None（可能原因：股票停牌、代码错误、接口限制或该股票不在交易时间）")
            else:
                print(f"  ⚠️ {symbol} API返回空数据（共{len(tick_df)}条，可能该股票今日无交易数据）")
                return None
                    

    def get_tick_data_worker(self, symbol):
        """多线程工作函数：获取单只股票的tick数据"""
        try:
            tick_df = self.get_tick_data(symbol)
            return symbol, tick_df
        except Exception as e:
            print(f"  {symbol} 多线程获取tick数据失败: {e}")
            return symbol, None
    def get_tick_data_batch(self, symbols, max_workers=10):
        print(f"🚀 开始多线程获取 {len(symbols)} 只股票的tick数据（{max_workers}个线程）...")
        
        tick_data_results = {}
        successful_count = 0
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_symbol = {
                executor.submit(self.get_tick_data_worker, symbol): symbol 
                for symbol in symbols
            }
            
            # 收集结果
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    symbol, tick_df = future.result()
                    if tick_df is not None:
                        tick_data_results[symbol] = tick_df
                        successful_count += 1
                        print(f"  ✅ {symbol} 获取成功")
                    else:
                        failed_count += 1
                        print(f"  ❌ {symbol} 获取失败（返回None，可能原因：API限制、股票停牌、无交易数据或不在交易时间）")
                except Exception as e:
                    failed_count += 1
                    print(f"  ❌ {symbol} 获取异常: {e}")
        
        print(f"📊 批量获取完成: 成功 {successful_count} 只，失败 {failed_count} 只")
        return tick_data_results
    def analyze_trade_direction(self, tick_df, symbol):
        """分析股票的主动买卖性质"""
        if tick_df is None or tick_df.empty:
            return {
                'buy_ratio': 0, 
                'sell_ratio': 0, 
                'net_buy_volume': 0,
                'active_buy_ratio': 0,
                'active_sell_ratio': 0,
                'buy_volume': 0,
                'sell_volume': 0,
                'total_trades': 0
            }
        
        # 统计买卖盘性质
        trade_counts = tick_df['买卖盘性质'].value_counts()
        total_trades = len(tick_df)
        
        # 计算各种交易类型的比例
        buy_count = trade_counts.get('买盘', 0)
        sell_count = trade_counts.get('卖盘', 0)
        
        buy_ratio = buy_count / total_trades if total_trades > 0 else 0
        sell_ratio = sell_count / total_trades if total_trades > 0 else 0
        
        # 计算净买入量（买盘成交量 - 卖盘成交量）
        buy_volume = tick_df[tick_df['买卖盘性质'] == '买盘']['成交量'].sum()
        sell_volume = tick_df[tick_df['买卖盘性质'] == '卖盘']['成交量'].sum()
        net_buy_volume = buy_volume - sell_volume
        
        # 计算主动买卖强度（基于成交量而不是交易次数）
        total_volume = buy_volume + sell_volume
        active_buy_ratio = buy_volume / total_volume if total_volume > 0 else 0.5
        active_sell_ratio = sell_volume / total_volume if total_volume > 0 else 0.5
        
        return {
            'buy_ratio': buy_ratio,
            'sell_ratio': sell_ratio,
            'net_buy_volume': net_buy_volume,
            'active_buy_ratio': active_buy_ratio,
            'active_sell_ratio': active_sell_ratio,
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'total_trades': total_trades
        }
    
    def calculate_score(self, symbol, tick_df, trade_direction):
        """计算股票上涨概率得分"""
        if tick_df is None or tick_df.empty:
            return 0
            
        score = 0
        
        # 1. 主动买卖强度得分 (70%) - 主要因子
        active_buy_ratio = trade_direction['active_buy_ratio']
        active_sell_ratio = trade_direction['active_sell_ratio']
        
        # 主动买入比例越高，得分越高
        buy_sell_score = (active_buy_ratio - active_sell_ratio) * 70  # 扩大范围到-70到+70
        buy_sell_score = min(max(buy_sell_score, -50), 50)  # 限制在-50到+50
        score += buy_sell_score * 0.70
        
        # 2. 净买入量得分 (30%) - 次要因子
        net_buy_volume = trade_direction['net_buy_volume']
        avg_volume = tick_df['成交量'].mean()
        # 优化净买入量计算，使用更合理的系数
        net_buy_score = min(max(net_buy_volume / (avg_volume * 10), -15), 15)  # 扩大范围到-15到+15
        score += net_buy_score * 0.30
        
        return score
    
    def analyze_custom_stocks_only(self, custom_stocks):
        """只分析自定义股票（不获取热门股票）- 批量并发处理"""
        all_stocks = custom_stocks
        
        if not all_stocks:
            print("❌ 没有股票需要分析")
            return []
        
        # 提取股票代码列表
        symbols = [stock['代码'] for stock in all_stocks]
        
        # 步骤0: 在获取tick数据前，先筛选股价、换手率和筹码分布（每个开市日只执行一次）
        print(f"📊 步骤0/4: 筛选自定义股票（股价5-30元 + 换手率>=3% + 筹码分布）...")
        
        # 先尝试从缓存加载（如果设置了强制刷新，则跳过缓存）
        if self.refresh_filter_cache:
            print("🔄 强制刷新筛选缓存，跳过缓存加载...")
            filtered_stocks = None
            cached_previous_close = None
        else:
            filtered_stocks, cached_previous_close = self.load_price_cyq_filter_cache()
        
        if filtered_stocks is None:
            # 缓存不存在或已过期，从API获取并筛选
            # 步骤1: 先批量获取股票价格和换手率并过滤
            print(f"  📊 步骤1/3: 获取股票价格和换手率并筛选（5-30元，换手率>=3%）...")
            price_data, turnover_data, previous_close_data = self.get_stock_price_batch(symbols)
            
            # 保存上一交易日收盘价到实例变量，供后续使用
            if not hasattr(self, 'previous_close_prices'):
                self.previous_close_prices = {}
            self.previous_close_prices.update(previous_close_data)
            
            # 先过滤价格（5-30元）和换手率（>=3%）
            price_filtered_stocks = []
            excluded_price = 0
            excluded_turnover = 0
            stock_dict = {stock['代码']: stock for stock in all_stocks}
            
            for symbol in symbols:
                stock = stock_dict[symbol]
                price = price_data.get(symbol)
                turnover = turnover_data.get(symbol)
                
                # 检查股价（5-30元）
                if price is None:
                    # 如果无法获取价格，使用股票信息中的最新价
                    if '最新价' in stock:
                        price = float(stock['最新价'])
                    else:
                        excluded_price += 1
                        print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 无法获取价格，跳过")
                continue
                
                if price < 5.0 or price > 30.0:
                    excluded_price += 1
                    print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 股价: {price:.2f}元 (不在5-30元范围内，剔除)")
                    continue
                
                # 检查换手率（>=3%，即>=0.03）
                if turnover is not None:
                    if turnover < 0.03:  # 换手率小于3%
                        excluded_turnover += 1
                        print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 换手率: {turnover:.2%} (<3%，剔除)")
                        continue
                else:
                    # 无法获取换手率，也剔除（保守策略）
                    excluded_turnover += 1
                    print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 无法获取换手率，跳过")
                    continue
                
                # 价格和换手率都符合条件，保留
                price_filtered_stocks.append(stock)
            
            if excluded_price > 0 or excluded_turnover > 0:
                print(f"  📊 价格和换手率筛选结果: {len(symbols)}只 → {len(price_filtered_stocks)}只（剔除价格{excluded_price}只，换手率{excluded_turnover}只）")
            
            # 步骤2: 对价格和换手率筛选后的股票，获取筹码分布并过滤
            if price_filtered_stocks:
                print(f"  📊 步骤2/3: 获取筹码分布并筛选（获利盘<70%, 集中度<0.11）...")
                price_filtered_symbols = [stock['代码'] for stock in price_filtered_stocks]
                cyq_data = self.get_cyq_data_batch(price_filtered_symbols)
                
                # 筛选符合条件的股票
                filtered_stocks = []
                excluded_cyq = 0
                price_filtered_dict = {stock['代码']: stock for stock in price_filtered_stocks}
                
                for symbol in price_filtered_symbols:
                    stock = price_filtered_dict[symbol]
                    price = price_data.get(symbol)
                    cyq_info = cyq_data.get(symbol)
                    
                    # 检查筹码分布
                    if cyq_info is not None:
                        profit_ratio = cyq_info['获利比例']
                        concentration = cyq_info['90集中度']
                        # 筛选条件：获利比例 < 70% 且 90集中度 < 0.11
                        if profit_ratio >= 0.70 or concentration >= 0.11:
                            excluded_cyq += 1
                            reason = []
                            if profit_ratio >= 0.70:
                                reason.append(f"获利盘{profit_ratio:.1%}过高")
                            if concentration >= 0.11:
                                reason.append(f"集中度{concentration:.3f}过大")
                            print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 筹码分布: {', '.join(reason)} (不符合条件，剔除)")
                            continue
                    
                    # 符合所有条件
                    filtered_stocks.append(stock)
                    print(f"  ✅ {symbol} {stock.get('股票名称', '未知')} 股价: {price:.2f}元 {'筹码良好' if cyq_info else '无筹码数据(保留)'} (符合条件)")
                
                    if excluded_cyq > 0:
                        print(f"  📊 筹码筛选结果: {len(price_filtered_stocks)}只 → {len(filtered_stocks)}只（剔除{excluded_cyq}只）")
                    
                    print(f"📊 最终筛选结果: {len(symbols)}只 → {len(filtered_stocks)}只")
                    if excluded_price > 0:
                        print(f"   • 股价筛选: 剔除{excluded_price}只股票（股价不在5-30元范围内）")
                if excluded_turnover > 0:
                    print(f"   • 换手率筛选: 剔除{excluded_turnover}只股票（换手率<3%）")
                if excluded_cyq > 0:
                    print(f"   • 筹码分布筛选: 剔除{excluded_cyq}只股票（获利盘>=70%或集中度>=0.11）")
            else:
                # 价格和换手率筛选后没有股票，直接返回空列表
                filtered_stocks = []
                print(f"📊 筛选结果: {len(symbols)}只 → 0只（价格和换手率筛选后没有符合条件的股票）")
            
            # 保存到缓存（包括上一交易日收盘价）
            # 只保存筛选后股票的昨日收盘价，避免缓存文件过大
            filtered_previous_close = {}
            if filtered_stocks:
                for stock in filtered_stocks:
                    symbol = stock['代码']
                    if symbol in previous_close_data:
                        filtered_previous_close[symbol] = previous_close_data[symbol]
                    else:
                        print(f"  ⚠️ {symbol} {stock.get('股票名称', '未知')} 在previous_close_data中不存在，无法保存昨日收盘价")
            
            print(f"📊 准备保存到缓存: 筛选后股票 {len(filtered_stocks)}只，昨日收盘价 {len(filtered_previous_close)}只")
            if 'SZ002333' in filtered_previous_close:
                print(f"  ✅ SZ002333 昨日收盘价已包含在缓存中: {filtered_previous_close['SZ002333']:.2f}元")
            elif 'SZ002333' in [s['代码'] for s in filtered_stocks]:
                print(f"  ⚠️ SZ002333 在筛选后股票中，但昨日收盘价未找到")
                if 'SZ002333' in previous_close_data:
                    print(f"  ✅ 但 SZ002333 在 previous_close_data 中: {previous_close_data['SZ002333']:.2f}元")
                else:
                    print(f"  ❌ SZ002333 不在 previous_close_data 中")
            
            self.save_price_cyq_filter_cache(filtered_stocks, filtered_previous_close)
        
        if not filtered_stocks:
            print("❌ 筛选后没有符合条件的股票")
            return []
        
        # 提取筛选后的股票代码列表
        filtered_symbols = [stock['代码'] for stock in filtered_stocks]
        
        # 如果使用缓存，需要将缓存的上一交易日收盘价更新到实例变量
        if not hasattr(self, 'previous_close_prices'):
            self.previous_close_prices = {}
        
        # 如果从缓存加载了上一交易日收盘价，更新到实例变量
        if cached_previous_close is not None:
            self.previous_close_prices.update(cached_previous_close)
        
        # 检查是否需要获取缺失的上一交易日收盘价
        missing_symbols = [symbol for symbol in symbols if symbol not in self.previous_close_prices]
        if missing_symbols:
            print(f"📊 获取{len(missing_symbols)}只股票的上一交易日收盘价（缓存中缺失）...")
            _, _, previous_close_data = self.get_stock_price_batch(missing_symbols)
            self.previous_close_prices.update(previous_close_data)
            # 更新缓存（合并新的数据）
            if cached_previous_close is not None:
                # 合并缓存中的数据和新增的数据
                updated_previous_close = cached_previous_close.copy()
                updated_previous_close.update(previous_close_data)
                self.save_price_cyq_filter_cache(filtered_stocks, updated_previous_close)
            else:
                # 如果之前没有缓存，直接保存
                self.save_price_cyq_filter_cache(filtered_stocks, self.previous_close_prices)
        
        # 步骤1: 使用多线程批量获取tick数据
        print(f"📊 步骤1/4: 批量获取 {len(symbols)} 只自定义股票的Tick数据（10线程）...")
        tick_data_results = self.get_tick_data_batch(symbols, max_workers=10)
        
        # 过滤出有效的tick数据
        valid_stocks = []
        stock_dict = {stock['代码']: stock for stock in filtered_stocks}
        
        for symbol, tick_df in tick_data_results.items():
            if tick_df is not None and not tick_df.empty:
                valid_stocks.append((stock_dict[symbol], tick_df))
            else:
                print(f"  ❌ {symbol} 无有效tick数据，跳过")
        
        if not valid_stocks:
            print("❌ 没有股票有有效的tick数据")
            return []
        
        print(f"✅ 步骤1完成: {len(valid_stocks)}/{len(symbols)} 只股票获取成功")
        
        # 步骤2: 批量分析交易方向和计算得分（10线程并发）
        print(f"📊 步骤2/4: 批量分析交易方向和计算得分（10线程）...")
        analysis_results = {}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self.analyze_stock_worker, stock, tick_df)
                for stock, tick_df in valid_stocks
            ]
            
            for future in futures:
                try:
                    symbol, result = future.result()
                    if result is not None:
                        analysis_results[symbol] = result
                        print(f"  ✅ {symbol} 分析完成，得分: {result['score']:.2f}")
                except Exception as e:
                    print(f"  ⚠️ 分析任务异常: {e}")
        
        print(f"✅ 步骤2完成: {len(analysis_results)} 只股票分析成功")
        
        # 合并所有结果
        print(f"📊 合并结果...")
        for symbol, analysis in analysis_results.items():
            # 存储结果（股价从tick数据中获取，不通过API）
            self.tick_data[symbol] = analysis['tick_df']
            self.trade_directions[symbol] = analysis['trade_direction']
            # 获取上一交易日收盘价（如果已保存）
            previous_close = None
            if hasattr(self, 'previous_close_prices') and symbol in self.previous_close_prices:
                previous_close = self.previous_close_prices[symbol]
            
            self.scores[symbol] = {
                'name': analysis['name'],
                'score': analysis['score'],
                'trade_direction': analysis['trade_direction'],
                'intraday_change': analysis.get('intraday_change', 0.0),
                'previous_close': previous_close  # 保存上一交易日收盘价
            }
        
        # 按得分排序
        sorted_stocks = sorted(self.scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        # 步骤3: 过滤掉日内涨跌幅>6%的股票
        print(f"📊 步骤3/4: 筛选日内涨跌幅（<=6%）...")
        final_filtered_stocks = []
        excluded_count = 0
        for symbol, data in sorted_stocks:
            intraday_change = data.get('intraday_change', 0.0)
            if intraday_change > 6.0:
                excluded_count += 1
                print(f"  ❌ {symbol} {data['name']} 日内涨跌幅:{intraday_change:.2f}% (超过6%，剔除)")
            else:
                final_filtered_stocks.append((symbol, data))
        
        if excluded_count > 0:
            print(f"📊 日内涨跌幅筛选: 剔除{excluded_count}只股票（日内涨跌幅>6%）")
            print(f"📊 筛选后剩余: {len(final_filtered_stocks)}只股票")
        
        sorted_stocks = final_filtered_stocks
        
        # 打印结果
        print(f"\n{'='*60}")
        print(f"🏆 自定义股票分析结果 Top {min(len(sorted_stocks), 50)}:")
        print(f"{'='*60}")
        
        for i, (symbol, data) in enumerate(sorted_stocks[:50], 1):
            trade_dir = data['trade_direction']
            buy_ratio = trade_dir['buy_ratio'] * 100
            sell_ratio = trade_dir['sell_ratio'] * 100
            active_buy_ratio = trade_dir['active_buy_ratio'] * 100
            net_buy_volume = trade_dir['net_buy_volume']
            
            intraday_change = data.get('intraday_change', 0.0)
            print(f"{i}. {symbol} {data['name']}")
            print(f"   • 得分: {data['score']:.2f}")
            print(f"   • 日内涨跌幅: {intraday_change:.2f}%")
            print(f"   • 买盘比例: {buy_ratio:.1f}%")
            print(f"   • 卖盘比例: {sell_ratio:.1f}%")
            print(f"   • 净买入量: {net_buy_volume:,}")
            print(f"   • 主动买入强度: {active_buy_ratio:.1f}%")
            print()
        
        return sorted_stocks
    
    def analyze_stock_worker(self, stock, tick_df):
        """分析单个股票的工作函数（计算交易方向和得分）"""
        symbol = stock['代码']
        name = stock['股票名称']
        
        try:
            # 计算日内涨跌幅（第一条成交价 vs 最后一条成交价）
            intraday_change = 0.0
            if tick_df is not None and not tick_df.empty and len(tick_df) > 0:
                # 确保按时间排序
                sorted_tick_df = tick_df.sort_values('时间')
                first_price = float(sorted_tick_df['成交价'].iloc[0])
                last_price = float(sorted_tick_df['成交价'].iloc[-1])
                if first_price > 0:
                    intraday_change = ((last_price - first_price) / first_price) * 100
                
            # 分析交易方向
            trade_direction = self.analyze_trade_direction(tick_df, symbol)
            
            # 计算得分
            score = self.calculate_score(symbol, tick_df, trade_direction)
            
            return (symbol, {
                'name': name,
                'score': score,
                'trade_direction': trade_direction,
                'tick_df': tick_df,
                'intraday_change': intraday_change
            })
        except Exception as e:
            print(f"  ⚠️ {symbol} 分析异常: {e}")
            return (symbol, None)
    
    def get_realtime_price_change_worker(self, stock):
        """获取单只股票的实时涨跌幅"""
        try:
            import random
            import time
            time.sleep(random.uniform(0.05, 0.15))
            
            symbol = stock['代码']
            realtime_info = self._get_single_stock_realtime_info(symbol)
            
            # 更新实时涨跌幅和股价
            stock['涨跌幅'] = realtime_info.get('涨跌幅', stock.get('涨跌幅', 0.0))
            stock['最新价'] = realtime_info.get('最新价', stock.get('最新价', 0.0))
            
            return stock
        except Exception as e:
            print(f"  ⚠️ {stock['代码']} 获取实时涨跌幅失败: {e}")
            return stock
    
    def get_realtime_price_change_batch(self, stocks):
        """批量获取实时涨跌幅（10线程并发）"""
        if not stocks:
            return stocks
        
        updated_stocks = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_stock = {
                executor.submit(self.get_realtime_price_change_worker, stock): stock
                for stock in stocks
            }
            
            for future in as_completed(future_to_stock):
                try:
                    updated_stock = future.result()
                    updated_stocks.append(updated_stock)
                except Exception as e:
                    stock = future_to_stock[future]
                    print(f"  ❌ {stock['代码']} 处理失败: {e}")
                    updated_stocks.append(stock)
        
        return updated_stocks
    
    def analyze_stocks(self):
        """分析所有股票（热门股票 + 自定义股票）- 批量并发处理"""
        # 获取合并后的股票列表
        all_stocks = self.get_combined_stocks()
        
        if not all_stocks:
            print("❌ 没有股票需要分析")
            return []
        
        # 提取股票代码列表
        symbols = [stock['代码'] for stock in all_stocks]
        
        # 步骤0: 在获取tick数据前，先筛选股价、换手率和筹码分布（每个开市日只执行一次）
        print(f"📊 步骤0/4: 筛选股票（股价5-30元 + 换手率>=3% + 筹码分布）...")
        
        # 先尝试从缓存加载（如果设置了强制刷新，则跳过缓存）
        if self.refresh_filter_cache:
            print("🔄 强制刷新筛选缓存，跳过缓存加载...")
            filtered_stocks = None
            cached_previous_close = None
        else:
            filtered_stocks, cached_previous_close = self.load_price_cyq_filter_cache()
        
        if filtered_stocks is None:
            # 缓存不存在或已过期，从API获取并筛选
            # 步骤1: 先批量获取股票价格和换手率并过滤
            print(f"  📊 步骤1/3: 获取股票价格和换手率并筛选（5-30元，换手率>=3%）...")
            price_data, turnover_data, previous_close_data = self.get_stock_price_batch(symbols)
            
            # 保存上一交易日收盘价到实例变量，供后续使用
            if not hasattr(self, 'previous_close_prices'):
                self.previous_close_prices = {}
            self.previous_close_prices.update(previous_close_data)
            
            # 先过滤价格（5-30元）和换手率（>=3%）
            price_filtered_stocks = []
            excluded_price = 0
            excluded_turnover = 0
            stock_dict = {stock['代码']: stock for stock in all_stocks}
            
            for symbol in symbols:
                stock = stock_dict[symbol]
                price = price_data.get(symbol)
                turnover = turnover_data.get(symbol)
                
                # 检查股价（5-30元）
                if price is None:
                    # 如果无法获取价格，使用股票信息中的最新价
                    if '最新价' in stock:
                        price = float(stock['最新价'])
                    else:
                        excluded_price += 1
                        print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 无法获取价格，跳过")
                        continue
                
                if price < 5.0 or price > 30.0:
                    excluded_price += 1
                    print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 股价: {price:.2f}元 (不在5-30元范围内，剔除)")
                    continue
                
                # 检查换手率（>=3%，即>=0.03）
                if turnover is not None:
                    if turnover < 0.03:  # 换手率小于3%
                        excluded_turnover += 1
                        print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 换手率: {turnover:.2%} (<3%，剔除)")
                        continue
                else:
                    # 无法获取换手率，也剔除（保守策略）
                    excluded_turnover += 1
                    print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 无法获取换手率，跳过")
                    continue
                
                # 价格和换手率都符合条件，保留
                price_filtered_stocks.append(stock)
            
            if excluded_price > 0 or excluded_turnover > 0:
                print(f"  📊 价格和换手率筛选结果: {len(symbols)}只 → {len(price_filtered_stocks)}只（剔除价格{excluded_price}只，换手率{excluded_turnover}只）")
            
            # 步骤2: 对价格和换手率筛选后的股票，获取筹码分布并过滤
            if price_filtered_stocks:
                print(f"  📊 步骤2/3: 获取筹码分布并筛选（获利盘<70%, 集中度<0.11）...")
                price_filtered_symbols = [stock['代码'] for stock in price_filtered_stocks]
                cyq_data = self.get_cyq_data_batch(price_filtered_symbols)
                
                # 筛选符合条件的股票
                filtered_stocks = []
                excluded_cyq = 0
                price_filtered_dict = {stock['代码']: stock for stock in price_filtered_stocks}
                
                for symbol in price_filtered_symbols:
                    stock = price_filtered_dict[symbol]
                    price = price_data.get(symbol)
                    cyq_info = cyq_data.get(symbol)
                    
                # 检查筹码分布
                    if cyq_info is not None:
                        profit_ratio = cyq_info['获利比例']
                        concentration = cyq_info['90集中度']
                        # 筛选条件：获利比例 < 70% 且 90集中度 < 0.11
                        if profit_ratio >= 0.70 or concentration >= 0.11:
                            excluded_cyq += 1
                            reason = []
                            if profit_ratio >= 0.70:
                                reason.append(f"获利盘{profit_ratio:.1%}过高")
                            if concentration >= 0.11:
                                reason.append(f"集中度{concentration:.3f}过大")
                            print(f"  ❌ {symbol} {stock.get('股票名称', '未知')} 筹码分布: {', '.join(reason)} (不符合条件，剔除)")
                            continue
                    
                    # 符合所有条件
                    filtered_stocks.append(stock)
                    print(f"  ✅ {symbol} {stock.get('股票名称', '未知')} 股价: {price:.2f}元 {'筹码良好' if cyq_info else '无筹码数据(保留)'} (符合条件)")
                
                    if excluded_cyq > 0:
                        print(f"  📊 筹码筛选结果: {len(price_filtered_stocks)}只 → {len(filtered_stocks)}只（剔除{excluded_cyq}只）")
                
                    print(f"📊 最终筛选结果: {len(symbols)}只 → {len(filtered_stocks)}只")
                    if excluded_price > 0:
                        print(f"   • 股价筛选: 剔除{excluded_price}只股票（股价不在5-30元范围内）")
                    if excluded_turnover > 0:
                        print(f"   • 换手率筛选: 剔除{excluded_turnover}只股票（换手率<3%）")
                if excluded_cyq > 0:
                    print(f"   • 筹码分布筛选: 剔除{excluded_cyq}只股票（获利盘>=70%或集中度>=0.11）")
            else:
                # 价格和换手率筛选后没有股票，直接返回空列表
                filtered_stocks = []
                print(f"📊 筛选结果: {len(symbols)}只 → 0只（价格和换手率筛选后没有符合条件的股票）")
            
            # 保存到缓存（包括上一交易日收盘价）
            # 只保存筛选后股票的昨日收盘价，避免缓存文件过大
            filtered_previous_close = {}
            if filtered_stocks:
                for stock in filtered_stocks:
                    symbol = stock['代码']
                    if symbol in previous_close_data:
                        filtered_previous_close[symbol] = previous_close_data[symbol]
                    else:
                        print(f"  ⚠️ {symbol} {stock.get('股票名称', '未知')} 在previous_close_data中不存在，无法保存昨日收盘价")
            
            print(f"📊 准备保存到缓存: 筛选后股票 {len(filtered_stocks)}只，昨日收盘价 {len(filtered_previous_close)}只")
            if 'SZ002333' in filtered_previous_close:
                print(f"  ✅ SZ002333 昨日收盘价已包含在缓存中: {filtered_previous_close['SZ002333']:.2f}元")
            elif 'SZ002333' in [s['代码'] for s in filtered_stocks]:
                print(f"  ⚠️ SZ002333 在筛选后股票中，但昨日收盘价未找到")
                if 'SZ002333' in previous_close_data:
                    print(f"  ✅ 但 SZ002333 在 previous_close_data 中: {previous_close_data['SZ002333']:.2f}元")
                else:
                    print(f"  ❌ SZ002333 不在 previous_close_data 中")
            
            self.save_price_cyq_filter_cache(filtered_stocks, filtered_previous_close)
        
        if not filtered_stocks:
            print("❌ 筛选后没有符合条件的股票")
            return []
        
        # 提取筛选后的股票代码列表
        filtered_symbols = [stock['代码'] for stock in filtered_stocks]
        
        # 如果使用缓存，需要将缓存的上一交易日收盘价更新到实例变量
        if not hasattr(self, 'previous_close_prices'):
            self.previous_close_prices = {}
        
        # 如果从缓存加载了上一交易日收盘价，更新到实例变量
        if cached_previous_close is not None:
            self.previous_close_prices.update(cached_previous_close)
            print(f"📊 从缓存更新昨日收盘价到实例变量: {len(cached_previous_close)}只股票")
            if 'SZ002333' in cached_previous_close:
                print(f"  ✅ SZ002333 昨日收盘价已从缓存加载: {cached_previous_close['SZ002333']:.2f}元")
            else:
                print(f"  ⚠️ SZ002333 不在缓存中")
        
        # 检查是否需要获取缺失的上一交易日收盘价
        missing_symbols = [symbol for symbol in filtered_symbols if symbol not in self.previous_close_prices]
        if missing_symbols:
            print(f"📊 获取{len(missing_symbols)}只股票的上一交易日收盘价（缓存中缺失）...")
            _, _, previous_close_data = self.get_stock_price_batch(missing_symbols)
            self.previous_close_prices.update(previous_close_data)
            # 更新缓存（合并新的数据）
            if cached_previous_close is not None:
                # 合并缓存中的数据和新增的数据
                updated_previous_close = cached_previous_close.copy()
                updated_previous_close.update(previous_close_data)
                self.save_price_cyq_filter_cache(filtered_stocks, updated_previous_close)
            else:
                # 如果之前没有缓存，直接保存
                self.save_price_cyq_filter_cache(filtered_stocks, self.previous_close_prices)
        
        # 步骤1: 使用多线程批量获取tick数据
        print(f"📊 步骤1/4: 批量获取 {len(filtered_symbols)} 只股票的Tick数据（10线程）...")
        tick_data_results = self.get_tick_data_batch(filtered_symbols, max_workers=10)
        
        # 过滤出有效的tick数据
        valid_stocks = []
        stock_dict = {stock['代码']: stock for stock in filtered_stocks}
        
        for symbol, tick_df in tick_data_results.items():
            if tick_df is not None and not tick_df.empty:
                valid_stocks.append((stock_dict[symbol], tick_df))
            else:
                print(f"  ❌ {symbol} 无有效tick数据，跳过")
        
        if not valid_stocks:
            print("❌ 没有股票有有效的tick数据")
            return []
        
        print(f"✅ 步骤1完成: {len(valid_stocks)}/{len(filtered_symbols)} 只股票获取成功")
        
        # 步骤2: 批量分析交易方向和计算得分（10线程并发）
        print(f"📊 步骤2/4: 批量分析交易方向和计算得分（10线程）...")
        analysis_results = {}
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(self.analyze_stock_worker, stock, tick_df)
                for stock, tick_df in valid_stocks
            ]
            
            for future in futures:
                try:
                    symbol, result = future.result()
                    if result is not None:
                        analysis_results[symbol] = result
                        print(f"  ✅ {symbol} 分析完成，得分: {result['score']:.2f}")
                except Exception as e:
                    print(f"  ⚠️ 分析任务异常: {e}")
        
        print(f"✅ 步骤2完成: {len(analysis_results)} 只股票分析成功")
        
        # 合并所有结果
        print(f"📊 合并结果...")
        for symbol, analysis in analysis_results.items():
            # 存储结果（股价从tick数据中获取，不通过API）
            self.tick_data[symbol] = analysis['tick_df']
            self.trade_directions[symbol] = analysis['trade_direction']
            # 获取上一交易日收盘价（如果已保存）
            previous_close = None
            if hasattr(self, 'previous_close_prices') and symbol in self.previous_close_prices:
                previous_close = self.previous_close_prices[symbol]
            
            self.scores[symbol] = {
                'name': analysis['name'],
                'score': analysis['score'],
                'trade_direction': analysis['trade_direction'],
                'intraday_change': analysis.get('intraday_change', 0.0),
                'previous_close': previous_close  # 保存上一交易日收盘价
            }
        
        # 按得分排序
        sorted_stocks = sorted(self.scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        print(f"📊 进入最终筛选的股票数量: {len(sorted_stocks)}只")
        
        # 筛选符合条件的股票（主动买入强度<100% 且 日内涨跌幅<=6%）
        filtered_stocks = []
        excluded_active_buy = 0
        excluded_intraday_change = 0
        for symbol, data in sorted_stocks:
            active_buy_ratio = data['trade_direction']['active_buy_ratio']
            intraday_change = data.get('intraday_change', 0.0)
            
            # 检查主动买入强度
            if active_buy_ratio >= 100:
                excluded_active_buy += 1
                print(f"  ❌ {symbol} {data['name']} 主动买入强度: {active_buy_ratio:.1f}% (不符合条件，剔除)")
                continue
            
            # 检查日内涨跌幅
            if intraday_change > 6.0:
                excluded_intraday_change += 1
                print(f"  ❌ {symbol} {data['name']} 日内涨跌幅: {intraday_change:.2f}% (超过6%，剔除)")
                continue
            
            # 符合所有条件
            filtered_stocks.append((symbol, data))
            print(f"  ✅ {symbol} {data['name']} 主动买入强度: {active_buy_ratio:.1f}%, 日内涨跌幅: {intraday_change:.2f}% (符合条件)")
        
        print(f"📊 股票筛选结果:")
        print(f"   • 进入筛选: {len(sorted_stocks)}只股票")
        if excluded_active_buy > 0:
            print(f"   • 主动买入强度筛选: 剔除{excluded_active_buy}只股票（主动买入强度>=100%）")
        if excluded_intraday_change > 0:
            print(f"   • 日内涨跌幅筛选: 剔除{excluded_intraday_change}只股票（日内涨跌幅>6%）")
        print(f"   • 筛选后剩余: {len(filtered_stocks)}只股票")
        
        if len(filtered_stocks) == 0 and len(sorted_stocks) > 0:
            print(f"\n⚠️ 所有股票都被筛选掉了！")
            print(f"   前5只股票的详细信息:")
            for i, (symbol, data) in enumerate(sorted_stocks[:5], 1):
                active_buy_ratio = data['trade_direction']['active_buy_ratio']
                intraday_change = data.get('intraday_change', 0.0)
                print(f"   {i}. {symbol} {data['name']}: 主动买入强度={active_buy_ratio:.1f}%, 日内涨跌幅={intraday_change:.2f}%")
        
        return filtered_stocks  # 返回所有符合条件的股票
    
    def analyze_stocks_no_filter(self):
        """不筛选，直接获取所有股票的tick数据并排名"""
        # 获取合并后的股票列表
        all_stocks = self.get_combined_stocks()
        
        if not all_stocks:
            print("❌ 没有股票需要分析")
            return []
        
        # 提取股票代码列表
        symbols = [stock['代码'] for stock in all_stocks]
        print(f"📊 直接获取 {len(symbols)} 只股票的tick数据（跳过筛选）...")
        
        # 步骤1: 批量获取tick数据（10线程并发）
        print(f"📊 步骤1/2: 批量获取tick数据（10线程）...")
        tick_data_results = self.get_tick_data_batch(symbols, max_workers=10)
        
        # 过滤出有效的tick数据
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
        
        # 步骤2: 批量分析交易方向和计算得分（10线程并发）
        print(f"📊 步骤2/2: 批量分析交易方向和计算得分（10线程）...")
        analysis_results = {}
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(self.analyze_stock_worker, stock, tick_df)
                for stock, tick_df in valid_stocks
            ]
            
            for future in futures:
                try:
                    symbol, result = future.result()
                    if result is not None:
                        analysis_results[symbol] = result
                        print(f"  ✅ {symbol} 分析完成，得分: {result['score']:.2f}")
                except Exception as e:
                    print(f"  ⚠️ 分析任务异常: {e}")
        
        print(f"✅ 步骤2完成: {len(analysis_results)} 只股票分析成功")
        
        # 合并所有结果
        print(f"📊 合并结果...")
        for symbol, analysis in analysis_results.items():
            # 存储结果（股价从tick数据中获取，不通过API）
            self.tick_data[symbol] = analysis['tick_df']
            self.trade_directions[symbol] = analysis['trade_direction']
            # 获取上一交易日收盘价（如果已保存）
            previous_close = None
            if hasattr(self, 'previous_close_prices') and symbol in self.previous_close_prices:
                previous_close = self.previous_close_prices[symbol]
            
            self.scores[symbol] = {
                'name': analysis['name'],
                'score': analysis['score'],
                'trade_direction': analysis['trade_direction'],
                'intraday_change': analysis.get('intraday_change', 0.0),
                'previous_close': previous_close  # 保存上一交易日收盘价
            }
        
        # 按得分排序（不进行筛选，返回所有股票）
        sorted_stocks = sorted(self.scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        print(f"📊 分析完成，共 {len(sorted_stocks)} 只股票")
        
        # 返回所有股票（按得分排序）
        return [(symbol, data) for symbol, data in sorted_stocks]
    
    def send_dingtalk_message(self, top_stocks):
        """发送钉钉消息"""
        webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=ae055118615b242c6fe43fc3273a228f316209f707d07e7ce39fc83f4270ed82"
        secret = "SECf2b2861525388e240846ad1e2beb3b93d3b5f0d2e6634e43176b593f050e77da"
        
        # 记录总股票数和实际发送数
        total_stocks_count = len(top_stocks)
        
        # 在发送前再次过滤：剔除日内涨跌幅>6%的股票
        filtered_stocks = []
        excluded_count = 0
        for symbol, data in top_stocks:
            intraday_change = data.get('intraday_change', 0.0)
            if intraday_change > 6.0:
                excluded_count += 1
                print(f"  ⚠️ {symbol} {data['name']} 日内涨跌幅:{intraday_change:.2f}% (超过6%，不发送)")
            else:
                filtered_stocks.append((symbol, data))
        
        if excluded_count > 0:
            print(f"📊 发送前筛选: 剔除{excluded_count}只股票（日内涨跌幅>6%）")
        
        stocks_to_send = filtered_stocks[:50]
        send_count = len(stocks_to_send)
        
        if not stocks_to_send:
            print("⚠️ 没有股票可发送，不发送钉钉消息")
            return False
        
        print(f"📤 准备发送钉钉消息: 符合条件{total_stocks_count}只，筛选后{len(filtered_stocks)}只，发送前{send_count}只")
        
        # 构建消息内容
        message = {
            "msgtype": "markdown",
            "markdown": {
                "title": "量化分析报告",
                "text": f"""# 📈 量化分析报告 - {datetime.now().strftime('%Y-%m-%d %H:%M')}

## 📊 筛选结果
- **符合条件**: {total_stocks_count}只股票
- **本次发送**: 前{send_count}只（按得分排序）

## 🏆 股票评分排序

"""
            }
        }
        
        for i, (symbol, data) in enumerate(stocks_to_send, 1):
            trade_direction = data['trade_direction']
            
            # 从tick数据中获取最新成交价（当前价格）
            stock_price = "N/A"
            current_price = None
            if symbol in self.tick_data:
                tick_df = self.tick_data[symbol]
                if tick_df is not None and not tick_df.empty and len(tick_df) > 0:
                    # 确保按时间排序，获取最后一条成交价
                    sorted_tick_df = tick_df.sort_values('时间')
                    current_price = float(sorted_tick_df['成交价'].iloc[-1])
                    stock_price = f"{current_price:.2f}元"
            
            # 计算实时涨跌幅：（当前价格 - 上一交易日收盘价）÷ 上一交易日收盘价 × 100%
            realtime_change = "N/A"
            previous_close = data.get('previous_close')
            # 如果data中没有previous_close，尝试从实例变量中获取
            if previous_close is None and hasattr(self, 'previous_close_prices') and symbol in self.previous_close_prices:
                previous_close = self.previous_close_prices[symbol]
            
            # 调试信息：如果实时涨跌幅为0，打印详细信息
            if symbol == 'SZ002333':
                print(f"\n🔍 调试 SZ002333 实时涨跌幅计算:")
                print(f"  - 当前价格 (current_price): {current_price}")
                print(f"  - 昨日收盘价 (previous_close): {previous_close}")
                print(f"  - data.get('previous_close'): {data.get('previous_close')}")
                print(f"  - self.previous_close_prices.get('{symbol}'): {self.previous_close_prices.get(symbol) if hasattr(self, 'previous_close_prices') else 'N/A'}")
                print(f"  - hasattr(self, 'previous_close_prices'): {hasattr(self, 'previous_close_prices')}")
                if hasattr(self, 'previous_close_prices'):
                    print(f"  - previous_close_prices 包含的股票数: {len(self.previous_close_prices)}")
                    if symbol in self.previous_close_prices:
                        print(f"  - previous_close_prices['{symbol}'] = {self.previous_close_prices[symbol]}")
            
            if current_price is not None and previous_close is not None and previous_close > 0:
                realtime_change_value = ((current_price - previous_close) / previous_close) * 100
                realtime_change = f"{realtime_change_value:.2f}%"
            elif symbol == 'SZ002333':
                print(f"  ⚠️ 实时涨跌幅计算失败: current_price={current_price}, previous_close={previous_close}")
            
            intraday_change = data.get('intraday_change', 0.0)
            message["markdown"]["text"] += f"""
### {i}. {symbol} {data['name']}
- **得分**: {data['score']:.2f}
- **股价**: {stock_price}
- **实时涨跌幅**: {realtime_change}
- **日内涨跌幅**: {intraday_change:.2f}%
- **买盘比例**: {trade_direction['buy_ratio']:.1%}
- **卖盘比例**: {trade_direction['sell_ratio']:.1%}
- **净买入量**: {trade_direction['net_buy_volume']:,.0f}
- **主动买入强度**: {trade_direction['active_buy_ratio']:.1%}

"""
        
        message["markdown"]["text"] += f"""
## 📊 分析说明
- 红色三角：买盘交易
- 绿色倒三角：卖盘交易  
- 灰色圆点：中性盘交易
- 得分综合考虑价格动量、成交量、主动买卖强度等因素

---
*数据来源：AKShare | 分析时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        # 生成签名
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{secret}"
        hmac_code = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
        sign = base64.b64encode(hmac_code).decode('utf-8')
        
        # 构建完整的webhook URL
        full_webhook_url = f"{webhook_url}&timestamp={timestamp}&sign={sign}"
        
        try:
            response = requests.post(full_webhook_url, json=message)
            if response.status_code == 200:
                print("钉钉消息发送成功！")
                return True
            else:
                print(f"钉钉消息发送失败: {response.text}")
                return False
        except Exception as e:
            print(f"发送钉钉消息时出错: {e}")
            return False
    
    def analyze_single_stock(self, symbol):
        """分析单个股票"""
        # 确保股票代码格式正确
        if not symbol.startswith(('SH', 'SZ')):
            if symbol.startswith('6'):
                symbol = f'SH{symbol}'
            elif symbol.startswith('0') or symbol.startswith('3'):
                symbol = f'SZ{symbol}'
        
        # 获取股票名称和实时信息
        pure_code = symbol[2:]  # 去掉SH/SZ前缀
        stock_name = self._get_stock_name_by_code(pure_code)
        
        # 如果获取到的名称是默认值，尝试强制从API获取（尝试多个接口）
        if stock_name.startswith('股票') or stock_name == f'股票{pure_code}':
            print(f"  🔍 尝试从API获取股票名称: {pure_code}")
            api_success = False
            
            # 尝试方法1: ak.stock_individual_info_em() (东财接口，最可靠)
            if not api_success:
                try:
                    info_df = ak.stock_individual_info_em(symbol=pure_code)
                    if info_df is not None and not info_df.empty:
                        # 查找'股票简称'这一行
                        name_row = info_df[info_df['item'] == '股票简称']
                        if not name_row.empty:
                            new_name = name_row['value'].iloc[0]
                            if new_name and not new_name.startswith('股票') and pd.notna(new_name):
                                stock_name = str(new_name).strip()
                                print(f"  ✅ 从东财API获取股票名称成功: {stock_name}")
                                # 更新到运行时缓存
                                if not hasattr(self, '_stock_names_cache'):
                                    self._stock_names_cache = {}
                                self._stock_names_cache[pure_code] = stock_name
                                api_success = True
                except Exception as e:
                    print(f"  ⚠️ 东财API获取失败: {e}")
            
            # 尝试方法2: ak.stock_zh_a_spot() (新浪接口)
            if not api_success:
                try:
                    spot_df = ak.stock_zh_a_spot()
                    if spot_df is not None and not spot_df.empty:
                        stock_row = spot_df[spot_df['代码'] == pure_code]
                        if not stock_row.empty and '名称' in stock_row.columns:
                            new_name = stock_row['名称'].iloc[0]
                            if new_name and not new_name.startswith('股票'):
                                stock_name = new_name
                                print(f"  ✅ 从新浪API获取股票名称成功: {stock_name}")
                                # 更新到运行时缓存
                                if not hasattr(self, '_stock_names_cache'):
                                    self._stock_names_cache = {}
                                self._stock_names_cache[pure_code] = stock_name
                                api_success = True
                except Exception as e:
                    print(f"  ⚠️ 新浪API获取失败: {e}")
            
            # 如果所有API都失败，提供手动添加的提示
            if not api_success:
                print(f"  ⚠️ 所有API接口都无法获取股票名称（可能被IP限制）")
                print(f"  💡 提示: 可以手动将股票代码 {pure_code} 添加到映射表中")
                print(f"  💡 或者等待IP限制解除后重新执行 --code 命令")
        
        stock_info = self._get_single_stock_realtime_info(symbol)
        
        print(f"\n{'='*50}")
        print(f"开始分析股票: {symbol} ({stock_name})")
        print(f"{'='*50}")
        
        # 1. 获取tick数据
        tick_df = self.get_tick_data(symbol)
        if tick_df is None:
            print(f"❌ 无法获取股票 {symbol} 的tick数据")
            return None
        
        # 2. 分析交易方向
        trade_direction = self.analyze_trade_direction(tick_df, symbol)
        
        # 3. 计算得分
        score = self.calculate_score(symbol, tick_df, trade_direction)
        
        # 计算日内涨跌幅（第一条成交价 vs 最后一条成交价）
        intraday_change = 0.0
        if tick_df is not None and not tick_df.empty and len(tick_df) > 0:
            sorted_tick_df = tick_df.sort_values('时间')
            first_price = float(sorted_tick_df['成交价'].iloc[0])
            last_price = float(sorted_tick_df['成交价'].iloc[-1])
            if first_price > 0:
                intraday_change = ((last_price - first_price) / first_price) * 100
        
        # 4. 存储结果
        self.tick_data[symbol] = tick_df
        self.trade_directions[symbol] = trade_direction
        # 获取上一交易日收盘价（如果已保存）
        previous_close = None
        if hasattr(self, 'previous_close_prices') and symbol in self.previous_close_prices:
            previous_close = self.previous_close_prices[symbol]
        
        self.scores[symbol] = {
            'name': stock_name,
            'score': score,
            'trade_direction': trade_direction,
            'intraday_change': intraday_change,
            'previous_close': previous_close  # 保存上一交易日收盘价
        }
        
        # 5. 将股票添加到自定义股票池（使用实时信息）
        # 如果名称还是默认值，给出警告
        if stock_name.startswith('股票') or stock_name == f'股票{pure_code}':
            print(f"\n⚠️  警告: 股票 {symbol} 的名称为默认值 '{stock_name}'，已保存到自定义股票池")
            print(f"⚠️  原因: API接口无法访问（可能被IP限制）")
            print(f"💡  解决方案:")
            print(f"   1. 等待IP限制解除后重新执行: python3 start_analysis.py --code {pure_code}")
            print(f"   2. 手动编辑文件 'quant_analysis copy.py'，在 _get_stock_name_by_code() 函数的 stock_names 字典中添加:")
            print(f"      '{pure_code}': '股票名称',")
            print(f"   3. 或者使用其他网络环境（如手机热点）重新执行 --code 命令")
        
        self.add_custom_stock(symbol, stock_name, score, trade_direction, stock_info)
        
        # 6. 输出分析结果
        print(f"\n📊 分析结果:")
        print(f"   得分: {score:.2f}")
        print(f"   日内涨跌幅: {intraday_change:.2f}%")
        print(f"   买盘比例: {trade_direction['buy_ratio']:.1%}")
        print(f"   卖盘比例: {trade_direction['sell_ratio']:.1%}")
        print(f"   净买入量: {trade_direction['net_buy_volume']:,.0f}")
        print(f"   主动买入强度: {trade_direction['active_buy_ratio']:.1%}")
        print(f"   主动卖出强度: {trade_direction['active_sell_ratio']:.1%}")
        print(f"   总交易次数: {trade_direction['total_trades']}")
        
        return {
            'symbol': symbol,
            'name': stock_name,
            'score': score,
            'trade_direction': trade_direction
        }

    def run_analysis(self, custom_only=False, no_filter=False):
        """运行完整分析流程"""
        # 如果只分析自定义股票，直接使用自定义股票列表
        if custom_only:
            print("📋 只分析自定义股票模式")
            custom_stocks = self.load_custom_stocks()
            if not custom_stocks:
                print("❌ 自定义股票池为空，请先使用 --code 添加股票")
            return
            print(f"⭐ 自定义股票: {len(custom_stocks)}只")
            for stock in custom_stocks:
                print(f"  {stock['代码']} {stock['股票名称']}")
        
            # 直接分析自定义股票，不调用get_combined_stocks
            top_stocks = self.analyze_custom_stocks_only(custom_stocks)
        elif no_filter:
            # 不筛选，直接获取所有股票的tick数据并排名
            print("📋 直接获取tick数据模式（跳过筛选）")
            top_stocks = self.analyze_stocks_no_filter()
        else:
            # 1. 分析股票（内部会自动获取股票列表）
            top_stocks = self.analyze_stocks()
        
        if not top_stocks:
            print("❌ 没有符合条件的股票")
            return
        
        # 4. 发送钉钉消息
        self.send_dingtalk_message(top_stocks)

def main():
    """主函数，支持命令行参数"""
    parser = argparse.ArgumentParser(description='量化分析系统')
    parser.add_argument('--code', '-c', type=str, help='分析指定股票代码 (例如: --code 000001)')
    parser.add_argument('--refresh', '-r', action='store_true', help='强制刷新热门股票缓存（重新调用API获取）')
    parser.add_argument('--refresh-filter', action='store_true', help='强制刷新股价和筹码筛选缓存（重新调用API获取）')
    parser.add_argument('--custom-only', action='store_true', help='只分析自定义股票，不分析热门股票')
    parser.add_argument('--no-filter', action='store_true', help='跳过筛选，直接获取所有股票的tick数据并排名')
    parser.add_argument('--source', '-s', type=str, choices=['ljqs', 'zt', 'hot_rank'], default='hot_rank', 
                       help='热门股票源: hot_rank=热门排行榜(默认), zt=昨日涨停池, ljqs=量价齐升')
    
    args = parser.parse_args()
    
    analyzer = QuantAnalysis()
    
    # 保存股票源选择
    analyzer.stock_source = args.source
    
    # 如果需要强制刷新缓存，删除缓存文件
    if args.refresh:
        import os
        if os.path.exists(analyzer.hot_stocks_cache_file):
            os.remove(analyzer.hot_stocks_cache_file)
            print("🔄 已删除热门股票缓存，将重新获取...")
    
    # 如果需要强制刷新筛选缓存，删除筛选缓存文件
    if args.refresh_filter:
        import os
        if os.path.exists(analyzer.price_cyq_filter_cache_file):
            os.remove(analyzer.price_cyq_filter_cache_file)
            print("🔄 已删除股价和筹码筛选缓存，将重新获取...")
        
        # 如果数据源是 hot_rank，也删除热门排行榜缓存
        if hasattr(analyzer, 'stock_source') and analyzer.stock_source == 'hot_rank':
            if os.path.exists(analyzer.hot_stocks_cache_file):
                try:
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
    
    if args.code:
        # 分析单个股票
        result = analyzer.analyze_single_stock(args.code)
        if result:
            print(f"\n🎯 分析完成！股票 {result['symbol']} ({result['name']}) 得分: {result['score']:.2f}")
        else:
            print(f"❌ 分析失败")
    else:
        # 默认分析股票
        if args.custom_only:
            print("🔍 量化分析系统 - 只分析自定义股票")
            analyzer.run_analysis(custom_only=True)
        elif args.no_filter:
            print("🔍 量化分析系统 - 直接获取tick数据模式（跳过筛选）")
            analyzer.run_analysis(no_filter=True)
        else:
            print("🔍 量化分析系统 - 分析热门股票 + 自定义股票")
            analyzer.run_analysis(custom_only=False)

if __name__ == "__main__":
    main()