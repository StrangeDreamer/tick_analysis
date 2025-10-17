#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tick数据采集器
每天收盘后自动采集符合条件的股票tick数据并保存到CSV文件
"""

import akshare as ak
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional
import warnings
warnings.filterwarnings('ignore')

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TickDataCollector:
    def __init__(self, data_dir: str = "tick_data"):
        """
        初始化Tick数据采集器
        
        Args:
            data_dir: 数据存储目录
        """
        self.data_dir = data_dir
        self.ensure_data_dir()
        
    def ensure_data_dir(self):
        """确保数据目录存在"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            logger.info(f"创建数据目录: {self.data_dir}")
    
    def get_qualified_stocks(self, max_retries: int = 3) -> List[Dict]:
        """
        获取符合条件的股票列表
        条件：沪深主板非ST股票，股价在5-30元之间
        
        Args:
            max_retries: 最大重试次数
        
        Returns:
            符合条件的股票列表
        """
        logger.info("开始获取符合条件的股票列表...")
        
        for attempt in range(max_retries):
            try:
                logger.info(f"尝试获取股票数据 (第{attempt + 1}次)...")
                
                # 分别获取沪市和深市股票数据
                sh_data = None
                sz_data = None
                
                try:
                    logger.info("获取沪市A股数据...")
                    sh_data = ak.stock_sh_a_spot_em()
                    if sh_data is not None and not sh_data.empty:
                        logger.info(f"沪市数据获取成功: {len(sh_data)} 只股票")
                    else:
                        logger.warning("沪市数据为空")
                        sh_data = None
                except Exception as e:
                    logger.warning(f"获取沪市数据失败: {e}")
                    sh_data = None
                
                try:
                    logger.info("获取深市A股数据...")
                    sz_data = ak.stock_sz_a_spot_em()
                    if sz_data is not None and not sz_data.empty:
                        logger.info(f"深市数据获取成功: {len(sz_data)} 只股票")
                    else:
                        logger.warning("深市数据为空")
                        sz_data = None
                except Exception as e:
                    logger.warning(f"获取深市数据失败: {e}")
                    sz_data = None
                
                # 合并数据
                if sh_data is not None and not sh_data.empty and sz_data is not None and not sz_data.empty:
                    stock_data = pd.concat([sh_data, sz_data], ignore_index=True)
                    logger.info(f"合并后总数据: {len(stock_data)} 只股票")
                elif sh_data is not None and not sh_data.empty:
                    stock_data = sh_data
                    logger.info(f"仅使用沪市数据: {len(stock_data)} 只股票")
                elif sz_data is not None and not sz_data.empty:
                    stock_data = sz_data
                    logger.info(f"仅使用深市数据: {len(stock_data)} 只股票")
                else:
                    stock_data = None
                
                if stock_data is None or stock_data.empty:
                    logger.warning(f"第{attempt + 1}次获取股票数据失败，数据为空")
                    if attempt < max_retries - 1:
                        time.sleep(5)  # 等待5秒后重试
                        continue
                    else:
                        logger.error("获取股票数据最终失败")
                        return []
            
                # 筛选条件
                # 1. 沪深主板（排除创业板300、科创板688、北交所8）
                # 2. 非ST股票
                # 3. 股价在5-30元之间
                # 4. 有成交量的股票
                
                filtered_stocks = stock_data[
                    (stock_data['代码'].str.startswith(('000', '001', '002', '600', '601', '603', '605'))) &  # 沪深主板
                    (~stock_data['名称'].str.contains('ST', na=False)) &  # 非ST
                    (stock_data['最新价'] >= 5) &  # 股价 >= 5元
                    (stock_data['最新价'] <= 30) &  # 股价 <= 30元
                    (stock_data['成交量'] > 0) &  # 有成交量
                    (stock_data['最新价'] > 0)  # 价格有效
                ].copy()
                
                # 转换为字典列表
                stocks_list = []
                for _, row in filtered_stocks.iterrows():
                    stocks_list.append({
                        'symbol': row['代码'],
                        'name': row['名称'],
                        'price': row['最新价'],
                        'volume': row['成交量'],
                        'amount': row['成交额']
                    })
                
                logger.info(f"找到 {len(stocks_list)} 只符合条件的股票")
                return stocks_list
                
            except Exception as e:
                logger.warning(f"第{attempt + 1}次获取股票列表失败: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"等待10秒后重试...")
                    time.sleep(10)  # 等待10秒后重试
                else:
                    logger.error(f"获取股票列表最终失败: {e}")
                    return []
        
        return []
    
    def get_fallback_stocks(self) -> List[Dict]:
        """
        获取备用股票列表（当网络不稳定时使用）
        使用stock_hot_rank_em接口获取热门股票排名
        
        Returns:
            备用股票列表
        """
        logger.info("使用备用股票列表...")
        
        try:
            # 尝试使用热门股票排名接口（带重试）
            hot_rank_data = None
            for attempt in range(3):
                try:
                    logger.info(f"尝试获取热门股票排名 (第{attempt + 1}次)...")
                    hot_rank_data = ak.stock_hot_rank_em()
                    if hot_rank_data is not None and not hot_rank_data.empty:
                        break
                    else:
                        logger.warning(f"第{attempt + 1}次获取热门排名数据为空")
                        if attempt < 2:
                            time.sleep(3)  # 等待3秒后重试
                except Exception as e:
                    logger.warning(f"第{attempt + 1}次获取热门排名失败: {e}")
                    if attempt < 2:
                        time.sleep(3)  # 等待3秒后重试
                    else:
                        raise e
            
            if hot_rank_data is not None and not hot_rank_data.empty:
                logger.info(f"成功获取热门股票排名: {len(hot_rank_data)} 只股票")
                
                # 转换数据格式
                fallback_stocks = []
                for _, row in hot_rank_data.iterrows():
                    try:
                        # 提取股票代码和名称
                        symbol_raw = str(row.get('代码', '')).strip()
                        name = str(row.get('股票名称', '')).strip()
                        
                        # 处理股票代码格式：SH601212 -> 601212, SZ002185 -> 002185
                        if symbol_raw.startswith('SH'):
                            symbol = symbol_raw[2:]  # 去掉SH前缀
                        elif symbol_raw.startswith('SZ'):
                            symbol = symbol_raw[2:]  # 去掉SZ前缀
                        else:
                            symbol = symbol_raw
                        
                        # 过滤掉创业板（300开头）和科创板（688开头）股票
                        if symbol.startswith(('300', '688')):
                            continue
                        
                        # 获取价格信息
                        price = 0
                        if '最新价' in row and pd.notna(row['最新价']):
                            price = float(row['最新价'])
                        
                        # 由于热门排名数据没有成交量信息，使用默认值
                        volume = 1000000  # 默认成交量
                        amount = price * volume if price > 0 else 0
                        
                        if symbol and name and price > 0:
                            fallback_stocks.append({
                                'symbol': symbol,
                                'name': name,
                                'price': price,
                                'volume': volume,
                                'amount': amount
                            })
                    except Exception as e:
                        logger.warning(f"处理热门股票数据时出错: {e}")
                        continue
                
                if fallback_stocks:
                    logger.info(f"从热门排名获取到 {len(fallback_stocks)} 只股票")
                else:
                    logger.warning("热门排名数据解析失败，使用预设股票列表")
                    fallback_stocks = self._get_preset_stocks()
            else:
                logger.warning("热门股票排名数据为空，使用预设股票列表")
                fallback_stocks = self._get_preset_stocks()
                
        except Exception as e:
            logger.warning(f"获取热门股票排名失败: {e}，使用预设股票列表")
            fallback_stocks = self._get_preset_stocks()
        
        # 筛选价格在5-30元之间的股票
        qualified_stocks = [stock for stock in fallback_stocks if 5 <= stock['price'] <= 30]
        
        logger.info(f"备用股票列表中找到 {len(qualified_stocks)} 只符合条件的股票")
        return qualified_stocks
    
    def _get_preset_stocks(self) -> List[Dict]:
        """
        获取预设的股票列表（最后的备用方案）
        
        Returns:
            预设股票列表
        """
        logger.info("使用预设股票列表...")
        
        # 预设一些常见的沪深主板股票
        preset_stocks = [
            {'symbol': '000001', 'name': '平安银行', 'price': 11.5, 'volume': 1000000, 'amount': 11500000},
            {'symbol': '000002', 'name': '万科A', 'price': 8.5, 'volume': 2000000, 'amount': 17000000},
            {'symbol': '600000', 'name': '浦发银行', 'price': 7.2, 'volume': 1500000, 'amount': 10800000},
            {'symbol': '600036', 'name': '招商银行', 'price': 35.8, 'volume': 800000, 'amount': 28640000},
            {'symbol': '600519', 'name': '贵州茅台', 'price': 1800.0, 'volume': 50000, 'amount': 90000000},
            {'symbol': '000858', 'name': '五粮液', 'price': 120.5, 'volume': 300000, 'amount': 36150000},
            {'symbol': '002415', 'name': '海康威视', 'price': 25.8, 'volume': 600000, 'amount': 15480000},
            {'symbol': '600276', 'name': '恒瑞医药', 'price': 28.5, 'volume': 400000, 'amount': 11400000},
            {'symbol': '000063', 'name': '中兴通讯', 'price': 22.3, 'volume': 500000, 'amount': 11150000},
            {'symbol': '600887', 'name': '伊利股份', 'price': 18.9, 'volume': 700000, 'amount': 13230000}
        ]
        
        return preset_stocks
    
    def get_tick_data(self, symbol: str, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """
        获取单只股票的tick数据
        
        Args:
            symbol: 股票代码
            max_retries: 最大重试次数
            
        Returns:
            tick数据DataFrame或None
        """
        for attempt in range(max_retries):
            try:
                # 转换股票代码格式
                if symbol.startswith(('000', '001', '002', '300')):
                    # 深市主板和创业板
                    tick_symbol = f'sz{symbol}'
                elif symbol.startswith(('600', '601', '603', '605', '688')):
                    # 沪市主板和科创板
                    tick_symbol = f'sh{symbol}'
                else:
                    logger.warning(f"不支持的股票代码格式: {symbol}")
                    return None
                
                # 获取tick数据
                tick_df = ak.stock_zh_a_tick_tx_js(symbol=tick_symbol)
                
                if tick_df is not None and not tick_df.empty:
                    # 添加股票代码和采集时间
                    tick_df['股票代码'] = symbol
                    tick_df['采集时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 重命名列以保持一致性
                    if '成交时间' in tick_df.columns:
                        tick_df = tick_df.rename(columns={
                            '成交时间': '时间',
                            '成交价格': '成交价',
                            '成交量': '成交量',
                            '成交金额': '成交额',
                            '性质': '买卖盘性质'
                        })
                    
                    logger.info(f"成功获取 {symbol} 的tick数据: {len(tick_df)} 条")
                    return tick_df
                else:
                    logger.warning(f"{symbol} tick数据为空")
                    return None
                    
            except Exception as e:
                logger.warning(f"获取 {symbol} tick数据失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)  # 等待2秒后重试
                else:
                    logger.error(f"获取 {symbol} tick数据最终失败")
                    return None
        
        return None
    
    def save_tick_data(self, symbol: str, tick_df: pd.DataFrame, date: str = None):
        """
        保存tick数据到CSV文件
        
        Args:
            symbol: 股票代码
            tick_df: tick数据
            date: 日期（默认为今天）
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        # 创建文件名
        filename = f"{symbol}_{date}_tick.csv"
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            # 保存到CSV
            tick_df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存 {symbol} tick数据到: {filepath}")
            
            # 记录文件信息
            info_file = os.path.join(self.data_dir, f"{date}_collection_info.txt")
            with open(info_file, 'a', encoding='utf-8') as f:
                f.write(f"{symbol}: {len(tick_df)} 条数据, 文件: {filename}\n")
                
        except Exception as e:
            logger.error(f"保存 {symbol} tick数据失败: {e}")
    
    def collect_daily_tick_data(self, date: str = None):
        """
        采集当天的tick数据
        
        Args:
            date: 日期（默认为今天）
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        logger.info(f"开始采集 {date} 的tick数据...")
        
        # 获取符合条件的股票
        stocks = self.get_qualified_stocks()
        if not stocks:
            logger.warning("无法获取实时股票列表，使用备用股票列表...")
            stocks = self.get_fallback_stocks()
            if not stocks:
                logger.error("没有找到符合条件的股票")
                return
        
        # 统计信息
        success_count = 0
        failed_count = 0
        total_tick_count = 0
        
        # 创建采集信息文件
        info_file = os.path.join(self.data_dir, f"{date}_collection_info.txt")
        with open(info_file, 'w', encoding='utf-8') as f:
            f.write(f"Tick数据采集报告 - {date}\n")
            f.write(f"采集时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"目标股票数量: {len(stocks)}\n")
            f.write("=" * 50 + "\n")
        
        # 逐个采集股票tick数据
        for i, stock in enumerate(stocks, 1):
            symbol = stock['symbol']
            name = stock['name']
            price = stock['price']
            
            logger.info(f"正在采集 {i}/{len(stocks)}: {symbol} {name} (价格: {price}元)")
            
            # 获取tick数据
            tick_df = self.get_tick_data(symbol)
            
            if tick_df is not None and not tick_df.empty:
                # 保存数据
                self.save_tick_data(symbol, tick_df, date)
                success_count += 1
                total_tick_count += len(tick_df)
                logger.info(f"✅ {symbol} 采集成功: {len(tick_df)} 条数据")
            else:
                failed_count += 1
                logger.error(f"❌ {symbol} 采集失败")
            
            # 添加延迟避免请求过于频繁
            time.sleep(1)
        
        # 更新采集信息
        with open(info_file, 'a', encoding='utf-8') as f:
            f.write(f"\n采集完成统计:\n")
            f.write(f"成功采集: {success_count} 只股票\n")
            f.write(f"采集失败: {failed_count} 只股票\n")
            f.write(f"总tick数据量: {total_tick_count} 条\n")
            f.write(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        logger.info(f"采集完成! 成功: {success_count}, 失败: {failed_count}, 总数据量: {total_tick_count}")
    
    def get_collection_summary(self, date: str = None) -> Dict:
        """
        获取采集摘要信息
        
        Args:
            date: 日期
            
        Returns:
            摘要信息字典
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        info_file = os.path.join(self.data_dir, f"{date}_collection_info.txt")
        
        if not os.path.exists(info_file):
            return {"error": "采集信息文件不存在"}
        
        try:
            with open(info_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 解析信息
            lines = content.split('\n')
            summary = {}
            
            for line in lines:
                if '目标股票数量:' in line:
                    summary['target_count'] = int(line.split(':')[1].strip())
                elif '成功采集:' in line:
                    summary['success_count'] = int(line.split(':')[1].strip().split()[0])
                elif '采集失败:' in line:
                    summary['failed_count'] = int(line.split(':')[1].strip().split()[0])
                elif '总tick数据量:' in line:
                    summary['total_tick_count'] = int(line.split(':')[1].strip().split()[0])
            
            return summary
            
        except Exception as e:
            return {"error": f"解析摘要信息失败: {e}"}

def main():
    """主函数"""
    logger.info("启动Tick数据采集器...")
    
    # 创建采集器实例
    collector = TickDataCollector()
    
    # 检查是否在交易时间
    now = datetime.now()
    current_time = now.time()
    
    # 交易时间检查（简单版本）
    is_trading_time = (
        (current_time >= datetime.strptime("09:30", "%H:%M").time() and 
         current_time <= datetime.strptime("11:30", "%H:%M").time()) or
        (current_time >= datetime.strptime("13:00", "%H:%M").time() and 
         current_time <= datetime.strptime("15:00", "%H:%M").time())
    )
    
    if is_trading_time:
        logger.warning("当前仍在交易时间内，建议在收盘后运行此脚本")
        response = input("是否继续执行？(y/N): ")
        if response.lower() != 'y':
            logger.info("用户取消执行")
            return
    
    # 开始采集
    try:
        collector.collect_daily_tick_data()
        
        # 显示摘要
        summary = collector.get_collection_summary()
        if "error" not in summary:
            logger.info("采集摘要:")
            logger.info(f"  目标股票: {summary.get('target_count', 0)} 只")
            logger.info(f"  成功采集: {summary.get('success_count', 0)} 只")
            logger.info(f"  采集失败: {summary.get('failed_count', 0)} 只")
            logger.info(f"  总数据量: {summary.get('total_tick_count', 0)} 条")
        else:
            logger.error(f"获取摘要失败: {summary['error']}")
            
    except Exception as e:
        logger.error(f"采集过程中发生错误: {e}")
        raise

if __name__ == "__main__":
    main()
