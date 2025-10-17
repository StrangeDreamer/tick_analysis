#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化分析系统：热门股票分析、主力拆单识别、分时图绘制
"""

import akshare as ak
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import requests
import json
import hashlib
import base64
import hmac
import time
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

class QuantAnalysis:
    def __init__(self):
        self.hot_stocks = []
        self.tick_data = {}
        self.trade_directions = {}
        self.scores = {}
        
    def get_hot_stocks(self):
        """获取当日最热的沪深主板非ST A股股票"""
        try:
            # 使用热门股票排名接口
            hot_rank_df = ak.stock_hot_rank_em()
            
            if hot_rank_df is not None and not hot_rank_df.empty:
                # 过滤条件：沪深主板、非ST的股票、股价在5-30元之间、涨跌幅-5% - 7%
                filtered_stocks = hot_rank_df[
                    (hot_rank_df['代码'].str.startswith(('SZ000', 'SZ001', 'SZ002', 'SH600', 'SH601', 'SH603', 'SH605'))) &
                    (~hot_rank_df['股票名称'].str.contains('ST')) &
                    (hot_rank_df['最新价'] >= 5) &
                    (hot_rank_df['最新价'] <= 30) &
                    (hot_rank_df['涨跌幅'] < 7) &
                    (hot_rank_df['涨跌幅'] > -5)
                ].copy()
                
                # 按排名排序
                filtered_stocks = filtered_stocks.sort_values('当前排名')
                
                # 直接使用过滤后的股票，不进行市值筛选
                final_stocks = filtered_stocks[['代码', '股票名称', '最新价', '涨跌幅']].to_dict('records')
                
                self.hot_stocks = final_stocks
                
                print(f"✅ 最终获取{len(self.hot_stocks)}只符合条件的热门股票")
                for stock in self.hot_stocks:
                    print(f"{stock['代码']} {stock['股票名称']} 价格:{stock['最新价']} 涨跌幅:{stock['涨跌幅']:.2f}%")
                
                return self.hot_stocks
            else:
                print("❌ 热门股票数据为空")
                return []
                
        except Exception as e:
            print(f"❌ 获取热门股票失败: {e}")
            return []
    
    
    def get_tick_data(self, symbol, date=None):
        """获取股票的tick数据"""
        try:
            # 转换股票代码格式（SZ000001 -> sz000001）
            if symbol.startswith('SZ'):
                tick_symbol = symbol.lower()  # SZ000001 -> sz000001
            elif symbol.startswith('SH'):
                tick_symbol = symbol.lower()  # SH600000 -> sh600000
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
                    '成交量': '成交量',
                    '成交金额': '成交额',
                    '性质': '买卖盘性质'
                })
                
                # 转换时间格式
                tick_df['时间'] = pd.to_datetime(tick_df['时间'])
                
                # 按时间排序
                tick_df = tick_df.sort_values('时间')
                
                # 计算资金流向相关指标
                # 1. 计算价格变动（元）
                tick_df['dp'] = tick_df['成交价'].diff().fillna(0)

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
                # 如果mf是负数，就是主动流出（卖盘）
                tick_df['买卖盘性质'] = tick_df['mf'].apply(lambda x: '卖盘' if x < 0 else '买盘')
                
                # 重新计算成交量：|mf|/成交价，转换为手数（1手=100股）
                tick_df['成交量'] = (np.abs(tick_df['mf']) / tick_df['成交价'] / 100).round().astype(int)
                
                # 重新计算成交金额：|mf|
                tick_df['成交额'] = np.abs(tick_df['mf']).round().astype(int)
                # 打印最新的5条tick数据
                print(tick_df.tail(5))
                return tick_df[['时间', '成交价', '成交量', '成交额', '买卖盘性质', 'meanV', 'w2', 'prob', 'mf']]
            else:
                print(f"  {symbol} stock_zh_a_tick_tx_js 数据为空，尝试兜底方案...")
                raise Exception("stock_zh_a_tick_tx_js 返回空数据")
                
        except Exception as e:
            print(f"  {symbol} stock_zh_a_tick_tx_js 失败: {e}")
            print(f"  尝试使用 stock_intraday_em 兜底方案...")
            
            # 兜底方案：使用 stock_intraday_em
            try:
                # 转换股票代码格式用于 stock_intraday_em（需要纯数字）
                if symbol.startswith('SZ'):
                    intraday_symbol = symbol[2:]  # SZ000001 -> 000001
                elif symbol.startswith('SH'):
                    intraday_symbol = symbol[2:]  # SH600000 -> 600000
                else:
                    intraday_symbol = symbol
                
                tick_df = ak.stock_intraday_em(symbol=intraday_symbol)
                
                if tick_df is not None and not tick_df.empty:
                    print(f"  成功获取 {len(tick_df)} 条tick数据 (stock_intraday_em 兜底)")
                    
                    # 重命名列以保持一致性
                    tick_df = tick_df.rename(columns={
                        '时间': '时间',
                        '成交价': '成交价',
                        '手数': '成交量',  # stock_intraday_em 返回的是手数
                        '买卖盘性质': '买卖盘性质'
                    })
                    
                    # 计算成交额（手数 * 成交价 * 100，因为1手=100股）
                    tick_df['成交额'] = tick_df['成交量'] * tick_df['成交价'] * 100
                    
                    # 转换时间格式
                    tick_df['时间'] = pd.to_datetime(tick_df['时间'])
                    
                    # 按时间排序
                    tick_df = tick_df.sort_values('时间')
                    
                    # 计算资金流向相关指标
                    # 1. 计算价格变动（元）
                    tick_df['dp'] = tick_df['成交价'].diff().fillna(0)

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
                    # 如果mf是负数，就是主动流出（卖盘）
                    tick_df['买卖盘性质'] = tick_df['mf'].apply(lambda x: '卖盘' if x < 0 else '买盘')
                    
                    # 重新计算成交量：|mf|/成交价，转换为手数（1手=100股）
                    tick_df['成交量'] = (np.abs(tick_df['mf']) / tick_df['成交价'] / 100).round().astype(int)
                    
                    # 重新计算成交金额：|mf|
                    tick_df['成交额'] = np.abs(tick_df['mf']).round().astype(int)
                    
                    return tick_df[['时间', '成交价', '成交量', '成交额', '买卖盘性质', 'meanV', 'w2', 'prob', 'mf']]
                else:
                    print(f"  {symbol} stock_intraday_em 兜底方案也返回空数据")
                    return None
                    
            except Exception as e2:
                print(f"  {symbol} stock_intraday_em 兜底方案也失败: {e2}")
                return None



    def analyze_trade_direction(self, tick_df, symbol):
        """分析股票的主动买卖性质"""
        if tick_df is None or tick_df.empty:
            return {'buy_ratio': 0, 'sell_ratio': 0, 'neutral_ratio': 0, 'net_buy_volume': 0}
        
        # 统计买卖盘性质
        trade_counts = tick_df['买卖盘性质'].value_counts()
        total_trades = len(tick_df)
        
        # 计算各种交易类型的比例
        buy_count = trade_counts.get('买盘', 0)
        sell_count = trade_counts.get('卖盘', 0)
        neutral_count = trade_counts.get('中性盘', 0)
        
        buy_ratio = buy_count / total_trades if total_trades > 0 else 0
        sell_ratio = sell_count / total_trades if total_trades > 0 else 0
        neutral_ratio = neutral_count / total_trades if total_trades > 0 else 0
        
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
            'neutral_ratio': neutral_ratio,
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
        buy_sell_score = (active_buy_ratio - active_sell_ratio) * 50
        buy_sell_score = min(max(buy_sell_score, -35), 35)
        score += buy_sell_score * 0.70
        
        # 2. 净买入量得分 (30%) - 次要因子
        net_buy_volume = trade_direction['net_buy_volume']
        avg_volume = tick_df['成交量'].mean()
        net_buy_score = min(max(net_buy_volume / (avg_volume * 100), -10), 10)
        score += net_buy_score * 0.30
        
        return score
    
    def analyze_stocks(self):
        """分析所有热门股票"""
        for stock in self.hot_stocks:
            symbol = stock['代码']
            name = stock['股票名称']
            
            print(f"分析 {symbol} {name}...")
            
            # 获取tick数据
            tick_df = self.get_tick_data(symbol)
            if tick_df is None:
                print(f"  {symbol} 获取tick数据失败")
                continue
                
            # 分析交易方向
            trade_direction = self.analyze_trade_direction(tick_df, symbol)
            
            # 计算得分
            score = self.calculate_score(symbol, tick_df, trade_direction)
            
            # 存储结果
            self.tick_data[symbol] = tick_df
            self.trade_directions[symbol] = trade_direction
            self.scores[symbol] = {
                'name': name,
                'score': score,
                'trade_direction': trade_direction,
                'price_change': stock['涨跌幅']  # 添加涨跌幅信息
            }
            
            print(f"  {symbol} 分析完成，得分: {score:.2f}")
        
        # 按得分排序
        sorted_stocks = sorted(self.scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        # 筛选主动买入强度小于100%的股票
        filtered_stocks = []
        for symbol, data in sorted_stocks:
            active_buy_ratio = data['trade_direction']['active_buy_ratio']
            if active_buy_ratio < 100:  # 主动买入强度严格小于100%
                filtered_stocks.append((symbol, data))
                print(f"✅ {symbol} {data['name']} 主动买入强度: {active_buy_ratio:.1f}% (符合条件)")
            else:
                print(f"❌ {symbol} {data['name']} 主动买入强度: {active_buy_ratio:.1f}% (不符合条件)")
        
        return filtered_stocks  # 返回所有符合条件的股票
    
    def plot_intraday_chart(self, symbol, name, tick_df, trade_direction):
        """绘制分时图并标注交易方向"""
        if tick_df is None or tick_df.empty:
            return None
            
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), height_ratios=[3, 1, 1])
        
        # 标注买卖盘性质
        buy_mask = tick_df['买卖盘性质'] == '买盘'
        sell_mask = tick_df['买卖盘性质'] == '卖盘'
        neutral_mask = tick_df['买卖盘性质'] == '中性盘'
        
        if buy_mask.any():
            ax1.scatter(tick_df[buy_mask]['时间'], tick_df[buy_mask]['成交价'], 
                       c='red', s=2, alpha=0.6, label='买盘', marker='o')
        if sell_mask.any():
            ax1.scatter(tick_df[sell_mask]['时间'], tick_df[sell_mask]['成交价'], 
                       c='green', s=2, alpha=0.6, label='卖盘', marker='o')
        if neutral_mask.any():
            ax1.scatter(tick_df[neutral_mask]['时间'], tick_df[neutral_mask]['成交价'], 
                       c='gray', s=1, alpha=0.4, label='中性盘', marker='o')
        
        ax1.set_title(f'{symbol} {name} 分时图 - 交易方向分析', fontsize=14, fontweight='bold')
        ax1.set_ylabel('价格 (元)', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.legend()
        
        # 格式化x轴时间显示
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax1.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
        
        # 绘制成交量
        ax2.bar(tick_df['时间'], tick_df['成交量'], width=timedelta(minutes=1), 
                alpha=0.6, color='lightblue', label='成交量')
        ax2.set_ylabel('成交量', fontsize=12)
        ax2.grid(True, alpha=0.3)
        ax2.legend()
        
        # 格式化x轴时间显示
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax2.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
        
        # 绘制买卖盘比例
        buy_ratio = trade_direction['buy_ratio']
        sell_ratio = trade_direction['sell_ratio']
        neutral_ratio = trade_direction['neutral_ratio']
        
        ax3.bar(['买盘', '卖盘', '中性盘'], [buy_ratio, sell_ratio, neutral_ratio], 
                color=['red', 'green', 'gray'], alpha=0.7)
        ax3.set_ylabel('比例', fontsize=12)
        ax3.set_xlabel('交易类型', fontsize=12)
        ax3.set_title(f'交易方向分布 (买盘:{buy_ratio:.1%}, 卖盘:{sell_ratio:.1%}, 中性盘:{neutral_ratio:.1%})', 
                     fontsize=10)
        ax3.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # 保存图片
        filename = f"{symbol}_{name}_分时图.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def send_dingtalk_message(self, top_stocks, chart_files):
        """发送钉钉消息"""
        webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=ae055118615b242c6fe43fc3273a228f316209f707d07e7ce39fc83f4270ed82"
        secret = "SECf2b2861525388e240846ad1e2beb3b93d3b5f0d2e6634e43176b593f050e77da"
        
        # 筛选买盘比例小于98%的股票（涨跌幅已在获取热门股票时过滤）
        filtered_stocks = []
        for symbol, data in top_stocks:
            trade_direction = data['trade_direction']
            buy_ratio = trade_direction['buy_ratio']
            
            # 只检查买盘比例条件
            if buy_ratio < 0.98:  # 买盘比例小于98%
                filtered_stocks.append((symbol, data))
                print(f"✅ {symbol} {data['name']} 买盘比例:{buy_ratio:.1%} (符合发送条件)")
            else:
                print(f"❌ {symbol} {data['name']} 买盘比例:{buy_ratio:.1%} (买盘比例过高)")
        
        if not filtered_stocks:
            print("⚠️ 没有股票符合买盘比例筛选条件，不发送钉钉消息")
            return False
        
        # 构建消息内容
        message = {
            "msgtype": "markdown",
            "markdown": {
                "title": "量化分析报告",
                "text": f"""# 📈 量化分析报告 - {datetime.now().strftime('%Y-%m-%d %H:%M')}

## 🏆 股票评分排序 (共{len(filtered_stocks)}只，买盘比例<98%)

"""
            }
        }
        
        for i, (symbol, data) in enumerate(filtered_stocks, 1):
            trade_direction = data['trade_direction']
            
            message["markdown"]["text"] += f"""
### {i}. {symbol} {data['name']}
- **得分**: {data['score']:.2f}
- **涨跌幅**: {data['price_change']:.2f}%
- **买盘比例**: {trade_direction['buy_ratio']:.1%}
- **卖盘比例**: {trade_direction['sell_ratio']:.1%}
- **中性盘比例**: {trade_direction['neutral_ratio']:.1%}
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
    
    def run_analysis(self):
        """运行完整分析流程"""
        # 1. 获取热门股票
        hot_stocks = self.get_hot_stocks()
        if not hot_stocks:
            return
        
        # 2. 分析股票
        top_stocks = self.analyze_stocks()
        if not top_stocks:
            return
        
        # 3. 绘制图表（只绘制前3只股票）
        chart_files = []
        for symbol, data in top_stocks[:3]:  # 只绘制前3只股票的图表
            if symbol in self.tick_data:
                chart_file = self.plot_intraday_chart(symbol, data['name'], 
                                                    self.tick_data[symbol], 
                                                    data['trade_direction'])
                if chart_file:
                    chart_files.append(chart_file)
        
        # 4. 发送钉钉消息
        self.send_dingtalk_message(top_stocks, chart_files)

if __name__ == "__main__":
    # 创建分析实例并运行
    analyzer = QuantAnalysis()
    analyzer.run_analysis()
