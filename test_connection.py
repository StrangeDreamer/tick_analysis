#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
网络连接测试脚本
快速测试API连接是否正常
"""

import akshare as ak
import time

def test_api_connections():
    """测试各个API接口的连接状态"""
    print("=" * 60)
    print("API连接测试")
    print("=" * 60)
    
    # 测试1：获取热门股票
    print("\n1️⃣ 测试热门股票API...")
    try:
        hot_rank_df = ak.stock_hot_rank_em()
        if hot_rank_df is not None and not hot_rank_df.empty:
            print(f"   ✅ 成功！获取到 {len(hot_rank_df)} 只热门股")
            print(f"   示例: {hot_rank_df.head(3)[['代码', '股票名称']].to_dict('records')}")
        else:
            print("   ❌ 失败：返回数据为空")
    except Exception as e:
        print(f"   ❌ 失败：{e}")
    
    time.sleep(1)
    
    # 测试2：获取实时行情
    print("\n2️⃣ 测试实时行情API...")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"   尝试 {attempt + 1}/{max_retries}...")
            spot_df = ak.stock_zh_a_spot_em()
            if spot_df is not None and not spot_df.empty:
                print(f"   ✅ 成功！获取到 {len(spot_df)} 只股票的行情")
                print(f"   列名: {list(spot_df.columns[:10])}...")
                break
            else:
                print(f"   ⚠️ 数据为空，2秒后重试...")
                time.sleep(2)
        except Exception as e:
            print(f"   ❌ 第 {attempt + 1} 次失败：{e}")
            if attempt < max_retries - 1:
                print(f"   ⏳ 2秒后重试...")
                time.sleep(2)
    
    time.sleep(1)
    
    # 测试3：获取大盘指数
    print("\n3️⃣ 测试大盘指数API...")
    try:
        market_df = ak.stock_individual_spot_xq(symbol="SH000001")
        if market_df is not None and not market_df.empty:
            change_row = market_df[market_df['item'] == '涨幅']
            if not change_row.empty:
                market_change = change_row['value'].iloc[0]
                print(f"   ✅ 成功！上证指数涨跌幅: {market_change}%")
        else:
            print("   ❌ 失败：返回数据为空")
    except Exception as e:
        print(f"   ❌ 失败：{e}")
    
    time.sleep(1)
    
    # 测试4：获取Tick数据（测试一只股票）
    print("\n4️⃣ 测试Tick数据API...")
    test_symbol = "sh600000"  # 浦发银行
    try:
        print(f"   测试股票: {test_symbol}")
        tick_df = ak.stock_zh_a_tick_tx_js(symbol=test_symbol)
        if tick_df is not None and not tick_df.empty:
            print(f"   ✅ 成功！获取到 {len(tick_df)} 条tick数据")
            print(f"   最新一条: {tick_df.tail(1).to_dict('records')}")
        else:
            print("   ❌ 失败：返回数据为空（可能是非交易时段）")
    except Exception as e:
        print(f"   ❌ 失败：{e}")
        print("   ℹ️ 提示：非交易时段无法获取tick数据是正常的")
    
    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)
    print("\n💡 提示：")
    print("   - 如果多个API都失败，可能是网络问题或API服务维护中")
    print("   - 收盘后某些API可能不可用，这是正常现象")
    print("   - 建议在开市时间（9:30-15:00）测试以获得最佳结果")

if __name__ == "__main__":
    test_api_connections()
