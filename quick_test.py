#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""快速测试备用方案"""

import akshare as ak

print("测试热门股票API...")
try:
    hot_rank_df = ak.stock_hot_rank_em()
    if hot_rank_df is not None and not hot_rank_df.empty:
        print(f"✅ 获取到 {len(hot_rank_df)} 只热门股")
        print(f"\n列名: {list(hot_rank_df.columns)}")
        print(f"\n前10只股票:")
        
        backup_stocks = []
        for _, row in hot_rank_df.iterrows():
            code = str(row['代码'])
            name = str(row.get('股票名称', row.get('名称', '')))
            
            # 修复后的筛选逻辑
            is_main_board = code.startswith('SH60') or code.startswith('SZ00')
            is_not_st = 'ST' not in name
            
            if is_main_board and is_not_st:
                backup_stocks.append({'代码': code, '股票名称': name})
                print(f"  ✅ {code} {name}")
            
            if len(backup_stocks) >= 10:
                break
        
        print(f"\n筛选结果: {len(backup_stocks)} 只主板非ST股票")
        
        if backup_stocks:
            print(f"\n✅ 备用方案有效！可以返回 {len(backup_stocks)} 只股票")
        else:
            print(f"\n❌ 备用方案无效，筛选后为空")
    else:
        print("❌ 未获取到数据")
except Exception as e:
    print(f"❌ 异常: {e}")
    import traceback
    traceback.print_exc()
