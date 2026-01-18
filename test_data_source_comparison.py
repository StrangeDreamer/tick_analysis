
import pandas as pd
import akshare as ak
import sys
from datetime import datetime, timedelta

def get_latest_trade_dates(num_days: int = 5):
    """
    Gets the most recent N trading days from akshare using robust date handling.
    """
    print(f"🔄 Fetching last {num_days} trade dates...")
    try:
        trade_dates_df = ak.tool_trade_date_hist_sina()
        all_trade_dates = pd.to_datetime(trade_dates_df['trade_date']).dt.date
        today = datetime.now().date()
        recent_dates = sorted([d for d in all_trade_dates if d <= today])
        latest_dates_obj = recent_dates[-num_days:]
        latest_dates_str = [d.strftime('%Y%m%d') for d in latest_dates_obj]
        print(f"✅ Found latest {len(latest_dates_str)} trade dates: {latest_dates_str}")
        return latest_dates_str
    except Exception as e:
        print(f"❌ Could not fetch trade dates. Error: {e}")
        return []

def get_top_hot_stock():
    """
    Gets the top stock from the East Money hot stock ranking.
    """
    print("🔄 Fetching hot stocks from stock_hot_rank_em...")
    try:
        hot_rank_df = ak.stock_hot_rank_em()
        if hot_rank_df is None or hot_rank_df.empty:
            print("❌ Could not fetch hot stocks.")
            return None
        
        symbol = hot_rank_df.iloc[0]['代码']
        print(f"✅ Found top hot stock: {symbol} ({hot_rank_df.iloc[0]['股票名称']})")
        return symbol
    except Exception as e:
        print(f"❌ Error fetching hot stocks: {e}")
        return None

def fetch_tencent_data(symbol: str):
    """Fetches and processes tick data from Tencent, standardizing units."""
    print(f"🔄 Fetching Tencent data for {symbol}...")
    try:
        df = ak.stock_zh_a_tick_tx_js(symbol=symbol.lower())
        if df is None or df.empty: return None
        
        df.rename(columns={'成交时间': 'time', '成交价格': 'price', '成交量': 'volume_lots', '性质': 'type', '成交金额': 'value'}, inplace=True)
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').apply(lambda t: t.replace(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day))
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['volume_lots'] = pd.to_numeric(df['volume_lots'], errors='coerce')
        df['volume_shares'] = df['volume_lots'] * 100
        df.dropna(subset=['time', 'price', 'volume_shares', 'type'], inplace=True)
        return df[['time', 'price', 'volume_shares', 'type', 'value']]
    except Exception as e:
        print(f"❌ Error fetching Tencent data: {e}")
        return None

def fetch_em_data(symbol: str):
    """Fetches and processes intraday data from East Money, standardizing units."""
    print(f"🔄 Fetching East Money data for {symbol}...")
    try:
        pure_code = symbol[2:]
        df = ak.stock_intraday_em(symbol=pure_code)
        if df is None or df.empty: return None
            
        df.rename(columns={'时间': 'time', '成交价': 'price', '手数': 'volume_lots', '买卖盘性质': 'type'}, inplace=True)
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').apply(lambda t: t.replace(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day))
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['volume_lots'] = pd.to_numeric(df['volume_lots'], errors='coerce')
        df['volume_shares'] = df['volume_lots'] * 100
        df['value'] = df['price'] * df['volume_shares']
        df.dropna(subset=['time', 'price', 'volume_shares', 'type'], inplace=True)
        return df[['time', 'price', 'volume_shares', 'type', 'value']]
    except Exception as e:
        print(f"❌ Error fetching East Money data: {e}")
        return None

def fetch_sina_hist_data(symbol: str, date: str):
    """Fetches and processes historical intraday data from Sina for a specific date."""
    print(f"🔄 Fetching Sina (historical) data for {symbol} on {date}...")
    try:
        df = ak.stock_intraday_sina(symbol=symbol, date=date)
        if df is None or df.empty:
            print(f"❌ Could not fetch Sina historical data for {date}.")
            return None

        df.rename(columns={'ticktime': 'time', 'volume': 'volume_shares', 'kind': 'type'}, inplace=True)
        date_obj = datetime.strptime(date, "%Y%m%d")
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').apply(lambda t: t.replace(year=date_obj.year, month=date_obj.month, day=date_obj.day))
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['volume_shares'] = pd.to_numeric(df['volume_shares'], errors='coerce')
        df['value'] = df['price'] * df['volume_shares']
        type_map = {'U': '买盘', 'D': '卖盘', 'E': '中性盘'}
        df['type'] = df['type'].map(type_map)
        df.dropna(subset=['time', 'price', 'volume_shares', 'type'], inplace=True)
        return df[['time', 'price', 'volume_shares', 'type', 'value']]
    except KeyError as e:
        print(f"❌ KeyError processing Sina data for {date}: {e}. This confirms an issue within the akshare library. Please patch the 'stock_intraday_sina' function as discussed.")
        return None
    except Exception as e:
        print(f"❌ An unexpected error occurred while fetching Sina data for {date}: {e}")
        return None

def run_comparison(symbol: str):
    """Runs a comprehensive comparison for both real-time and historical data sources."""
    print(f"\n{'='*25} REAL-TIME DATA COMPARISON (Today) {'='*25}")
    
    tencent_df = fetch_tencent_data(symbol)
    em_df = fetch_em_data(symbol)
    
    print("\n" + "-"*20 + " Tencent (Last 15 Ticks) " + "-"*20)
    if tencent_df is not None:
        print(tencent_df.tail(15))
    else:
        print("No data available.")
        
    print("\n" + "-"*20 + " East Money (Last 15 Ticks) " + "-"*20)
    if em_df is not None:
        print(em_df.tail(15))
    else:
        print("No data available.")

    print(f"\n\n{'='*25} HISTORICAL DATA REVIEW (Sina) {'='*25}")
    trade_dates = get_latest_trade_dates(num_days=5)
    
    if not trade_dates:
        print("\n--- Historical review failed: Could not determine trade dates. ---")
    else:
        for date in trade_dates:
            sina_df = fetch_sina_hist_data(symbol, date=date)
            print("\n" + "-"*20 + f" Sina Data for {date} (Last 15 Ticks) " + "-"*20)
            if sina_df is not None:
                print(sina_df.tail(15))
            else:
                print("No data available for this day.")
        
    print("\n--- Test Complete ---")


if __name__ == "__main__":
    test_symbol = get_top_hot_stock()
    if test_symbol:
        run_comparison(test_symbol)
    else:
        print("Could not retrieve a stock to test. Exiting.")
        sys.exit(1)
