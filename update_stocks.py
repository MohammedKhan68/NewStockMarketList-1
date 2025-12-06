# update_stocks.py
# Fetches current price and percent returns for 1m/3m/6m/1y using yfinance
# Writes stocks.csv
# Improved: 3y history, retry logic, explicit "NA" for missing returns

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import time

TICKER_FILE = "tickers.txt"
OUTPUT_CSV = "stocks.csv"

# trading-day approximations
OFFSETS = {
    "OneMonth": 21,
    "ThreeMonth": 63,
    "SixMonth": 126,
    "OneYear": 252
}

RETRY_ATTEMPTS = 3
RETRY_SLEEP = 2  # seconds

def safe_history(ticker, period="3y", interval="1d"):
    attempt = 0
    while attempt < RETRY_ATTEMPTS:
        try:
            hist = ticker.history(period=period, interval=interval, actions=False, auto_adjust=False)
            # ensure index is sorted and no duplicate indexes
            if not hist.empty:
                hist = hist.sort_index().drop_duplicates()
            return hist
        except Exception as e:
            attempt += 1
            time.sleep(RETRY_SLEEP)
    return pd.DataFrame()

def pct_change_from_n_days(close_series, n_days):
    """
    close_series: pandas Series of close prices (chronological order)
    n_days: trading-day offset to look-back from the last available close
    returns percent (float) or None if not enough data
    """
    if close_series is None or len(close_series) == 0:
        return None
    last_idx = len(close_series) - 1
    lookback_idx = last_idx - n_days
    if lookback_idx < 0:
        return None
    try:
        last = float(close_series.iloc[last_idx])
        earlier = float(close_series.iloc[lookback_idx])
        if earlier == 0:
            return None
        return (last - earlier) / earlier * 100.0
    except Exception:
        return None

def get_company_name(ticker_obj, symbol):
    try:
        info = ticker_obj.info
        name = info.get("shortName") or info.get("longName") or symbol
        return name
    except Exception:
        return symbol

def fetch_one(sym):
    t = yf.Ticker(sym)
    hist = safe_history(t, period="3y", interval="1d")
    if not hist.empty and "Close" in hist.columns:
        close_series = hist["Close"].dropna().reset_index(drop=True)
        cur_price = float(close_series.iloc[-1]) if len(close_series)>0 else None
    else:
        # fallback to fast_info or quotes
        try:
            cur_price = t.fast_info.get("lastPrice") if hasattr(t, "fast_info") else None
        except Exception:
            cur_price = None
        close_series = pd.Series(dtype=float)

    one_m = pct_change_from_n_days(close_series, OFFSETS["OneMonth"])
    three_m = pct_change_from_n_days(close_series, OFFSETS["ThreeMonth"])
    six_m = pct_change_from_n_days(close_series, OFFSETS["SixMonth"])
    one_y = pct_change_from_n_days(close_series, OFFSETS["OneYear"])

    company = get_company_name(t, sym)

    return {
        "Symbol": sym,
        "Company": company,
        "CurrentPrice": round(float(cur_price), 4) if cur_price is not None else "",
        "OneMonthPct": round(one_m, 2) if one_m is not None else "NA",
        "ThreeMonthPct": round(three_m, 2) if three_m is not None else "NA",
        "SixMonthPct": round(six_m, 2) if six_m is not None else "NA",
        "OneYearPct": round(one_y, 2) if one_y is not None else "NA",
        "DataAsOf": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

def main():
    with open(TICKER_FILE, "r") as f:
        tickers = [l.strip().upper() for l in f if l.strip()]

    rows = []
    for sym in tickers:
        try:
            print(f"Fetching {sym} ...")
            row = fetch_one(sym)
            rows.append(row)
            print(f" OK {sym}")
        except Exception as e:
            print(f"Error fetching {sym}: {e}")
            rows.append({
                "Symbol": sym,
                "Company": sym,
                "CurrentPrice": "",
                "OneMonthPct": "NA",
                "ThreeMonthPct": "NA",
                "SixMonthPct": "NA",
                "OneYearPct": "NA",
                "DataAsOf": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })

    df = pd.DataFrame(rows, columns=["Symbol","Company","CurrentPrice","OneMonthPct","ThreeMonthPct","SixMonthPct","OneYearPct","DataAsOf"])
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
