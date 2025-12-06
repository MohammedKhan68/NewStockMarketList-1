# update_stocks.py
# Fetches current price and percent returns for 1m/3m/6m/1y using yfinance
# Writes stocks.csv

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

TICKER_FILE = "tickers.txt"
OUTPUT_CSV = "stocks.csv"
COMPANY_NAMES_FILE = "company_names.csv"  # optional: if present, will be used

# trading-day approximations
OFFSETS = {
    "OneMonth": 21,
    "ThreeMonth": 63,
    "SixMonth": 126,
    "OneYear": 252
}

def nearest_close(series, target_idx):
    # return the closest available value at or before the index
    if target_idx < 0:
        return np.nan
    try:
        return series.iloc[target_idx]
    except Exception:
        return np.nan

def pct_change_from_n_days(close_series, n_days):
    if len(close_series) == 0:
        return np.nan
    last = nearest_close(close_series, -1)
    idx_n = -1 - n_days
    if abs(idx_n) > len(close_series):
        # not enough history
        return np.nan
    earlier = nearest_close(close_series, idx_n)
    if pd.isna(last) or pd.isna(earlier) or earlier == 0:
        return np.nan
    return (last - earlier) / earlier * 100.0

def get_company_name(ticker):
    try:
        t = yf.Ticker(ticker)
        info = t.info
        name = info.get("shortName") or info.get("longName") or ticker
        return name
    except Exception:
        return ticker

def main():
    with open(TICKER_FILE, "r") as f:
        tickers = [l.strip().upper() for l in f if l.strip()]

    rows = []
    for sym in tickers:
        try:
            t = yf.Ticker(sym)
            # get history for last ~400 trading days to be safe
            hist = t.history(period="1y", interval="1d", actions=False, auto_adjust=False)
            if hist.empty:
                # fallback to fast info
                cur_price = t.info.get("regularMarketPrice", None)
                close_series = pd.Series(dtype=float)
            else:
                close_series = hist['Close'].dropna().reset_index(drop=True)
                cur_price = close_series.iloc[-1] if len(close_series)>0 else t.info.get("regularMarketPrice", None)

            one_m = pct_change_from_n_days(close_series, OFFSETS["OneMonth"])
            three_m = pct_change_from_n_days(close_series, OFFSETS["ThreeMonth"])
            six_m = pct_change_from_n_days(close_series, OFFSETS["SixMonth"])
            one_y = pct_change_from_n_days(close_series, OFFSETS["OneYear"])

            company = get_company_name(sym)

            rows.append({
                "Symbol": sym,
                "Company": company,
                "CurrentPrice": round(float(cur_price) if cur_price is not None else np.nan, 4),
                "OneMonthPct": round(one_m, 2) if not pd.isna(one_m) else "",
                "ThreeMonthPct": round(three_m, 2) if not pd.isna(three_m) else "",
                "SixMonthPct": round(six_m, 2) if not pd.isna(six_m) else "",
                "OneYearPct": round(one_y, 2) if not pd.isna(one_y) else "",
                "DataAsOf": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })
            print(f"Fetched {sym}")
        except Exception as e:
            print(f"Error fetching {sym}: {e}")
            rows.append({
                "Symbol": sym,
                "Company": sym,
                "CurrentPrice": "",
                "OneMonthPct": "",
                "ThreeMonthPct": "",
                "SixMonthPct": "",
                "OneYearPct": "",
                "DataAsOf": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })

    df = pd.DataFrame(rows, columns=["Symbol","Company","CurrentPrice","OneMonthPct","ThreeMonthPct","SixMonthPct","OneYearPct","DataAsOf"])
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
