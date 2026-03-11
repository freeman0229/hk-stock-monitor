import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time

MAX_LOOKBACK = 30  # look back up to 30 calendar days

def find_col_robust(df, keywords, fallback_numeric=False):
    for c in df.columns:
        for kw in keywords:
            if kw in c:
                return c
    if fallback_numeric:
        numeric_cols = df.select_dtypes(include="number").columns
        if len(numeric_cols) > 0:
            return numeric_cols[-1]
    return None

def format_code(series):
    return series.astype(str).str.zfill(5)

# ===============================
# TODAY SHORT SELL (latest available)
# ===============================
def get_short_today():
    date = datetime.now()
    attempts = 0
    while attempts < MAX_LOOKBACK:
        try:
            df = ak.stock_hksell_summary(date=date.strftime("%Y%m%d"))
            if not df.empty:
                code_col = find_col_robust(df, ["代","代码","代碼","证券代码","f13"])
                ratio_col = find_col_robust(df, ["比率","比例"])
                df["股票代碼"] = format_code(df[code_col])
                df["short_today"] = pd.to_numeric(df[ratio_col], errors="coerce")
                return df[["股票代碼","short_today"]]
        except:
            pass
        # fallback to previous day
        date -= timedelta(days=1)
        attempts += 1
        time.sleep(0.2)
    # If all fail, return empty df
    return pd.DataFrame(columns=["股票代碼","short_today"])

# ===============================
# HISTORICAL 5-DAY SHORT AVG
# ===============================
def get_historical_short_avg(days=5):
    results = []
    date = datetime.now() - timedelta(days=1)
    checked = 0
    while len(results) < days and checked < MAX_LOOKBACK:
        try:
            df = ak.stock_hksell_summary(date=date.strftime("%Y%m%d"))
            if not df.empty:
                code_col = find_col_robust(df, ["代","代码","代碼","证券代码","f13"])
                ratio_col = find_col_robust(df, ["比率","比例"])
                df["股票代碼"] = format_code(df[code_col])
                df["short_avg"] = pd.to_numeric(df[ratio_col], errors="coerce")
                results.append(df[["股票代碼","short_avg"]])
        except:
            pass
        date -= timedelta(days=1)
        checked += 1
        time.sleep(0.2)
    if not results:
        return pd.DataFrame(columns=["股票代碼","short_avg"])
    return pd.concat(results).groupby("股票代碼")["short_avg"].mean().reset_index()
