import akshare as ak
import pandas as pd
import requests
import os
import json
import time
from datetime import datetime, timedelta
import random

# =========================
# CONFIG
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

TOP_VOLUME = 40
TOP_OUTPUT = 30
SHORT_RATIO_DROP = 0.75  # threshold for short-covering signal
STRONG_INFLOW = 10
MAX_LOOKBACK = 30  # max calendar days to check for 5 trading days

requests.adapters.DEFAULT_RETRIES = 5

# =========================
# HELPERS
# =========================
def find_col_robust(df, keywords, fallback_numeric=False):
    """Return first column matching any keyword, else last numeric column if fallback_numeric=True."""
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

def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("[Telegram] Not configured")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code == 200:
            print("[Telegram] Message sent")
        else:
            print("[Telegram] Error:", r.text)
    except Exception as e:
        print("[Telegram] Request failed:", e)

# =========================
# HK MARKET DATA
# =========================
def safe_hk_spot(retries=4):
    url = "https://push2ex.eastmoney.com/api/default/getHKStockList"
    params = {"pageSize": "300", "pageIndex": "1", "source": "web", "quoteType": "1", "_": int(time.time()*1000)}
    headers = {"User-Agent": "Mozilla/5.0"}
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=8)
            data = r.json()["data"]["list"]
            df = pd.DataFrame(data)
            df = df.rename(columns={"f12": "名称", "f13": "代码", "f14": "成交额"})
            df["股票代碼"] = format_code(df["代码"])
            return df
        except Exception as e:
            print("Retry:", e)
            time.sleep(random.randint(3, 6))
    return pd.DataFrame()

# =========================
# Fetch most recent short sell data
# =========================
def get_short_recent():
    date = datetime.now()
    for _ in range(MAX_LOOKBACK):
        date_str = date.strftime("%Y%m%d")
        try:
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                print(f"[Info] Short data found for {date_str}")
                code_col = find_col_robust(df, ["代","代码","代碼","股票代码","f13"])
                ratio_col = find_col_robust(df, ["比率","比例","沽空比率","沽空比例","f9"])
                df["股票代碼"] = format_code(df[code_col])
                df = df[["股票代碼", ratio_col]].rename(columns={ratio_col: "short_today"})
                return df
        except:
            pass
        date -= timedelta(days=1)
    print("[Warning] No recent short sell data available")
    return pd.DataFrame(columns=["股票代碼","short_today"])

# =========================
# Historical 5-day average
# =========================
def get_historical_short_avg(days=5):
    results = []
    date = datetime.now() - timedelta(days=1)
    checked = 0
    while len(results) < days and checked < MAX_LOOKBACK:
        date_str = date.strftime("%Y%m%d")
        try:
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                code_col = find_col_robust(df, ["代","代码","代碼","股票代码","f13"])
                ratio_col = find_col_robust(df, ["比率","比例","沽空比率","沽空比例","f9"])
                df["股票代碼"] = format_code(df[code_col])
                results.append(df[["股票代碼", ratio_col]].rename(columns={ratio_col:"short_avg"}))
        except:
            pass
        date -= timedelta(days=1)
        checked += 1
        time.sleep(0.2)
    if not results:
        return pd.DataFrame(columns=["股票代碼","short_avg"])
    return pd.concat(results).groupby("股票代碼")["short_avg"].mean().reset_index()

# =========================
# Southbound capital
# =========================
def get_southbound():
    try:
        df = pd.concat([
            ak.stock_hk_ggt_board_em(symbol="滬港通"),
            ak.stock_hk_ggt_board_em(symbol="深港通")
        ])
        code_col = find_col_robust(df, ["代","代码","代碼","股票代码","f13"])
        buy_col = find_col_robust(df, ["买入","買入"])
        sell_col = find_col_robust(df, ["卖出","賣出"])
        df["股票代碼"] = format_code(df[code_col])
        df["net_inflow"] = (pd.to_numeric(df[buy_col],errors="coerce") - pd.to_numeric(df[sell_col],errors="coerce")) / 1e8
        return df[["股票代碼","net_inflow"]].drop_duplicates()
    except:
        return pd.DataFrame(columns=["股票代碼","net_inflow"])

# =========================
# Main analysis
# =========================
def run_analysis():
    print("[Info] Starting AI HK stock analysis...")
    df_all = safe_hk_spot()
    if df_all.empty:
        print("[Error] No market data retrieved")
        return

    vol_col = find_col_robust(df_all, ["成交额","成交金额","成交額","成交金額","f14"], fallback_numeric=True)
    name_col = find_col_robust(df_all, ["名称","名稱","f12"])
    df_all[vol_col] = pd.to_numeric(df_all[vol_col], errors="coerce")
    df_all = df_all.sort_values(by=vol_col, ascending=False).head(TOP_VOLUME)

    df_gt = get_southbound()
    df_short_today = get_short_recent()
    df_short_avg = get_historical_short_avg()

    df = df_all[["股票代碼", name_col]]
    df = df.merge(df_gt, on="股票代碼", how="left")
    df = df.merge(df_short_today, on="股票代碼", how="left")
    df = df.merge(df_short_avg, on="股票代碼", how="left")

    df["net_inflow"] = df["net_inflow"].fillna(0)
    df["short_today"] = df["short_today"].fillna(0)
    df["short_avg"] = df["short_avg"].fillna(0)
    df = df.head(TOP_OUTPUT)

    results = []
    for row in df.itertuples():
        inflow = float(row.net_inflow)
        curr = float(row.short_today)
        avg = float(row.short_avg) if pd.notna(row.short_avg) else None

        insight = "✅ 正常"
        # 空頭平倉邏輯：今日沽空率顯著低於平均
        if avg and avg > 0 and curr < avg*SHORT_RATIO_DROP and inflow > 1.5:
            insight = "⚠️ 空頭平倉"
        elif inflow > STRONG_INFLOW:
            insight = "🔥 主力掃貨"

        results.append({
            "code": row.股票代碼,
            "name": getattr(row,name_col),
            "inflow": round(inflow,2),
            "short_today": round(curr,2),
            "short_avg": round(avg,2) if avg else None,
            "insight": insight
        })

    output = {"update_time": datetime.now().strftime("%Y-%m-%d %H:%M"), "stocks": results}
    with open("data.json","w",encoding="utf-8") as f:
        json.dump(output,f,ensure_ascii=False,indent=4)
    print("[Info] data.json updated")

    if results and any(s["short_today"] > 0 or s["net_inflow"] > 0 for s in results):
        msg = f"📊 港股 AI 看板更新\n時間: {output['update_time']}\n榜首: {results[0]['name']}"
        send_telegram(msg)
    else:
        print("[Info] No valid data for Telegram push")

# =========================
# MAIN
# =========================
if __name__=="__main__":
    run_analysis()
