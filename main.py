import requests
import pandas as pd
import json
import os
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
SHORT_RATIO_DROP = 0.25
MAX_LOOKBACK_DAYS = 30  # calendar days to collect 5 trading days

requests.adapters.DEFAULT_RETRIES = 5

# =========================
# HELPERS
# =========================
def format_code(series):
    return series.astype(str).str.zfill(5)

def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("Telegram not configured")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        r = requests.post(url, json=payload, timeout=15)
        print("Telegram status:", r.status_code)
    except Exception as e:
        print("Telegram request failed:", e)

# =========================
# HK MARKET DATA
# =========================
def safe_hk_spot(retries=4):
    url = "https://push2ex.eastmoney.com/api/default/getHKStockList"
    params = {"pageSize":"300","pageIndex":"1","source":"web","quoteType":"1","_":int(time.time()*1000)}
    headers = {"User-Agent":"Mozilla/5.0"}
    for _ in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=8)
            r.raise_for_status()
            data = r.json().get("data", {}).get("list", [])
            df = pd.DataFrame(data)
            df = df.rename(columns={"f12":"名称","f13":"代码","f14":"成交额"})
            df["股票代碼"] = format_code(df["代码"])
            df["成交额"] = pd.to_numeric(df["成交额"], errors="coerce")
            return df
        except Exception as e:
            print("Retry:", e)
            time.sleep(random.randint(3,6))
    return pd.DataFrame()

# =========================
# FETCH TODAY'S SHORT SELL
# =========================
def fetch_hk_short(date_str):
    url = "https://emhkgapi.eastmoney.com/api/StockShortInterest/GetStockShortInterestList"
    params = {"pageIndex":1,"pageSize":1000,"tradeDate":date_str,"_":int(time.time()*1000)}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data.get("Data") or not data["Data"].get("Data"):
            return pd.DataFrame(columns=["股票代碼","short_today"])
        rows = data["Data"]["Data"]
        df = pd.DataFrame(rows)
        df["股票代碼"] = df["TSecCode"].astype(str).str.zfill(5)
        df["short_today"] = pd.to_numeric(df.get("ShortRate", 0), errors="coerce")
        return df[["股票代碼","short_today"]]
    except Exception as e:
        print(f"[fetch_hk_short] Error for {date_str}: {e}")
        return pd.DataFrame(columns=["股票代碼","short_today"])

# =========================
# HISTORICAL 5-DAY AVERAGE
# =========================
def get_historical_short_avg(days=5):
    short_histories = []
    target_date = datetime.now() - timedelta(days=1)
    checked = 0
    while len(short_histories) < days and checked < MAX_LOOKBACK_DAYS:
        date_str = target_date.strftime("%Y%m%d")
        try:
            df = fetch_hk_short(date_str)
            if not df.empty:
                short_histories.append(df)
        except Exception as e:
            print(f"Error fetching {date_str}: {e}")
        target_date -= timedelta(days=1)
        checked +=1
        time.sleep(0.5)
    if not short_histories:
        return pd.DataFrame(columns=["股票代碼","short_avg"])
    df_all = pd.concat(short_histories)
    return df_all.groupby("股票代碼")["short_today"].mean().reset_index().rename(columns={"short_today":"short_avg"})

# =========================
# MAIN ANALYSIS
# =========================
def run_analysis():
    print("🚀 AI HK Stock Analysis Start...")
    df_all = safe_hk_spot()
    if df_all.empty:
        print("❌ No market data")
        return

    df_all = df_all.sort_values(by="成交额", ascending=False).head(TOP_VOLUME)

    today_str = datetime.now().strftime("%Y%m%d")
    df_short_today = fetch_hk_short(today_str)
    df_short_avg = get_historical_short_avg()

    df = df_all[["股票代碼","名称"]].copy()
    df = df.merge(df_short_today, on="股票代碼", how="left")
    df = df.merge(df_short_avg, on="股票代碼", how="left")
    df["short_today"] = df["short_today"].fillna(0)
    df["short_avg"] = df["short_avg"].fillna(0)

    results = []
    for row in df.itertuples():
        curr = float(row.short_today)
        avg = float(row.short_avg)
        insight = "✅ 正常"
        if avg > 0 and curr < avg*(1-SHORT_RATIO_DROP):
            insight = "⚠️ 空頭平倉"
        results.append({
            "code": row.股票代碼,
            "name": row.名称,
            "short_today": round(curr,2),
            "short_avg": round(avg,2),
            "insight": insight
        })

    output = {"update_time": datetime.now().strftime("%Y-%m-%d %H:%M"), "stocks": results}
    with open("data.json","w",encoding="utf-8") as f:
        json.dump(output,f, ensure_ascii=False, indent=4)
    print("🎉 data.json updated")

    if results:
        msg = f"📊 港股 AI 看板更新\n時間: {output['update_time']}\n榜首: {results[0]['name']} | {results[0]['insight']}"
        send_telegram(msg)

# =========================
# RUN
# =========================
if __name__=="__main__":
    run_analysis()
