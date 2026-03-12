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
SHORT_DROP_RATIO = 0.75
STRONG_INFLOW = 10
MAX_LOOKBACK = 30  # calendar days to collect 5 trading days

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
        if r.status_code == 200:
            print("Telegram sent")
        else:
            print("Telegram error:", r.text)
    except Exception as e:
        print("Telegram request failed:", e)

# =========================
# HK MARKET DATA
# =========================
def safe_hk_spot(retries=4):
    url = "https://push2ex.eastmoney.com/api/default/getHKStockList"
    params = {"pageSize": "300", "pageIndex": "1", "source": "web", "quoteType": "1", "_": int(time.time()*1000)}
    headers = {"User-Agent": "Mozilla/5.0"}
    for _ in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=8)
            data = r.json().get("data", {}).get("list", [])
            df = pd.DataFrame(data)
            df = df.rename(columns={"f12": "名称", "f13": "代码", "f14": "成交额"})
            df["股票代碼"] = format_code(df["代码"])
            return df
        except Exception as e:
            print("Retry:", e)
            time.sleep(random.randint(3, 6))
    return pd.DataFrame()

# =========================
# DUMMY SHORT DATA (REPLACE WITH REAL SOURCE)
# =========================
def get_short_data_stub():
    # Replace this with your working data source
    df = safe_hk_spot()
    if df.empty: return pd.DataFrame(columns=["股票代碼", "short_today", "short_avg"])
    df["short_today"] = [round(random.uniform(0,5),2) for _ in range(len(df))]
    df["short_avg"] = [round(random.uniform(1,4),2) for _ in range(len(df))]
    return df[["股票代碼","short_today","short_avg"]]

# =========================
# ANALYSIS
# =========================
def run_analysis():
    print("AI analysis starting...")
    df_all = safe_hk_spot()
    if df_all.empty:
        print("No market data")
        return

    df_all["成交额"] = pd.to_numeric(df_all["成交额"], errors="coerce")
    df_all = df_all.sort_values(by="成交额", ascending=False).head(TOP_VOLUME)

    df_short = get_short_data_stub()

    # Merge
    df = df_all.merge(df_short, on="股票代碼", how="left").fillna(0)
    df = df.head(TOP_OUTPUT)

    # Rank calculation
    df["rank"] = df["成交额"].rank(method="min", ascending=False)
    # For rank change, use previous rank if available (here we use a stub)
    df["prev_rank"] = df["rank"].shift(1).fillna(df["rank"])
    df["rank_change"] = (df["prev_rank"] - df["rank"]).astype(int)

    # Insights
    results = []
    for row in df.itertuples():
        curr = float(row.short_today)
        avg = float(row.short_avg)
        inflow = 0  # placeholder for net inflow
        insight = "✅ 正常"
        if avg > 0 and curr < avg*SHORT_DROP_RATIO:
            insight = "⚠️ 空頭平倉"
        results.append({
            "code": row.股票代碼,
            "name": row.名称,
            "inflow": round(inflow,2),
            "short_today": round(curr,2),
            "short_avg": round(avg,2),
            "insight": insight,
            "rank_change": int(row.rank_change)
        })

    output = {"update_time": datetime.now().strftime("%Y-%m-%d %H:%M"), "stocks": results}
    with open("data.json","w",encoding="utf-8") as f:
        json.dump(output,f,ensure_ascii=False,indent=4)
    print("data.json updated")

    if results:
        msg = f"📊 港股 AI 看板更新\n時間: {output['update_time']}\n榜首: {results[0]['name']}"
        send_telegram(msg)

# =========================
# MAIN
# =========================
if __name__=="__main__":
    run_analysis()
