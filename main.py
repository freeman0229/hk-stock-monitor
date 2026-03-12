import os, time, json, random
import pandas as pd
from datetime import datetime, timedelta
import requests
import akshare as ak

# =========================
# CONFIG
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
TOP_VOLUME = 40
TOP_OUTPUT = 30
MAX_LOOKBACK = 30  # days for 5-day short avg
SHORT_DROP_RATIO = 0.75

# =========================
# HELPERS
# =========================
def format_code(series):
    return series.astype(str).str.zfill(5)

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=15)
        if r.status_code==200:
            print("Telegram sent")
        else:
            print("Telegram error:", r.text)
    except Exception as e:
        print("Telegram request failed:", e)

# =========================
# HK MARKET DATA
# =========================
def get_hk_market():
    try:
        data = ak.stock_hk_spot_em()  # akshare stable function
        data["股票代码"] = format_code(data["代码"])
        data["成交额"] = pd.to_numeric(data["成交额"], errors="coerce")
        data = data.sort_values("成交额", ascending=False).head(TOP_VOLUME)
        return data[["股票代码","名称","成交额"]]
    except:
        return pd.DataFrame(columns=["股票代码","名称","成交额"])

# =========================
# SOUTHBOUND CAPITAL
# =========================
def get_southbound():
    try:
        df1 = ak.stock_hk_ggt_board_em(symbol="沪港通")
        df2 = ak.stock_hk_ggt_board_em(symbol="深港通")
        df = pd.concat([df1, df2])
        df["股票代码"] = format_code(df["证券代码"])
        df["net_inflow"] = pd.to_numeric(df["买入额"], errors="coerce") - pd.to_numeric(df["卖出额"], errors="coerce")
        df["net_inflow"] = df["net_inflow"] / 1e8  # to billions
        return df[["股票代码","net_inflow"]].drop_duplicates()
    except:
        return pd.DataFrame(columns=["股票代码","net_inflow"])

# =========================
# SHORT SELL TODAY
# =========================
def get_short_today(date=None):
    date = date or datetime.now()
    # Example: use CSV from SFC, e.g. "sfc_short_YYYYMMDD.csv"
    filename = f"sfc_short_{date.strftime('%Y%m%d')}.csv"
    if os.path.exists(filename):
        df = pd.read_csv(filename)
        df["股票代码"] = format_code(df["股票代码"])
        return df[["股票代码","short_today"]]
    return pd.DataFrame(columns=["股票代码","short_today"])

# =========================
# 5-DAY AVERAGE SHORT SELL
# =========================
def get_short_avg(days=5):
    short_histories = []
    target = datetime.now() - timedelta(days=1)
    checked = 0
    while len(short_histories)<days and checked<MAX_LOOKBACK:
        df = get_short_today(target)
        if not df.empty:
            short_histories.append(df.rename(columns={"short_today":"short_avg"}))
        target -= timedelta(days=1)
        checked +=1
        time.sleep(0.2)
    if not short_histories:
        return pd.DataFrame(columns=["股票代码","short_avg"])
    return pd.concat(short_histories).groupby("股票代码")["short_avg"].mean().reset_index()

# =========================
# MAIN ANALYSIS
# =========================
def run_analysis():
    print("Starting AI analysis...")
    df_all = get_hk_market()
    if df_all.empty:
        print("No market data")
        return
    df_gt = get_southbound()
    df_short_today = get_short_today()
    df_short_avg = get_short_avg()

    df = df_all.merge(df_gt, on="股票代码", how="left")
    df = df.merge(df_short_today, on="股票代码", how="left")
    df = df.merge(df_short_avg, on="股票代码", how="left")
    df["net_inflow"] = df["net_inflow"].fillna(0)
    df["short_today"] = df["short_today"].fillna(0)
    df["short_avg"] = df["short_avg"].fillna(0)
    df = df.head(TOP_OUTPUT)

    results = []
    for row in df.itertuples():
        inflow = float(row.net_inflow)
        curr = float(row.short_today)
        avg = float(row.short_avg)
        insight = "✅ 正常"
        if avg>0 and curr<(avg*SHORT_DROP_RATIO) and inflow>1.5:
            insight = "⚠️ 空頭平倉"
        elif inflow>10:
            insight = "🔥 主力掃貨"
        results.append({
            "code": row.股票代码,
            "name": row.名称,
            "inflow": round(inflow,2),
            "short_today": round(curr,2),
            "short_avg": round(avg,2),
            "insight": insight,
            "rank_change":0
        })

    output = {"update_time": datetime.now().strftime("%Y-%m-%d %H:%M"), "stocks": results}
    with open("data.json","w",encoding="utf-8") as f:
        json.dump(output,f,ensure_ascii=False, indent=4)
    print("data.json updated")

    if results:
        msg = f"📊 港股 AI 看板更新\n時間: {output['update_time']}\n榜首: {results[0]['name']}"
        send_telegram(msg)

if __name__=="__main__":
    run_analysis()
