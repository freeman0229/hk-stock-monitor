import akshare as ak
import pandas as pd
import requests
import os
import json
import time
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

TOP_VOLUME = 40
TOP_OUTPUT = 30
MAX_LOOKBACK = 30  # max calendar days to look back for 5 trading days

# =========================
# TELEGRAM FUNCTION
# =========================
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("[WARN] Telegram not configured")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        r = requests.post(url, data=payload, timeout=20)
        print("[INFO] Telegram response:", r.text)
    except Exception as e:
        print("[ERROR] Telegram send failed:", e)

# =========================
# UTILITY FUNCTIONS
# =========================
def find_col(df, keywords, col_type=""):
    for c in df.columns:
        for k in keywords:
            if k in c:
                print(f"[INFO] Detected {col_type} column: '{c}' for keywords {keywords}")
                return c
    print(f"[WARN] Could not detect {col_type} column. Available columns: {list(df.columns)}")
    return None

def format_code(series):
    return series.astype(str).str.zfill(5)

# =========================
# HK MARKET VOLUME
# =========================
def get_market_volume():
    try:
        df = ak.stock_hk_spot_em()
        code_col = find_col(df, ["代码","代碼"], "market code")
        name_col = find_col(df, ["名称","名稱"], "market name")
        vol_col = find_col(df, ["成交额","成交金額","成交額"], "market volume")
        if not code_col or not vol_col:
            return pd.DataFrame()
        df["股票代碼"] = format_code(df[code_col])
        df["成交額"] = pd.to_numeric(df[vol_col], errors="coerce")
        df = df.sort_values("成交額", ascending=False)
        return df[["股票代碼", name_col, "成交額"]].head(TOP_VOLUME)
    except Exception as e:
        print("[ERROR] Market volume fetch failed:", e)
        return pd.DataFrame()

# =========================
# SOUTHBOUND CAPITAL
# =========================
def get_southbound():
    try:
        df1 = ak.stock_hk_ggt_board_em(symbol="沪港通")
        df2 = ak.stock_hk_ggt_board_em(symbol="深港通")
        df = pd.concat([df1, df2])
        code_col = find_col(df, ["代码","代碼"], "southbound code")
        buy_col = find_col(df, ["买入","買入"], "southbound buy")
        sell_col = find_col(df, ["卖出","賣出"], "southbound sell")
        if not code_col or not buy_col or not sell_col:
            return pd.DataFrame(columns=["股票代碼","net_inflow"])
        df["股票代碼"] = format_code(df[code_col])
        buy = pd.to_numeric(df[buy_col], errors="coerce")
        sell = pd.to_numeric(df[sell_col], errors="coerce")
        df["net_inflow"] = (buy - sell) / 1e8
        return df[["股票代碼","net_inflow"]].groupby("股票代碼").sum().reset_index()
    except Exception as e:
        print("[ERROR] Southbound fetch failed:", e)
        return pd.DataFrame(columns=["股票代碼","net_inflow"])

# =========================
# TODAY SHORT SELL
# =========================
def get_short_today():
    try:
        df = ak.stock_hksell_summary()
        code_col = find_col(df, ["股票代码","股票代碼","代码","代碼"], "short today code")
        ratio_col = find_col(df, ["沽空比率","沽空比例","short_ratio","Short Ratio","ratio"], "short today ratio")
        if not code_col or not ratio_col:
            return pd.DataFrame(columns=["股票代碼","short_today"])
        df["股票代碼"] = format_code(df[code_col].astype(str))
        df["short_today"] = pd.to_numeric(df[ratio_col], errors="coerce")
        return df[["股票代碼","short_today"]]
    except Exception as e:
        print("[ERROR] Short today fetch failed:", e)
        return pd.DataFrame(columns=["股票代碼","short_today"])

# =========================
# HISTORICAL 5-DAY SHORT AVG
# =========================
def get_short_avg():
    histories = []
    date = datetime.now() - timedelta(days=1)
    attempts = 0
    while len(histories) < 5 and attempts < MAX_LOOKBACK:
        try:
            df = ak.stock_hksell_summary(date=date.strftime("%Y%m%d"))
            if not df.empty:
                code_col = find_col(df, ["股票代码","股票代碼","代码","代碼"], "short avg code")
                ratio_col = find_col(df, ["沽空比率","沽空比例","short_ratio","Short Ratio","ratio"], "short avg ratio")
                if code_col and ratio_col:
                    df["股票代碼"] = format_code(df[code_col].astype(str))
                    df["ratio"] = pd.to_numeric(df[ratio_col], errors="coerce")
                    histories.append(df[["股票代碼","ratio"]])
        except Exception as e:
            print("[ERROR] Historical short fetch failed:", e)
        date -= timedelta(days=1)
        attempts += 1
        time.sleep(0.4)
    if not histories:
        print("[WARN] No historical short data found")
        return pd.DataFrame(columns=["股票代碼","short_avg"])
    df_all = pd.concat(histories)
    avg_df = df_all.groupby("股票代碼")["ratio"].mean().reset_index()
    avg_df.rename(columns={"ratio":"short_avg"}, inplace=True)
    return avg_df

# =========================
# MAIN ANALYSIS
# =========================
def run_analysis():
    print("[INFO] Starting analysis...")
    df_vol = get_market_volume()
    if df_vol.empty:
        print("[WARN] No market data")
        return
    df_gt = get_southbound()
    df_short_today = get_short_today()
    df_short_avg = get_short_avg()

    df = df_vol.merge(df_gt, on="股票代碼", how="left")
    df = df.merge(df_short_today, on="股票代碼", how="left")
    df = df.merge(df_short_avg, on="股票代碼", how="left")

    df["net_inflow"] = df["net_inflow"].fillna(0)
    df["short_today"] = df["short_today"].fillna(0)

    results = []

    for row in df.head(TOP_OUTPUT).itertuples():
        inflow = float(row.net_inflow)
        curr = float(row.short_today)
        avg = float(row.short_avg) if pd.notna(row.short_avg) else None
        insight = "✅ 正常"
        if avg and curr < avg*0.6 and inflow > 5:
            insight = "⚠️ 逼空信號"
        elif inflow > 10:
            insight = "🔥 主力掃貨"
        results.append({
            "code": row.股票代碼,
            "name": getattr(row, df_vol.columns[1]),
            "inflow": round(inflow,2),
            "short_today": round(curr,2),
            "short_avg": round(avg,2) if avg else None,
            "insight": insight
        })

    output = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "stocks": results
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    print("[INFO] data.json updated")

    if results:
        top = results[0]
        msg = (
            f"📊 港股 AI 監控更新\n"
            f"時間: {output['update_time']}\n"
            f"Top: {top['name']} ({top['code']})\n"
            f"淨流入: {top['inflow']}億\n"
            f"沽空比率: {top['short_today']}%"
        )
        send_telegram(msg)

# =========================
# RUN
# =========================
if __name__ == "__main__":
    run_analysis()
