import akshare as ak
import pandas as pd
import requests
import json
import time
import random
from datetime import datetime, timedelta

# =========================
# TELEGRAM CONFIG
# =========================
TELEGRAM_TOKEN = "YOUR_BOT_TOKEN_HERE"
CHAT_ID = "83151187"

# =========================
# SETTINGS
# =========================
TOP_VOLUME = 40
TOP_OUTPUT = 30
MAX_LOOKBACK = 30

requests.adapters.DEFAULT_RETRIES = 5

# =========================
# HELPER FUNCTIONS
# =========================
def find_col(df, keywords, fallback_numeric=False):
    for col in df.columns:
        for k in keywords:
            if k in col:
                return col

    if fallback_numeric:
        nums = df.select_dtypes(include="number").columns
        if len(nums) > 0:
            return nums[-1]

    return None


def format_code(series):
    return series.astype(str).str.zfill(5)


# =========================
# TELEGRAM
# =========================
def send_telegram(msg):

    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("Telegram not configured")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    payload = {
        "chat_id": str(CHAT_ID),
        "text": msg
    }

    try:
        r = requests.post(url, data=payload, timeout=15)
        print("Telegram:", r.status_code, r.text)
    except Exception as e:
        print("Telegram failed:", e)


# =========================
# HK MARKET DATA
# =========================
def safe_hk_spot(retries=4):

    url = "https://push2ex.eastmoney.com/api/default/getHKStockList"

    params = {
        "pageSize": "300",
        "pageIndex": "1",
        "source": "web",
        "quoteType": "1",
        "_": int(time.time() * 1000),
    }

    headers = {"User-Agent": "Mozilla/5.0"}

    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=8)
            raw = r.json()["data"]["list"]

            df = pd.DataFrame(raw)

            df = df.rename(columns={
                "f12": "名稱",
                "f13": "代碼",
                "f14": "成交額"
            })

            df["股票代碼"] = format_code(df["代碼"])

            return df

        except Exception as e:
            print("Retry market fetch:", e)
            time.sleep(random.randint(3, 6))

    return pd.DataFrame()


# =========================
# SOUTHBOUND CAPITAL
# =========================
def get_southbound():

    try:

        df = pd.concat([
            ak.stock_hk_ggt_board_em(symbol="滬港通"),
            ak.stock_hk_ggt_board_em(symbol="深港通")
        ])

        code_col = find_col(df, ["代碼","代码","证券代码","代"])
        buy_col = find_col(df, ["買入","买入"])
        sell_col = find_col(df, ["賣出","卖出"])

        df["股票代碼"] = format_code(df[code_col])

        df["net_inflow"] = (
            pd.to_numeric(df[buy_col], errors="coerce")
            - pd.to_numeric(df[sell_col], errors="coerce")
        ) / 1e8

        return df[["股票代碼","net_inflow"]].drop_duplicates()

    except:
        return pd.DataFrame(columns=["股票代碼","net_inflow"])


# =========================
# TODAY SHORT SELL
# =========================
def get_short_today():

    try:

        df = ak.stock_hksell_summary()

        code_col = find_col(df, ["代碼","代码","证券代码","代"])
        ratio_col = find_col(df, ["比率","比例"])

        df["股票代碼"] = format_code(df[code_col])

        return df[["股票代碼", ratio_col]].rename(
            columns={ratio_col:"short_today"}
        )

    except:
        return pd.DataFrame(columns=["股票代碼","short_today"])


# =========================
# HISTORICAL SHORT AVERAGE
# =========================
def get_historical_short_avg(days=5):

    results = []

    date = datetime.now() - timedelta(days=1)
    checked = 0

    while len(results) < days and checked < MAX_LOOKBACK:

        try:

            df = ak.stock_hksell_summary(
                date=date.strftime("%Y%m%d")
            )

            if not df.empty:

                code_col = find_col(df, ["代碼","代码","证券代码","代"])
                ratio_col = find_col(df, ["比率","比例"])

                df["股票代碼"] = format_code(df[code_col])

                results.append(
                    df[["股票代碼", ratio_col]]
                    .rename(columns={ratio_col:"short_avg"})
                )

        except:
            pass

        date -= timedelta(days=1)
        checked += 1
        time.sleep(0.5)

    if not results:
        return pd.DataFrame(columns=["股票代碼","short_avg"])

    return (
        pd.concat(results)
        .groupby("股票代碼")["short_avg"]
        .mean()
        .reset_index()
    )


# =========================
# MAIN ANALYSIS
# =========================
def run_analysis():

    print("AI analysis starting...")

    send_telegram("🚀 HK Short Scanner started")

    df_all = safe_hk_spot()

    if df_all.empty:
        print("No market data")
        return

    vol_col = find_col(df_all,
        ["成交額","成交额","成交金額","成交金额","f14"],
        fallback_numeric=True
    )

    name_col = find_col(df_all, ["名稱","名称","f12"])

    df_all[vol_col] = pd.to_numeric(df_all[vol_col], errors="coerce")

    df_all = (
        df_all
        .sort_values(by=vol_col, ascending=False)
        .head(TOP_VOLUME)
    )

    df_gt = get_southbound()
    df_short_today = get_short_today()
    df_short_avg = get_historical_short_avg()

    df = df_all[["股票代碼", name_col]]

    df = df.merge(df_gt, on="股票代碼", how="left")
    df = df.merge(df_short_today, on="股票代碼", how="left")
    df = df.merge(df_short_avg, on="股票代碼", how="left")

    df["net_inflow"] = df["net_inflow"].fillna(0)
    df["short_today"] = df["short_today"].fillna(0)

    df = df.head(TOP_OUTPUT)

    results = []

    for row in df.itertuples():

        inflow = float(row.net_inflow)
        curr = float(row.short_today)

        avg = float(row.short_avg) if pd.notna(row.short_avg) else None

        score = 0

        if avg and avg > 0:
            drop = (avg - curr) / avg
            if drop > 0.25:
                score += 1
            if drop > 0.40:
                score += 2

        if inflow > 1:
            score += 1

        if inflow > 5:
            score += 2

        score += 1

        if score >= 5:
            insight = "🚀 強烈逼空信號"
        elif score >= 3:
            insight = "⚠️ 空頭平倉"
        elif inflow > 10:
            insight = "🔥 主力掃貨"
        else:
            insight = "✅ 正常"

        results.append({
            "code": row.股票代碼,
            "name": getattr(row, name_col),
            "inflow": round(inflow,2),
            "short_today": round(curr,2),
            "short_avg": round(avg,2) if avg else None,
            "insight": insight
        })

    output = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "stocks": results
    }

    with open("data.json","w",encoding="utf-8") as f:
        json.dump(output,f,ensure_ascii=False,indent=4)

    print("data.json updated")

    if results:

        msg = (
            f"📊 港股 AI 看板更新\n"
            f"時間: {output['update_time']}\n"
            f"榜首: {results[0]['name']} {results[0]['insight']}"
        )

        send_telegram(msg)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    run_analysis()
