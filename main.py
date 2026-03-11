import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time
import random

# ============================
# Global Settings / Secrets
# ============================
requests.adapters.DEFAULT_RETRIES = 5
TELEGRAM_TOKEN = str(os.getenv("TELEGRAM_TOKEN", "")).strip()
CHAT_ID = str(os.getenv("CHAT_ID", "")).strip()

# ============================
# Safe HK Market Fetch
# ============================
def safe_hk_spot(retries=4, timeout=8):
    """Fetch HK stock spot data with retries and timeout."""
    url = "https://push2ex.eastmoney.com/api/default/getHKStockList"
    params = {
        "cb": "",
        "pageSize": "300",
        "pageIndex": "1",
        "source": "web",
        "quoteType": "1",
        "_": int(time.time() * 1000),
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    for attempt in range(1, retries + 1):
        try:
            print(f"🔄 Fetching HK market data (attempt {attempt})")
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if "data" not in data or "list" not in data["data"]:
                raise Exception("Unexpected response format")
            raw = data["data"]["list"]
            df = pd.DataFrame(raw)
            df = df.rename(columns={"f12": "名称", "f13": "代码", "f14": "成交额"})
            df["股票代碼"] = df["代码"].astype(str).str.zfill(5)
            print(f"✅ Eastmoney data loaded: {len(df)} rows")
            return df
        except Exception as e:
            print(f"⚠️ Fetch failed: {e}")
            time.sleep(random.randint(3, 7))
    print("❌ Failed to fetch HK stock data after retries")
    return pd.DataFrame()

# ============================
# Historical Short Sell Average
# ============================
def get_historical_short_avg(days=5):
    short_histories = []
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    attempts = 0
    while len(short_histories) < days and attempts < 12:
        date_str = target_date.strftime("%Y%m%d")
        try:
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                c_col = [c for c in df.columns if "代" in c][0]
                s_col = [c for c in df.columns if "比率" in c or "比例" in c][0]
                df["股票代碼"] = df[c_col].astype(str).str.zfill(5)
                short_histories.append(
                    df[["股票代碼", s_col]].rename(columns={s_col: "short_avg"})
                )
        except Exception as e:
            print(f"⚠️ Historical short data error: {e}")
        target_date -= datetime.timedelta(days=1)
        attempts += 1
        time.sleep(1)
    if not short_histories:
        return pd.DataFrame(columns=["股票代碼", "short_avg"])
    return (
        pd.concat(short_histories)
        .groupby("股票代碼")["short_avg"]
        .mean()
        .reset_index()
    )

# ============================
# Main Analysis
# ============================
def run_analysis():
    print("🚀 啟動 AI 深度分析...")
    df_all = safe_hk_spot()
    if df_all.empty:
        print("❌ No market data retrieved. Skipping analysis.")
        return

    t_col = [c for c in df_all.columns if "成交额" in c or "成交金额" in c][0]
    c_col = [c for c in df_all.columns if "代码" in c or "代碼" in c][0]
    n_col = [c for c in df_all.columns if "名称" in c or "名稱" in c][0]

    df_all[t_col] = pd.to_numeric(df_all[t_col], errors="coerce")
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)
    df_all["股票代碼"] = df_all[c_col].astype(str).str.zfill(5)

    # =============================
    # Southbound Capital
    # =============================
    try:
        df_gt_raw = pd.concat([
            ak.stock_hk_ggt_board_em(symbol="滬港通"),
            ak.stock_hk_ggt_board_em(symbol="深港通")
        ])
        gc_col = [c for c in df_gt_raw.columns if "代" in c][0]
        gb_col = [c for c in df_gt_raw.columns if "买入" in c or "買入" in c][0]
        gs_col = [c for c in df_gt_raw.columns if "卖出" in c or "賣出" in c][0]
        df_gt_raw["股票代碼"] = df_gt_raw[gc_col].astype(str).str.zfill(5)
        df_gt_raw["net_inflow"] = (
            pd.to_numeric(df_gt_raw[gb_col], errors="coerce") - pd.to_numeric(df_gt_raw[gs_col], errors="coerce")
        ) / 1e8
        df_gt = df_gt_raw[["股票代碼", "net_inflow"]].drop_duplicates()
        print(f"✅ 南向資金數據: {len(df_gt)} 筆")
    except Exception as e:
        print(f"⚠️ Southbound data error: {e}")
        df_gt = pd.DataFrame(columns=["股票代碼", "net_inflow"])

    # =============================
    # Today's Short Sell
    # =============================
    try:
        df_short_raw = ak.stock_hksell_summary()
        sc_col = [c for c in df_short_raw.columns if "代" in c][0]
        ss_col = [c for c in df_short_raw.columns if "比率" in c or "比例" in c][0]
        df_short_raw["股票代碼"] = df_short_raw[sc_col].astype(str).str.zfill(5)
        df_short_today = df_short_raw[["股票代碼", ss_col]].rename(columns={ss_col: "short_today"})
    except Exception as e:
        print(f"⚠️ Short sell data error: {e}")
        df_short_today = pd.DataFrame(columns=["股票代碼", "short_today"])

    # =============================
    # Merge Data
    # =============================
    df_f = pd.merge(df_all[["股票代碼", n_col]], df_gt, on="股票代碼", how="left")
    df_f = pd.merge(df_f, df_short_today, on="股票代碼", how="left")
    df_avg = get_historical_short_avg(5)
    df_f = pd.merge(df_f, df_avg.rename(columns={"short_avg": "avg_s"}), on="股票代碼", how="left").fillna(0).head(30)

    # =============================
    # Build Results
    # =============================
    final_results = []
    for _, row in df_f.iterrows():
        inflow = float(row["net_inflow"])
        curr_s = float(row.get("short_today", 0))
        avg_s = float(row.get("avg_s", 0))
        insight = "✅ 正常"
        if avg_s > 0 and curr_s < (avg_s * 0.75) and inflow > 1.5:
            insight = "⚠️ 空頭平倉"
        elif inflow > 10:
            insight = "🔥 主力掃貨"
        final_results.append({
            "code": row["股票代碼"],
            "name": row[n_col],
            "inflow": round(inflow, 2),
            "short_today": round(curr_s, 2),
            "short_avg": round(avg_s, 2),
            "insight": insight,
            "rank_change": 0,
        })

    output = {"update_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "stocks": final_results}
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    print("🎉 data.json 更新成功！")

    # =============================
    # Telegram Notification
    # =============================
    if TELEGRAM_TOKEN and CHAT_ID and final_results:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            msg = f"📊 港股 AI 看板更新\n時間: {output['update_time']}\n榜首: {final_results[0]['name']}"
            res = requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=15)
            print(f"📬 Telegram status: {res.status_code}")
        except Exception as e:
            print(f"❌ Telegram push failed: {e}")


if __name__ == "__main__":
    run_analysis()
