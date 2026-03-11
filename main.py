import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time

TELEGRAM_TOKEN = str(os.getenv('TELEGRAM_TOKEN', '')).strip()
CHAT_ID = str(os.getenv('CHAT_ID', '')).strip()


def get_historical_short_avg(days=5):
    short_histories = []
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    attempts = 0

    while len(short_histories) < days and attempts < 12:
        date_str = target_date.strftime('%Y%m%d')

        try:
            df = ak.stock_hksell_summary(date=date_str)

            if df.empty:
                target_date -= datetime.timedelta(days=1)
                attempts += 1
                continue

            c_col = [c for c in df.columns if '代' in c]
            s_col = [c for c in df.columns if '比率' in c or '比例' in c]

            if not c_col or not s_col:
                continue

            c_col = c_col[0]
            s_col = s_col[0]

            df['股票代碼'] = df[c_col].astype(str).str.zfill(5)

            short_histories.append(
                df[['股票代碼', s_col]].rename(columns={s_col: 'short_avg'})
            )

        except Exception as e:
            print("Short history error:", e)

        target_date -= datetime.timedelta(days=1)
        attempts += 1
        time.sleep(1)

    if not short_histories:
        return pd.DataFrame(columns=["股票代碼", "short_avg"])

    return pd.concat(short_histories).groupby('股票代碼')['short_avg'].mean().reset_index()


def run_analysis():
    print("🚀 啟動 AI 深度分析...")

    df_all = ak.stock_hk_spot_em()

    t_col = [c for c in df_all.columns if '成交额' in c or '成交金额' in c][0]
    c_col = [c for c in df_all.columns if '代码' in c or '代碼' in c][0]
    n_col = [c for c in df_all.columns if '名称' in c or '名稱' in c][0]

    df_all[t_col] = pd.to_numeric(df_all[t_col], errors='coerce')
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)

    df_all['股票代碼'] = df_all[c_col].astype(str).str.zfill(5)

    try:
        df_gt_raw = pd.concat([
            ak.stock_hk_ggt_board_em(symbol="滬港通"),
            ak.stock_hk_ggt_board_em(symbol="深港通")
        ])

        gc_col = [c for c in df_gt_raw.columns if '代' in c][0]
        gb_col = [c for c in df_gt_raw.columns if '买入' in c or '買入' in c][0]
        gs_col = [c for c in df_gt_raw.columns if '卖出' in c or '賣出' in c][0]

        df_gt_raw['股票代碼'] = df_gt_raw[gc_col].astype(str).str.zfill(5)

        df_gt_raw['net_inflow'] = (
            pd.to_numeric(df_gt_raw[gb_col], errors='coerce')
            - pd.to_numeric(df_gt_raw[gs_col], errors='coerce')
        ) / 1e8

        df_gt = df_gt_raw[['股票代碼', 'net_inflow']].drop_duplicates()

    except Exception as e:
        print("Southbound data error:", e)
        df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])

    try:
        df_short_raw = ak.stock_hksell_summary()

        sc_col = [c for c in df_short_raw.columns if '代' in c][0]
        ss_col = [c for c in df_short_raw.columns if '比率' in c or '比例' in c][0]

        df_short_raw['股票代碼'] = df_short_raw[sc_col].astype(str).str.zfill(5)

        df_short_today = df_short_raw[['股票代碼', ss_col]].rename(
            columns={ss_col: 'short_today'}
        )

    except Exception as e:
        print("Short sell error:", e)
        df_short_today = pd.DataFrame(columns=['股票代碼', 'short_today'])

    df_f = pd.merge(df_all[['股票代碼', n_col]], df_gt, on='股票代碼', how='left')
    df_f = pd.merge(df_f, df_short_today, on='股票代碼', how='left')

    df_avg = get_historical_short_avg(5)

    df_f = pd.merge(
        df_f,
        df_avg.rename(columns={'short_avg': 'avg_s'}),
        on='股票代碼',
        how='left'
    ).fillna(0).head(30)

    final_results = []

    for _, row in df_f.iterrows():
        inflow = float(row['net_inflow'])
        curr_s = float(row.get('short_today', 0))
        avg_s = float(row.get('avg_s', 0))

        insight = "✅ 正常"

        if avg_s > 0 and curr_s < avg_s * 0.75 and inflow > 1.5:
            insight = "⚠️ 空頭平倉"
        elif inflow > 10:
            insight = "🔥 主力掃貨"

        final_results.append({
            "code": row['股票代碼'],
            "name": row[n_col],
            "inflow": round(inflow, 2),
            "short_today": round(curr_s, 2),
            "short_avg": round(avg_s, 2),
            "insight": insight
        })

    output = {
        "update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
        "stocks": final_results
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    print("🎉 data.json 更新成功")

    if TELEGRAM_TOKEN and CHAT_ID and final_results:

        top_stock = final_results[0]['name']

        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

        msg = f"📊 港股 AI 看板更新\n時間: {output['update_time']}\n榜首: {top_stock}"

        res = requests.post(
            url,
            data={
                "chat_id": CHAT_ID,
                "text": msg
            },
            timeout=15
        )

        print(res.text)


if __name__ == "__main__":
    run_analysis()
