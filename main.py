import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time

# 🚩 自動清理 Token 中的空格
TELEGRAM_TOKEN = str(os.getenv('TELEGRAM_TOKEN', '')).strip().replace('bot', '')
CHAT_ID = str(os.getenv('CHAT_ID', '')).strip()

def get_historical_short_avg(days=5):
    short_histories = []
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    attempts = 0
    while len(short_histories) < days and attempts < 10:
        date_str = target_date.strftime('%Y%m%d')
        try:
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                c_col = [c for c in df.columns if '代' in c][0]
                df['股票代碼'] = df[c_col].astype(str).str.zfill(5)
                short_histories.append(df[['股票代碼', '沽空比率']])
        except: pass
        target_date -= datetime.timedelta(days=1)
        attempts += 1
    return pd.concat(short_histories).groupby('股票代碼')['沽空比率'].mean().reset_index() if short_histories else pd.DataFrame()

def run_analysis():
    print("🚀 啟動數據掃描...")
    df_all = ak.stock_hk_spot_em()
    if df_all.empty: return

    # 自動識別欄位
    t_col = [c for c in df_all.columns if '成交额' in c or '成交金额' in c][0]
    c_col = [c for c in df_all.columns if '代码' in c or '代碼' in c][0]
    n_col = [c for c in df_all.columns if '名称' in c or '名稱' in c][0]
    
    df_all[t_col] = pd.to_numeric(df_all[t_col], errors='coerce')
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)
    df_all['股票代碼'] = df_all[c_col].astype(str).str.zfill(5)

    # 獲取南向與沽空 (加入空值填充防止 0)
    try:
        df_gt_raw = pd.concat([ak.stock_hk_ggt_board_em(symbol="滬港通"), ak.stock_hk_ggt_board_em(symbol="深港通")])
        gc_col = [c for c in df_gt_raw.columns if '代' in c][0]
        gb_col = [c for c in df_gt_raw.columns if '买入' in c or '買入' in c][0]
        gs_col = [c for c in df_gt_raw.columns if '卖出' in c or '賣出' in c][0]
        df_gt_raw['股票代碼'] = df_gt_raw[gc_col].astype(str).str.zfill(5)
        df_gt_raw['net_inflow'] = (pd.to_numeric(df_gt_raw[gb_col]) - pd.to_numeric(df_gt_raw[gs_col])) / 1e8
        df_gt = df_gt_raw[['股票代碼', 'net_inflow']].drop_duplicates(subset=['股票代碼'])
    except: df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])

    try:
        df_short_raw = ak.stock_hksell_summary()
        sc_col = [c for c in df_short_raw.columns if '代' in c][0]
        df_short_raw['股票代碼'] = df_short_raw[sc_col].astype(str).str.zfill(5)
        df_short_today = df_short_raw[['股票代碼', '沽空比率']]
    except: df_short_today = pd.DataFrame(columns=['股票代碼', '沽空比率'])

    # 整合與分析
    df_f = pd.merge(df_all[['股票代碼', n_col]], df_gt, on='股票代碼', how='left').fillna(0)
    df_f = pd.merge(df_f, df_short_today, on='股票代碼', how='left').fillna(0)
    df_avg = get_historical_short_avg(5)
    df_f = pd.merge(df_f, df_avg.rename(columns={'avg_short_ratio': 'short_avg'}), on='股票代碼', how='left').fillna(0).head(30)

    # 生成結果
    final_results = []
    for i, (_, row) in enumerate(df_f.iterrows()):
        insight = "✅ 正常"
        if row.get('short_avg', 0) > 0 and row['沽空比率'] < (row['short_avg'] * 0.75) and row['net_inflow'] > 1.5:
            insight = "⚠️ 空頭平倉"
        elif row['net_inflow'] > 10: insight = "🔥 主力掃貨"

        final_results.append({
            "code": row['股票代碼'], "name": row[n_col], "inflow": round(row['net_inflow'], 2),
            "short_today": round(row['沽空比率'], 2), "short_avg": round(row.get('short_avg', 0), 2),
            "insight": insight
        })

    # 儲存 JSON
    output = {"update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "stocks": final_results}
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    print("✅ data.json 更新成功")

    # Telegram 推送
    if TELEGRAM_TOKEN and CHAT_ID:
        url = f"https://api.telegram.org{TELEGRAM_TOKEN}/sendMessage"
        msg = f"📊 *港股 AI 看板更新成功*\n" + "\n".join([f"{s['name']}: {s['insight']} (入:{s['inflow']}億)" for s in final_results[:10]])
        res = requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"})
        print(f"📬 Telegram 狀態: {res.status_code}")

if __name__ == "__main__":
    run_analysis()
