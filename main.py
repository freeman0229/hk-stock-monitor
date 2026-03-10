import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time

# 🚩 1. 強化變數讀取與清理
TELEGRAM_TOKEN = str(os.getenv('TELEGRAM_TOKEN', '')).strip()
CHAT_ID = str(os.getenv('CHAT_ID', '')).strip()

# 偵錯用：在日誌顯示變數是否存在
print(f"DEBUG: Token Length: {len(TELEGRAM_TOKEN)}, Chat ID Length: {len(CHAT_ID)}")

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
                s_col = [c for c in df.columns if '比率' in c or '比例' in c][0]
                df['股票代碼'] = df[c_col].astype(str).str.zfill(5)
                short_histories.append(df[['股票代碼', s_col]].rename(columns={s_col: 'short_avg'}))
        except: pass
        target_date -= datetime.timedelta(days=1)
        attempts += 1
    return pd.concat(short_histories).groupby('股票代碼')['short_avg'].mean().reset_index() if short_histories else pd.DataFrame()

def run_analysis():
    print("🚀 開始深度分析...")
    # 1. 行情 Top 40
    df_all = ak.stock_hk_spot_em()
    t_col = [c for c in df_all.columns if '成交额' in c or '成交金额' in c][0]
    c_col = [c for c in df_all.columns if '代码' in c or '代碼' in c][0]
    n_col = [c for c in df_all.columns if '名称' in c or '名稱' in c][0]
    
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)
    df_all['股票代碼'] = df_all[c_col].astype(str).str.zfill(5)

    # 2. 南向資金 (北水)
    try:
        df_gt_raw = pd.concat([ak.stock_hk_ggt_board_em(symbol="滬港通"), ak.stock_hk_ggt_board_em(symbol="深港通")])
        gc_col = [c for c in df_gt_raw.columns if '代' in c][0]
        gb_col = [c for c in df_gt_raw.columns if '买入' in c or '買入' in c][0]
        gs_col = [c for c in df_gt_raw.columns if '卖出' in c or '賣出' in c][0]
        
        df_gt_raw['股票代碼'] = df_gt_raw[gc_col].astype(str).str.zfill(5)
        df_gt_raw['net_inflow'] = (pd.to_numeric(df_gt_raw[gb_col]) - pd.to_numeric(df_gt_raw[gs_col])) / 1e8
        df_gt = df_gt_raw[['股票代碼', 'net_inflow']].drop_duplicates(subset=['股票代碼'])
        print(f"✅ 抓取南向數據: {len(df_gt)} 筆")
    except: 
        df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])

    # 3. 今日沽空
    try:
        df_short_raw = ak.stock_hksell_summary()
        sc_col = [c for c in df_short_raw.columns if '代' in c][0]
        ss_col = [c for c in df_short_raw.columns if '比率' in c or '比例' in c][0]
        df_short_raw['股票代碼'] = df_short_raw[sc_col].astype(str).str.zfill(5)
        df_short_today = df_short_raw[['股票代碼', ss_col]].rename(columns={ss_col: 'short_today'})
        print(f"✅ 抓取今日沽空數據")
    except: 
        df_short_today = pd.DataFrame(columns=['股票代碼', 'short_today'])

    # 4. 數據大整合
    df_f = pd.merge(df_all[['股票代碼', n_col]], df_gt, on='股票代碼', how='left')
    df_f = pd.merge(df_f, df_short_today, on='股票代碼', how='left')
    df_avg = get_historical_short_avg(5)
    
    if not df_avg.empty:
        df_f = pd.merge(df_f, df_avg, on='股票代碼', how='left')
    else:
        df_f['short_avg'] = 0
    
    df_f = df_f.fillna(0).head(30)

    # 5. 生成 JSON
    final_results = []
    for i, (_, row) in enumerate(df_f.iterrows()):
        final_results.append({
            "code": row['股票代碼'], "name": row[n_col], "inflow": round(float(row['net_inflow']), 2),
            "short_today": round(float(row.get('short_today', 0)), 2), 
            "short_avg": round(float(row.get('short_avg', 0)), 2),
            "insight": "✅ 正常", "is_new": True, "rank_change": 0
        })

    output = {"update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "stocks": final_results}
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    print("🎉 data.json 儲存成功")

    # 🚩 6. Telegram 推送 (強化 Token 解析)
    if len(TELEGRAM_TOKEN) > 5 and len(CHAT_ID) > 5:
        try:
            token = TELEGRAM_TOKEN.replace('bot', '').strip()
            url = f"https://api.telegram.org{token}/sendMessage"
            msg = f"📊 *港股 AI 看板更新成功*\n時間: {output['update_time']}\n榜首: {final_results[0]['name']}"
            res = requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=15)
            print(f"📬 Telegram 回應碼: {res.status_code}")
        except Exception as e:
            print(f"❌ 推送出錯: {e}")
    else:
        print("⚠️ 警告：未偵測到有效的 Telegram Secrets，請檢查 GitHub 設定。")

if __name__ == "__main__":
    run_analysis()
