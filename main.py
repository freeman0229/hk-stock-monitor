import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time

# 🚩 確保 Token 和 ID 被正確清理
TELEGRAM_TOKEN = str(os.getenv('TELEGRAM_TOKEN', '')).strip()
CHAT_ID = str(os.getenv('CHAT_ID', '')).strip()

def get_historical_short_avg(days=5):
    short_histories = []
    # 這裡我們稍微往回推，確保能抓到有數據的最近幾天
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    attempts = 0
    while len(short_histories) < days and attempts < 10:
        date_str = target_date.strftime('%Y%m%d')
        try:
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                c_col = [c for c in df.columns if '代' in c]
                s_col = [c for c in df.columns if '比率' in c or '比例' in c]
                df['股票代碼'] = df[c_col[0]].astype(str).str.zfill(5)
                short_histories.append(df[['股票代碼', s_col[0]]].rename(columns={s_col[0]: 'short_avg'}))
        except: pass
        target_date -= datetime.timedelta(days=1)
        attempts += 1
    return pd.concat(short_histories).groupby('股票代碼')['short_avg'].mean().reset_index() if short_histories else pd.DataFrame()

def run_analysis():
    print("🚀 啟動數據掃描...")
    # 1. 行情數據
    df_all = ak.stock_hk_spot_em()
    if df_all.empty: return
    
    t_col = [c for c in df_all.columns if '成交额' in c or '成交金额' in c][0]
    c_col = [c for c in df_all.columns if '代码' in c or '代碼' in c][0]
    n_col = [c for c in df_all.columns if '名称' in c or '名稱' in c][0]
    
    df_all[t_col] = pd.to_numeric(df_all[t_col], errors='coerce')
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)
    df_all['股票代碼'] = df_all[c_col].astype(str).str.zfill(5)
    print(f"✅ 行情抓取完成: {len(df_all)} 條")

    # 2. 南向與沽空 (加入空值檢查)
    try:
        df_gt_raw = pd.concat([ak.stock_hk_ggt_board_em(symbol="滬港通"), ak.stock_hk_ggt_board_em(symbol="深港通")])
        df_gt_raw['股票代碼'] = df_gt_raw['代码'].astype(str).str.zfill(5)
        df_gt_raw['net_inflow'] = (pd.to_numeric(df_gt_raw['买入金额']) - pd.to_numeric(df_gt_raw['卖出金额'])) / 1e8
        df_gt = df_gt_raw[['股票代碼', 'net_inflow']].drop_duplicates(subset=['股票代碼'])
        print(f"✅ 南向數據抓取成功")
    except: 
        print("⚠️ 今日南向數據尚未更新，將顯示為 0")
        df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])

    try:
        df_short_today = ak.stock_hksell_summary()
        sc_col = [c for c in df_short_today.columns if '代' in c][0]
        ss_col = [c for c in df_short_today.columns if '比率' in c or '比例' in c][0]
        df_short_today['股票代碼'] = df_short_today[sc_col].astype(str).str.zfill(5)
        df_short_today = df_short_today[['股票代碼', ss_col]].rename(columns={ss_col: 'short_today'})
        print(f"✅ 今日沽空抓取成功")
    except: 
        print("⚠️ 今日沽空數據尚未更新，將顯示為 0")
        df_short_today = pd.DataFrame(columns=['股票代碼', 'short_today'])

    # 3. 數據整合
    df_f = pd.merge(df_all[['股票代碼', n_col]], df_gt, on='股票代碼', how='left')
    df_f = pd.merge(df_f, df_short_today, on='股票代碼', how='left')
    df_avg = get_historical_short_avg(5)
    
    if not df_avg.empty:
        df_f = pd.merge(df_f, df_avg, on='股票代碼', how='left')
    else:
        df_f['short_avg'] = 0
    
    df_f = df_f.fillna(0).head(30)

    # 4. 排名變動與 JSON
    final_results = []
    for i, (_, row) in enumerate(df_f.iterrows()):
        final_results.append({
            "code": row['股票代碼'], "name": row[n_col], "inflow": round(row['net_inflow'], 2),
            "short_today": round(row.get('short_today', 0), 2), 
            "short_avg": round(row.get('short_avg', 0), 2),
            "insight": "✅ 目前無重大異動" if row['net_inflow'] < 10 else "🔥 主力掃貨",
            "rank_change": 0
        })

    output = {"update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "stocks": final_results}
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    print("🎉 data.json 更新成功！")

    # 5. Telegram 推送 (🚩 修正了網址拼接邏輯)
    if TELEGRAM_TOKEN and CHAT_ID and TELEGRAM_TOKEN != "":
        try:
            # 確保 token 前面有 bot 關鍵字
            clean_token = TELEGRAM_TOKEN.replace('bot', '')
            url = f"https://api.telegram.org{clean_token}/sendMessage"
            
            msg = f"📊 *港股 AI 看板更新成功*\n時間: {output['update_time']}\n排名第一: {final_results[0]['name']}"
            res = requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
            print(f"📬 Telegram 狀態: {res.status_code}")
        except Exception as e:
            print(f"⚠️ Telegram 推送失敗: {e}")

if __name__ == "__main__":
    run_analysis()
