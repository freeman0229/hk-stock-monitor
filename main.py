import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time

# 配置 GitHub Secrets
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
                s_col = [c for c in df.columns if '比率' in c or '比例' in c][0]
                df['股票代碼'] = df[c_col].astype(str).str.zfill(5)
                short_histories.append(df[['股票代碼', s_col]].rename(columns={s_col: '沽空比率'}))
                print(f"📊 成功獲取 {date_str} 歷史數據")
        except: pass
        target_date -= datetime.timedelta(days=1)
        attempts += 1
    return pd.concat(short_histories).groupby('股票代碼')['沽空比率'].mean().reset_index().rename(columns={'沽空比率': 'short_avg'}) if short_histories else pd.DataFrame()

def run_analysis():
    print("🚀 啟動數據掃描...")
    # 1. 行情數據
    df_all = ak.stock_hk_spot_em()
    t_col = [c for c in df_all.columns if '成交额' in c or '成交金额' in c][0]
    c_col = [c for c in df_all.columns if '代码' in c or '代碼' in c][0]
    n_col = [c for c in df_all.columns if '名称' in c or '名稱' in c][0]
    
    df_all[t_col] = pd.to_numeric(df_all[t_col], errors='coerce')
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)
    df_all['股票代碼'] = df_all[c_col].astype(str).str.zfill(5)
    print(f"✅ 行情抓取完成: {len(df_all)} 條")

    # 2. 南向數據
    df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])
    try:
        df_gt_raw = pd.concat([ak.stock_hk_ggt_board_em(symbol="滬港通"), ak.stock_hk_ggt_board_em(symbol="深港通")])
        gc_col = [c for c in df_gt_raw.columns if '代' in c][0]
        gb_col = [c for c in df_gt_raw.columns if '买入' in c or '買入' in c][0]
        gs_col = [c for c in df_gt_raw.columns if '卖出' in c or '賣出' in c][0]
        df_gt_raw['股票代碼'] = df_gt_raw[gc_col].astype(str).str.zfill(5)
        df_gt_raw['net_inflow'] = (pd.to_numeric(df_gt_raw[gb_col]) - pd.to_numeric(df_gt_raw[gs_col])) / 1e8
        df_gt = df_gt_raw[['股票代碼', 'net_inflow']].drop_duplicates(subset=['股票代碼'])
        print(f"✅ 南向資金抓取完成: {len(df_gt)} 條")
    except: print("⚠️ 今日南向數據尚未更新")

    # 3. 今日沽空
    df_short_today = pd.DataFrame(columns=['股票代碼', 'short_today'])
    try:
        df_short_raw = ak.stock_hksell_summary()
        sc_col = [c for c in df_short_raw.columns if '代' in c][0]
        ss_col = [c for c in df_short_raw.columns if '比率' in c or '比例' in c][0]
        df_short_raw['股票代碼'] = df_short_raw[sc_col].astype(str).str.zfill(5)
        df_short_today = df_short_raw[['股票代碼', ss_col]].rename(columns={ss_col: 'short_today'})
        print(f"✅ 今日沽空抓取完成: {len(df_short_today)} 條")
    except: print("⚠️ 今日沽空數據尚未更新")

    # 4. 整合
    df_f = pd.merge(df_all[['股票代碼', n_col]], df_gt, on='股票代碼', how='left')
    df_f = pd.merge(df_f, df_short_today, on='股票代碼', how='left')
    df_avg = get_historical_short_avg(5)
    
    if not df_avg.empty:
        df_f = pd.merge(df_f, df_avg, on='股票代碼', how='left')
    else:
        df_f['short_avg'] = 0
    
    df_f = df_f.fillna(0).head(30)

    # 5. 結果生成與排名
    old_ranks = {}
    if os.path.exists('data.json'):
        try:
            with open('data.json', 'r', encoding='utf-8') as f:
                prev = json.load(f).get('stocks', [])
                old_ranks = {s['code']: i for i, s in enumerate(prev)}
        except: pass

    final_results = []
    for i, (_, row) in enumerate(df_f.iterrows()):
        code, name = row['股票代碼'], row[n_col]
        inflow, curr_s, avg_s = float(row['net_inflow']), float(row.get('short_today', 0)), float(row.get('short_avg', 0))
        
        insight = "✅ 目前無重大異動"
        if avg_s > 0 and curr_s < (avg_s * 0.75) and inflow > 1.5: insight = "⚠️ 空頭平倉"
        elif inflow > 10: insight = "🔥 主力掃貨"

        final_results.append({
            "code": code, "name": name, "inflow": round(inflow, 2),
            "short_today": round(curr_s, 2), "short_avg": round(avg_s, 2),
            "insight": insight, "rank_change": old_ranks.get(code, i) - i
        })

    # 6. 儲存
    output = {"update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "stocks": final_results}
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    print("🎉 data.json 更新成功！")

    # 7. Telegram 推送 (強制發送，用於確認)
    if TELEGRAM_TOKEN and CHAT_ID:
        url = f"https://api.telegram.org{TELEGRAM_TOKEN}/sendMessage"
        msg = f"📊 *港股 AI 看板更新成功*\n時間: {output['update_time']}\n榜首: {final_results[0]['name']}\n"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)

if __name__ == "__main__":
    run_analysis()
