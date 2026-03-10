import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time

# 配置 GitHub Secrets
TELEGRAM_TOKEN = str(os.getenv('TELEGRAM_TOKEN', '')).strip()
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
                # 🚩 模糊匹配代碼欄位
                c_col = [c for c in df.columns if '代' in c][0]
                df['股票代碼'] = df[c_col].astype(str).str.zfill(5)
                short_histories.append(df[['股票代碼', '沽空比率']])
        except: pass
        target_date -= datetime.timedelta(days=1)
        attempts += 1
    return pd.concat(short_histories).groupby('股票代碼')['沽空比率'].mean().reset_index() if short_histories else pd.DataFrame()

def run_analysis():
    print("🚀 啟動數據掃描...")
    # 1. 抓取今日行情
    df_all = ak.stock_hk_spot_em()
    t_col = [c for c in df_all.columns if '成交额' in c or '成交金额' in c][0]
    c_col = [c for c in df_all.columns if '代码' in c or '代碼' in c][0]
    n_col = [c for c in df_all.columns if '名称' in c or '名稱' in c][0]
    
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)
    df_all['股票代碼'] = df_all[c_col].astype(str).str.zfill(5)

    # 2. 獲取南向資金 (活躍股)
    df_gt = pd.DataFrame()
    try:
        df_gt_sh = ak.stock_hk_ggt_board_em(symbol="滬港通")
        df_gt_sz = ak.stock_hk_ggt_board_em(symbol="深港通")
        df_gt_raw = pd.concat([df_gt_sh, df_gt_sz])
        
        # 🚩 模糊匹配買賣金額
        gc_col = [c for c in df_gt_raw.columns if '代' in c][0]
        gb_col = [c for c in df_gt_raw.columns if '买入' in c or '買入' in c][0]
        gs_col = [c for c in df_gt_raw.columns if '卖出' in c or '賣出' in c][0]
        
        df_gt_raw['股票代碼'] = df_gt_raw[gc_col].astype(str).str.zfill(5)
        df_gt_raw['net_inflow'] = (pd.to_numeric(df_gt_raw[gb_col]) - pd.to_numeric(df_gt_raw[gs_col])) / 1e8
        df_gt = df_gt_raw[['股票代碼', 'net_inflow']].drop_duplicates(subset=['股票代碼'])
    except: print("⚠️ 無法獲取南向資金數據")

    # 3. 獲取當日沽空
    df_short_today = pd.DataFrame()
    try:
        df_short_raw = ak.stock_hksell_summary()
        sc_col = [c for c in df_short_raw.columns if '代' in c][0]
        df_short_raw['股票代碼'] = df_short_raw[sc_col].astype(str).str.zfill(5)
        df_short_today = df_short_raw[['股票代碼', '沽空比率']]
    except: print("⚠️ 無法獲取今日沽空數據")

    # 4. 數據整合 (使用 Left Join 防止 0 數值)
    df_f = pd.merge(df_all[['股票代碼', n_col]], df_gt, on='股票代碼', how='left').fillna(0)
    df_f = pd.merge(df_f, df_short_today, on='股票代碼', how='left').fillna(0)
    
    df_avg = get_historical_short_avg(5)
    if not df_avg.empty:
        df_avg.columns = ['股票代碼', 'avg_short_ratio']
        df_f = pd.merge(df_f, df_avg, on='股票代碼', how='left').fillna(0)
    else:
        df_f['avg_short_ratio'] = 0

    # 5. 排名與結果生成
    old_ranks = {}
    if os.path.exists('data.json'):
        try:
            with open('data.json', 'r', encoding='utf-8') as f:
                old_ranks = {s['code']: i for i, s in enumerate(json.load(f).get('stocks', []))}
        except: pass

    final_results = []
    for i, (_, row) in enumerate(df_f.head(30).iterrows()):
        insight = "✅ 正常"
        if row.get('avg_short_ratio', 0) > 0:
            if row['沽空比率'] < (row['avg_short_ratio'] * 0.75) and row['net_inflow'] > 1.5:
                insight = "⚠️ 空頭平倉"
        if row['net_inflow'] > 10: insight = "🔥 主力掃貨"

        final_results.append({
            "code": row['股票代碼'], "name": row[n_col], "inflow": round(row['net_inflow'], 2),
            "short_today": round(row['沽空比率'], 2), "short_avg": round(row.get('avg_short_ratio', 0), 2),
            "insight": insight, "is_new": row['股票代碼'] not in old_ranks,
            "rank_change": old_ranks.get(row['股票代碼'], i) - i
        })

    # 6. 儲存 JSON
    output = {"update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "stocks": final_results}
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    print("✅ data.json 更新成功")

    # 7. 強制 Telegram 推送 (檢測心跳)
    if TELEGRAM_TOKEN and CHAT_ID:
        try:
            token = TELEGRAM_TOKEN.replace('bot', '').strip()
            url = f"https://api.telegram.org{token}/sendMessage"
            msg = f"📊 *港股 AI 看板更新成功*\n時間: {output['update_time']}\n"
            msg += "\n".join([f"{s['name']}: {s['insight']} (入:{s['inflow']}億)" for s in final_results[:12]])
            requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=15)
            print("📬 Telegram 訊息已發出")
        except Exception as e: print(f"❌ 推送失敗: {e}")

if __name__ == "__main__":
    run_analysis()
