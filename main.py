import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time

# 配置 GitHub Secrets
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

def get_historical_short_avg(days=5):
    """獲取過去5個交易日的平均沽空率 (修正函數名為 stock_hksell_summary)"""
    short_histories = []
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    attempts = 0
    while len(short_histories) < days and attempts < 12:
        date_str = target_date.strftime('%Y%m%d')
        try:
            # 🚩 修正後的最新 AkShare 接口名
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                df['股票代碼'] = df['股票代碼'].str.zfill(5)
                short_histories.append(df[['股票代碼', '沽空比率']])
        except: pass
        target_date -= datetime.timedelta(days=1)
        attempts += 1
        time.sleep(0.5)
    if not short_histories: return pd.DataFrame()
    avg_df = pd.concat(short_histories).groupby('股票代碼')['沽空比率'].mean().reset_index()
    avg_df.columns = ['股票代碼', 'avg_short_ratio']
    return avg_df

def run_analysis():
    # 1. 抓取今日成交 Top 30
    df_all = ak.stock_hk_spot_em()
    target_col = "成交额" if "成交额" in df_all.columns else "成交金额"
    code_col = "代码" if "代码" in df_all.columns else "代碼"
    name_col = "名称" if "名称" in df_all.columns else "名稱"
    
    df_all = df_all.sort_values(by=target_col, ascending=False).head(40)
    df_all['股票代碼'] = df_all[code_col].astype(str).str.zfill(5)

    # 2. 獲取南向資金 (修正後的活躍股接口)
    try:
        df_gt_sh = ak.stock_hk_ggt_board_em(symbol="滬港通")
        df_gt_sz = ak.stock_hk_ggt_board_em(symbol="深港通")
        df_gt = pd.concat([df_gt_sh, df_gt_sz]).drop_duplicates(subset=['代码'])
        
        b_col = "买入金额" if "买入金额" in df_gt.columns else "買入金額"
        s_col = "卖出金额" if "卖出金额" in df_gt.columns else "賣出金額"
        c_col = "代码" if "代码" in df_gt.columns else "代碼"
        
        df_gt['net_inflow'] = (df_gt[b_col] - df_gt[s_col]) / 1e8
        df_gt['股票代碼'] = df_gt[c_col].astype(str).str.zfill(5)
    except:
        df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])

    # 3. 獲取當日沽空數據 (🚩 修正後的最新接口名)
    try:
        df_short_today = ak.stock_hksell_summary()
        df_short_today['股票代碼'] = df_short_today['股票代碼'].str.zfill(5)
    except:
        df_short_today = pd.DataFrame(columns=['股票代碼', '沽空比率'])
        
    df_avg = get_historical_short_avg(5)

    # 4. 數據大整合
    df_m = pd.merge(df_all[['股票代碼', name_col]], df_gt[['股票代碼', 'net_inflow']], on='股票代碼', how='left')
    df_m = pd.merge(df_m, df_short_today[['股票代碼', '沽空比率']], on='股票代碼', how='left')
    df_f = pd.merge(df_m, df_avg, on='股票代碼', how='left').head(30)

    # 5. 排名變動邏輯 (讀取昨日排名)
    old_ranks = {}
    if os.path.exists('data.json'):
        try:
            with open('data.json', 'r', encoding='utf-8') as f:
                prev = json.load(f).get('stocks', [])
                old_ranks = {s['code']: i for i, s in enumerate(prev)}
        except: pass

    final_results = []
    for i, (_, row) in enumerate(df_f.iterrows()):
        code, name = row['股票代碼'], row[name_col]
        inflow = row['net_inflow'] if not pd.isna(row['net_inflow']) else 0
        curr_s = row['沽空比率'] if not pd.isna(row['沽空比率']) else 0
        avg_s = row['avg_short_ratio'] if not pd.isna(row['avg_short_ratio']) else curr_s
        
        insight = "✅ 正常"
        if curr_s < (avg_s * 0.75) and inflow > 1.5: insight = "⚠️ 空頭平倉"
        elif curr_s > (avg_s * 1.4): insight = "⚡ 沽空激增"
        if inflow > 10: insight = "🔥 主力掃貨"

        final_results.append({
            "code": code, "name": name, "inflow": round(inflow, 2),
            "short_today": round(curr_s, 2), "short_avg": round(avg_s, 2),
            "insight": insight, "is_new": code not in old_ranks,
            "rank_change": old_ranks.get(code, i) - i
        })

    # 6. 儲存 JSON (供網頁讀取)
    output = {"update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "stocks": final_results}
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    # 7. Telegram 推送 (🚩 修正後的 URL 格式)
    if TELEGRAM_TOKEN and CHAT_ID:
        msg = f"📊 *港股 Top 30 策略報告*\n" + "\n".join([f"{s['name']}: {s['insight']} (入:{s['inflow']}億)" for s in final_results[:12]])
        url = f"https://api.telegram.org{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"})

if __name__ == "__main__":
    run_analysis()
