import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time

# 配置 GitHub Secrets (從環境變量讀取)
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '').strip()
CHAT_ID = os.getenv('CHAT_ID', '').strip()

def get_historical_short_avg(days=5):
    """獲取過去5個交易日的平均沽空率"""
    short_histories = []
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    attempts = 0
    while len(short_histories) < days and attempts < 12:
        date_str = target_date.strftime('%Y%m%d')
        try:
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                df['股票代碼'] = df['股票代碼'].str.zfill(5)
                short_histories.append(df[['股票代碼', '沽空比率']])
        except: pass
        target_date -= datetime.timedelta(days=1)
        attempts += 1
        time.sleep(0.5)
    
    if not short_histories: 
        return pd.DataFrame(columns=['股票代碼', 'avg_short_ratio'])
    
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

    # 2. 獲取南向資金
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

    # 3. 獲取當日沽空數據
    try:
        df_short_today = ak.stock_hksell_summary()
        df_short_today['股票代碼'] = df_short_today['股票代碼'].str.zfill(5)
    except:
        df_short_today = pd.DataFrame(columns=['股票代碼', '沽空比率'])
        
    df_avg = get_historical_short_avg(5)

    # 4. 數據整合 (安全檢查)
    df_m = pd.merge(df_all[['股票代碼', name_col]], df_gt[['股票代碼', 'net_inflow']], on='股票代碼', how='left')
    
    if not df_short_today.empty:
        df_m = pd.merge(df_m, df_short_today[['股票代碼', '沽空比率']], on='股票代碼', how='left')
    else:
        df_m['沽空比率'] = None

    if not df_avg.empty and '股票代碼' in df_avg.columns:
        df_f = pd.merge(df_m, df_avg, on='股票代碼', how='left').head(30)
    else:
        df_f = df_m.copy()
        df_f['avg_short_ratio'] = None
        df_f = df_f.head(30)

    # 5. 排名變動邏輯
    old_ranks = {}
    if os.path.exists('data.json'):
        try:
            with open('data.json', 'r', encoding='utf-8') as f:
                old_json = json.load(f)
                prev_stocks = old_json.get('stocks', [])
                old_ranks = {s['code']: i for i, s in enumerate(prev_stocks)}
        except: pass

    final_results = []
    for i, (_, row) in enumerate(df_f.iterrows()):
        code, name = row['股票代碼'], row[name_col]
        inflow = row['net_inflow'] if not pd.isna(row['net_inflow']) else 0
        curr_s = row['沽空比率'] if not pd.isna(row['沽空比率']) else 0
        avg_s = row['avg_short_ratio'] if not pd.isna(row['avg_short_ratio']) else curr_s
        
        insight = "✅ 正常"
        if avg_s and avg_s > 0:
            if curr_s < (avg_s * 0.75) and inflow > 1.5: insight = "⚠️ 空頭平倉"
            elif curr_s > (avg_s * 1.4): insight = "⚡ 沽空激增"
        if inflow > 10: insight = "🔥 主力掃貨"

        final_results.append({
            "code": code, "name": name, "inflow": round(inflow, 2),
            "short_today": round(curr_s, 2) if curr_s else 0, 
            "short_avg": round(avg_s, 2) if avg_s else 0,
            "insight": insight, "is_new": code not in old_ranks,
            "rank_change": old_ranks.get(code, i) - i
        })

    # 6. 儲存 JSON (🚩 確保這一步先於 Telegram 執行)
    output = {"update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "stocks": final_results}
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    print("✅ data.json 更新成功！")

    # 7. Telegram 推送 (🚩 加入 URL 安全檢查)
    if TELEGRAM_TOKEN and CHAT_ID:
        try:
            # 安全構建 URL，移除多餘空格與 bot 前綴檢查
            token = TELEGRAM_TOKEN.replace('bot', '') 
            url = f"https://api.telegram.org{token}/sendMessage"
            
            msg = f"📊 *港股 Top 30 策略報告*\n" + "\n".join([f"{s['name']}: {s['insight']} (入:{s['inflow']}億)" for s in final_results[:12]])
            res = requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
            print(f"Telegram 推送狀態碼: {res.status_code}")
        except Exception as e:
            print(f"⚠️ Telegram 推送失敗，但不影響數據儲存: {e}")
    else:
        print("ℹ️ 跳過 Telegram 推送 (Token 或 ID 為空)")

if __name__ == "__main__":
    run_analysis()
