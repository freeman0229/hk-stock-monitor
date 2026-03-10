import akshare as ak
import pandas as pd
import requests
import json
import os
import datetime
import time

# 配置 GitHub Secrets
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '').strip()
CHAT_ID = os.getenv('CHAT_ID', '').strip()

def get_historical_short_avg(days=5):
    """獲取過去5個交易日的平均沽空率 (包含容錯)"""
    short_histories = []
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    attempts = 0
    while len(short_histories) < days and attempts < 12:
        date_str = target_date.strftime('%Y%m%d')
        try:
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                # 統一代碼格式
                code_col = '股票代碼' if '股票代碼' in df.columns else '代码'
                df['股票代碼'] = df[code_col].astype(str).str.zfill(5)
                short_histories.append(df[['股票代碼', '沽空比率']])
        except: pass
        target_date -= datetime.timedelta(days=1)
        attempts += 1
    
    if not short_histories: 
        return pd.DataFrame(columns=['股票代碼', 'avg_short_ratio'])
    
    avg_df = pd.concat(short_histories).groupby('股票代碼')['沽空比率'].mean().reset_index()
    avg_df.columns = ['股票代碼', 'avg_short_ratio']
    return avg_df

def run_analysis():
    print("🚀 開始執行深度市場分析...")
    # 1. 抓取今日成交 Top 40
    df_all = ak.stock_hk_spot_em()
    target_col = "成交额" if "成交额" in df_all.columns else "成交金额"
    code_col = "代码" if "代码" in df_all.columns else "代碼"
    name_col = "名称" if "名称" in df_all.columns else "名稱"
    
    df_all = df_all.sort_values(by=target_col, ascending=False).head(40)
    df_all['股票代碼'] = df_all[code_col].astype(str).str.zfill(5)

    # 2. 獲取南向資金 (強制獲取)
    try:
        df_gt_sh = ak.stock_hk_ggt_board_em(symbol="滬港通")
        df_gt_sz = ak.stock_hk_ggt_board_em(symbol="深港通")
        df_gt = pd.concat([df_gt_sh, df_gt_sz])
        c_col = "代码" if "代码" in df_gt.columns else "代碼"
        df_gt['股票代碼'] = df_gt[c_col].astype(str).str.zfill(5)
        
        b_col = "买入金额" if "买入金额" in df_gt.columns else "買入金額"
        s_col = "卖出金额" if "卖出金额" in df_gt.columns else "賣出金額"
        df_gt['net_inflow'] = (pd.to_numeric(df_gt[b_col]) - pd.to_numeric(df_gt[s_col])) / 1e8
        df_gt = df_gt[['股票代碼', 'net_inflow']].drop_duplicates(subset=['股票代碼'])
    except:
        df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])

    # 3. 獲取當日沽空
    try:
        df_short_today = ak.stock_hksell_summary()
        c_col_s = '股票代碼' if '股票代碼' in df_short_today.columns else '代码'
        df_short_today['股票代碼'] = df_short_today[c_col_s].astype(str).str.zfill(5)
    except:
        df_short_today = pd.DataFrame(columns=['股票代碼', '沽空比率'])
        
    df_avg = get_historical_short_avg(5)

    # 4. 數據整合 (使用 Left Join 確保 Top 30 核心不丟失)
    df_final = pd.merge(df_all[['股票代碼', name_col]], df_gt, on='股票代碼', how='left')
    df_final = pd.merge(df_final, df_short_today[['股票代碼', '沽空比率']], on='股票代碼', how='left')
    df_final = pd.merge(df_final, df_avg, on='股票代碼', how='left').head(30)

    # 5. 排名邏輯
    old_ranks = {}
    if os.path.exists('data.json'):
        try:
            with open('data.json', 'r', encoding='utf-8') as f:
                old_ranks = {s['code']: i for i, s in enumerate(json.load(f).get('stocks', []))}
        except: pass

    final_results = []
    for i, (_, row) in enumerate(df_final.iterrows()):
        code, name = row['股票代碼'], row[name_col]
        inflow = row.get('net_inflow', 0)
        curr_s = row.get('沽空比率', 0)
        avg_s = row.get('avg_short_ratio', 0)
        
        inflow = 0 if pd.isna(inflow) else inflow
        curr_s = 0 if pd.isna(curr_s) else curr_s
        avg_s = 0 if pd.isna(avg_s) else avg_s
        
        insight = "✅ 正常"
        if avg_s > 0 and curr_s < (avg_s * 0.75) and inflow > 1.5: insight = "⚠️ 空頭平倉"
        elif inflow > 10: insight = "🔥 主力掃貨"

        final_results.append({
            "code": code, "name": name, "inflow": round(inflow, 2),
            "short_today": round(curr_s, 2), "short_avg": round(avg_s, 2),
            "insight": insight, "is_new": code not in old_ranks,
            "rank_change": old_ranks.get(code, i) - i
        })

    # 6. 儲存 JSON
    output = {"update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "stocks": final_results}
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    print("✅ data.json 儲存成功")

    # 7. Telegram 強制發送 (Heartbeat 模式)
    if TELEGRAM_TOKEN and CHAT_ID:
        try:
            clean_token = TELEGRAM_TOKEN.replace('bot', '').strip()
            url = f"https://api.telegram.org{clean_token}/sendMessage"
            
            # 組裝訊息 (包含前 15 名)
            msg = f"📊 *港股 Top 30 監測報告*\n更新時間: {output['update_time']}\n"
            msg += "\n".join([f"{s['name']}: {s['insight']} (入:{s['inflow']}億)" for s in final_results[:15]])
            
            requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=15)
            print("📬 Telegram 訊息已發出")
        except Exception as e:
            print(f"⚠️ 推送失敗: {e}")

if __name__ == "__main__":
    run_analysis()
