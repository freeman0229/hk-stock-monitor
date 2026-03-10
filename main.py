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
    """獲取過去5個交易日的平均沽空率"""
    short_histories = []
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    attempts = 0
    
    while len(short_histories) < days and attempts < 10:
        date_str = target_date.strftime('%Y%m%d')
        try:
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                # 找到包含 '代' 字的列名 (股票代碼相關)
                c_col = [c for c in df.columns if '代' in c]
                if c_col:
                    df['股票代碼'] = df[c_col[0]].astype(str).str.zfill(5)
                    short_histories.append(df[['股票代碼', '沽空比率']])
        except Exception as e:
            print(f"⚠️ 獲取 {date_str} 沽空數據失敗: {e}")
        
        target_date -= datetime.timedelta(days=1)
        attempts += 1
    
    if not short_histories: 
        print("⚠️ 無法獲取歷史沽空數據")
        return pd.DataFrame(columns=['股票代碼', 'short_avg'])
    
    # 合併所有歷史數據並計算平均值
    avg_df = pd.concat(short_histories, ignore_index=True)
    avg_df = avg_df.groupby('股票代碼')['沽空比率'].mean().reset_index()
    avg_df.columns = ['股票代碼', 'short_avg']
    return avg_df

def run_analysis():
    print("🚀 啟動數據掃描...")
    
    # 1. 獲取港股行情
    try:
        df_all = ak.stock_hk_spot_em()
        if df_all.empty:
            print("❌ 無法獲取港股行情數據")
            return
    except Exception as e:
        print(f"❌ 獲取港股行情失敗: {e}")
        return

    # 找到成交額、代碼、名稱列
    t_col = [c for c in df_all.columns if '成交额' in c or '成交金额' in c or '成交額' in c]
    c_col = [c for c in df_all.columns if '代码' in c or '代碼' in c]
    n_col = [c for c in df_all.columns if '名称' in c or '名稱' in c]
    
    if not t_col or not c_col or not n_col:
        print("❌ 無法識別數據列名")
        return
    
    t_col, c_col, n_col = t_col[0], c_col[0], n_col[0]
    
    # 轉換成交額為數值並排序
    df_all[t_col] = pd.to_numeric(df_all[t_col], errors='coerce')
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)
    df_all['股票代碼'] = df_all[c_col].astype(str).str.zfill(5)

    # 2. 獲取南向資金 (滬港通 + 深港通)
    try:
        df_hgt = ak.stock_hk_ggt_board_em(symbol="滬港通")
        df_sgt = ak.stock_hk_ggt_board_em(symbol="深港通")
        df_gt_raw = pd.concat([df_hgt, df_sgt], ignore_index=True)
        
        gc_col = [c for c in df_gt_raw.columns if '代' in c][0]
        gb_col = [c for c in df_gt_raw.columns if '买入' in c or '買入' in c][0]
        gs_col = [c for c in df_gt_raw.columns if '卖出' in c or '賣出' in c][0]
        
        df_gt_raw['股票代碼'] = df_gt_raw[gc_col].astype(str).str.zfill(5)
        # 計算淨流入 (億)
        df_gt_raw['net_inflow'] = (
            pd.to_numeric(df_gt_raw[gb_col], errors='coerce') - 
            pd.to_numeric(df_gt_raw[gs_col], errors='coerce')
        ) / 1e8
        
        df_gt = df_gt_raw[['股票代碼', 'net_inflow']].drop_duplicates(subset=['股票代碼'])
        print(f"✅ 獲取南向資金數據: {len(df_gt)} 隻股票")
    except Exception as e:
        print(f"⚠️ 獲取南向資金失敗: {e}")
        df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])

    # 3. 獲取今日沽空數據
    try:
        df_short_raw = ak.stock_hksell_summary()
        sc_col = [c for c in df_short_raw.columns if '代' in c][0]
        df_short_raw['股票代碼'] = df_short_raw[sc_col].astype(str).str.zfill(5)
        df_short_today = df_short_raw[['股票代碼', '沽空比率']]
        print(f"✅ 獲取今日沽空數據: {len(df_short_today)} 隻股票")
    except Exception as e:
        print(f"⚠️ 獲取今日沽空數據失敗: {e}")
        df_short_today = pd.DataFrame(columns=['股票代碼', '沽空比率'])

    # 4. 獲取歷史平均沽空率
    df_avg = get_historical_short_avg(5)

    # 5. 數據整合
    df_f = pd.merge(df_all[['股票代碼', n_col]], df_gt, on='股票代碼', how='left')
    df_f = pd.merge(df_f, df_short_today, on='股票代碼', how='left')
    
    if not df_avg.empty and '股票代碼' in df_avg.columns:
        df_f = pd.merge(df_f, df_avg, on='股票代碼', how='left')
    else:
        df_f['short_avg'] = 0

    # 填充空值為 0
    df_f['net_inflow'] = df_f['net_inflow'].fillna(0)
    df_f['沽空比率'] = df_f['沽空比率'].fillna(0)
    df_f['short_avg'] = df_f['short_avg'].fillna(0)
    df_f = df_f.head(30)

    print(f"✅ 數據整合完成: {len(df_f)} 隻股票")

    # 6. 讀取舊排名
    old_ranks = {}
    if os.path.exists('data.json'):
        try:
            with open('data.json', 'r', encoding='utf-8') as f:
                old_json = json.load(f)
                prev_stocks = old_json.get('stocks', [])
                old_ranks = {s['code']: i for i, s in enumerate(prev_stocks)}
        except Exception as e:
            print(f"⚠️ 讀取舊數據失敗: {e}")

    # 7. 生成結果
    final_results = []
    for i, (_, row) in enumerate(df_f.iterrows()):
        code = row['股票代碼']
        name = row[n_col]
        inflow = float(row['net_inflow'])
        curr_s = float(row['沽空比率'])
        avg_s = float(row['short_avg'])
        
        # 市場洞察邏輯
        insight = "✅ 正常"
        if avg_s > 0 and curr_s < (avg_s * 0.75) and inflow > 1.5:
            insight = "⚠️ 空頭平倉"
        elif inflow > 10: 
            insight = "🔥 主力掃貨"

        # 計算排名變化
        old_rank = old_ranks.get(code, i)
        rank_change = old_rank - i

        final_results.append({
            "code": code,
            "name": name,
            "inflow": round(inflow, 2),
            "short_today": round(curr_s, 2),
            "short_avg": round(avg_s, 2),
            "insight": insight,
            "rank_change": rank_change
        })

    # 8. 儲存 JSON
    output = {
        "update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
        "stocks": final_results
    }
    
    try:
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=4)
        print("✅ data.json 更新成功")
    except Exception as e:
        print(f"❌ 儲存 JSON 失敗: {e}")

    # 9. Telegram 推送
    if TELEGRAM_TOKEN and CHAT_ID:
        try:
            # 修正: 添加 /bot 前綴
            token = TELEGRAM_TOKEN if TELEGRAM_TOKEN.startswith('bot') else f"bot{TELEGRAM_TOKEN}"
            url = f"https://api.telegram.org/{token}/sendMessage"
            
            # 生成訊息
            top_stocks = final_results[:10]
            msg_lines = ["📊 *港股 AI 看板更新成功*\n"]
            for s in top_stocks:
                msg_lines.append(
                    f"• {s['name']} ({s['code']})\n"
                    f"  {s['insight']} | 淨流入: {s['inflow']}億"
                )
            
            msg = "\n".join(msg_lines)
            
            payload = {
                "chat_id": CHAT_ID,
                "text": msg,
                "parse_mode": "Markdown"
            }
            
            res = requests.post(url, json=payload, timeout=10)
            
            if res.status_code == 200:
                print(f"✅ Telegram 推送成功")
            else:
                print(f"⚠️ Telegram 推送失敗: {res.status_code}, {res.text}")
        except Exception as e:
            print(f"⚠️ Telegram 推送異常: {e}")
    else:
        print("⚠️ 未配置 Telegram (TELEGRAM_TOKEN 或 CHAT_ID 為空)")

if __name__ == "__main__":
    run_analysis()
