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

def normalize_stock_code(code):
    """
    標準化股票代碼
    處理各種可能的格式: '700', '00700', 'HK.00700' 等
    統一轉換為 5 位數字格式: '00700'
    """
    if pd.isna(code):
        return None
    
    # 轉為字符串並去除空格
    code = str(code).strip()
    
    # 移除常見前綴
    code = code.replace('HK.', '').replace('hk.', '')
    code = code.replace('SH.', '').replace('sh.', '')
    
    # 只保留數字
    code = ''.join(filter(str.isdigit, code))
    
    # 補零到 5 位
    if len(code) > 0:
        return code.zfill(5)
    return None

def get_historical_short_avg(days=5):
    """獲取過去5個交易日的平均沽空率"""
    print(f"\n📊 開始獲取過去 {days} 天的沽空數據...")
    short_histories = []
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    attempts = 0
    
    while len(short_histories) < days and attempts < 10:
        date_str = target_date.strftime('%Y%m%d')
        try:
            df = ak.stock_hksell_summary(date=date_str)
            if not df.empty:
                print(f"  ✓ {date_str}: 獲取 {len(df)} 隻股票")
                c_col = [c for c in df.columns if '代' in c]
                if c_col:
                    df['股票代碼'] = df[c_col[0]].apply(normalize_stock_code)
                    df = df[df['股票代碼'].notna()]  # 移除無效代碼
                    short_histories.append(df[['股票代碼', '沽空比率']])
        except Exception as e:
            print(f"  ⚠️ {date_str} 失敗: {e}")
        
        target_date -= datetime.timedelta(days=1)
        attempts += 1
    
    if not short_histories: 
        print("❌ 無法獲取歷史沽空數據")
        return pd.DataFrame(columns=['股票代碼', 'short_avg'])
    
    avg_df = pd.concat(short_histories, ignore_index=True)
    avg_df = avg_df.groupby('股票代碼')['沽空比率'].mean().reset_index()
    avg_df.columns = ['股票代碼', 'short_avg']
    print(f"✅ 歷史沽空平均值: {len(avg_df)} 隻股票")
    return avg_df

def run_analysis():
    print("🚀 啟動數據掃描...")
    print(f"⏰ 執行時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 獲取港股行情
    print("\n📈 步驟 1: 獲取港股行情數據...")
    try:
        df_all = ak.stock_hk_spot_em()
        if df_all.empty:
            print("❌ 無法獲取港股行情數據")
            return
        print(f"✅ 獲取 {len(df_all)} 隻港股數據")
    except Exception as e:
        print(f"❌ 獲取港股行情失敗: {e}")
        return

    # 找到成交額、代碼、名稱列
    t_col = [c for c in df_all.columns if '成交额' in c or '成交金额' in c or '成交額' in c]
    c_col = [c for c in df_all.columns if '代码' in c or '代碼' in c]
    n_col = [c for c in df_all.columns if '名称' in c or '名稱' in c]
    
    if not t_col or not c_col or not n_col:
        print(f"❌ 無法識別數據列名")
        return
    
    t_col, c_col, n_col = t_col[0], c_col[0], n_col[0]
    
    # 轉換成交額並排序
    df_all[t_col] = pd.to_numeric(df_all[t_col], errors='coerce')
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)
    
    # ⭐ 使用標準化函數
    df_all['股票代碼'] = df_all[c_col].apply(normalize_stock_code)
    df_all = df_all[df_all['股票代碼'].notna()]  # 移除無效代碼
    
    print(f"✅ 篩選成交額前 40 名")
    print(f"   樣本代碼: {df_all['股票代碼'].head(5).tolist()}")

    # 2. 獲取南向資金
    print("\n💰 步驟 2: 獲取南向資金數據...")
    try:
        df_hgt = ak.stock_hk_ggt_board_em(symbol="滬港通")
        df_sgt = ak.stock_hk_ggt_board_em(symbol="深港通")
        df_gt_raw = pd.concat([df_hgt, df_sgt], ignore_index=True)
        
        gc_col = [c for c in df_gt_raw.columns if '代' in c][0]
        gb_col = [c for c in df_gt_raw.columns if '买入' in c or '買入' in c][0]
        gs_col = [c for c in df_gt_raw.columns if '卖出' in c or '賣出' in c][0]
        
        # ⭐ 使用標準化函數
        df_gt_raw['股票代碼'] = df_gt_raw[gc_col].apply(normalize_stock_code)
        df_gt_raw = df_gt_raw[df_gt_raw['股票代碼'].notna()]
        
        df_gt_raw['net_inflow'] = (
            pd.to_numeric(df_gt_raw[gb_col], errors='coerce') - 
            pd.to_numeric(df_gt_raw[gs_col], errors='coerce')
        ) / 1e8
        
        df_gt = df_gt_raw[['股票代碼', 'net_inflow']].drop_duplicates(subset=['股票代碼'])
        
        print(f"✅ 獲取南向資金: {len(df_gt)} 隻")
        print(f"   樣本代碼: {df_gt['股票代碼'].head(5).tolist()}")
        
        # 🔍 檢查重疊
        overlap = set(df_all['股票代碼']) & set(df_gt['股票代碼'])
        print(f"   🔗 與成交額前40重疊: {len(overlap)} 隻")
        
        if len(overlap) == 0:
            print("   ⚠️⚠️⚠️ 警告: 沒有重疊股票!")
            
    except Exception as e:
        print(f"⚠️ 獲取南向資金失敗: {e}")
        df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])

    # 3. 獲取今日沽空數據
    print("\n📉 步驟 3: 獲取今日沽空數據...")
    try:
        df_short_raw = ak.stock_hksell_summary()
        sc_col = [c for c in df_short_raw.columns if '代' in c][0]
        
        # ⭐ 使用標準化函數
        df_short_raw['股票代碼'] = df_short_raw[sc_col].apply(normalize_stock_code)
        df_short_raw = df_short_raw[df_short_raw['股票代碼'].notna()]
        
        df_short_today = df_short_raw[['股票代碼', '沽空比率']]
        
        print(f"✅ 獲取今日沽空: {len(df_short_today)} 隻")
        print(f"   樣本代碼: {df_short_today['股票代碼'].head(5).tolist()}")
        
        # 🔍 檢查重疊
        overlap = set(df_all['股票代碼']) & set(df_short_today['股票代碼'])
        print(f"   🔗 與成交額前40重疊: {len(overlap)} 隻")
        
        if len(overlap) == 0:
            print("   ⚠️⚠️⚠️ 警告: 沒有重疊股票!")
            
    except Exception as e:
        print(f"⚠️ 獲取今日沽空數據失敗: {e}")
        df_short_today = pd.DataFrame(columns=['股票代碼', '沽空比率'])

    # 4. 獲取歷史平均沽空率
    df_avg = get_historical_short_avg(5)

    # 5. 數據整合
    print("\n🔗 步驟 5: 數據整合...")
    df_f = pd.merge(df_all[['股票代碼', n_col]], df_gt, on='股票代碼', how='left')
    print(f"   合併南向資金: 有數據 {df_f['net_inflow'].notna().sum()}/{len(df_f)} 隻")
    
    df_f = pd.merge(df_f, df_short_today, on='股票代碼', how='left')
    print(f"   合併今日沽空: 有數據 {df_f['沽空比率'].notna().sum()}/{len(df_f)} 隻")
    
    if not df_avg.empty:
        df_f = pd.merge(df_f, df_avg, on='股票代碼', how='left')
        print(f"   合併歷史沽空: 有數據 {df_f['short_avg'].notna().sum()}/{len(df_f)} 隻")
    else:
        df_f['short_avg'] = 0

    # 填充空值
    df_f['net_inflow'] = df_f['net_inflow'].fillna(0)
    df_f['沽空比率'] = df_f['沽空比率'].fillna(0)
    df_f['short_avg'] = df_f['short_avg'].fillna(0)
    df_f = df_f.head(30)

    print(f"\n✅ 最終數據: {len(df_f)} 隻")
    print(f"   非零淨流入: {(df_f['net_inflow'] != 0).sum()} 隻")
    print(f"   非零沽空: {(df_f['沽空比率'] != 0).sum()} 隻")
    
    # 顯示前5名樣本
    print(f"\n📋 前5名樣本:")
    print(df_f[['股票代碼', n_col, 'net_inflow', '沽空比率']].head().to_string(index=False))

    # 6. 讀取舊排名
    old_ranks = {}
    if os.path.exists('data.json'):
        try:
            with open('data.json', 'r', encoding='utf-8') as f:
                old_json = json.load(f)
                prev_stocks = old_json.get('stocks', [])
                old_ranks = {s['code']: i for i, s in enumerate(prev_stocks)}
        except:
            pass

    # 7. 生成結果
    print("\n🎯 步驟 7: 生成分析結果...")
    final_results = []
    insights_count = {"✅ 正常": 0, "⚠️ 空頭平倉": 0, "🔥 主力掃貨": 0}
    
    for i, (_, row) in enumerate(df_f.iterrows()):
        code = row['股票代碼']
        name = row[n_col]
        inflow = float(row['net_inflow'])
        curr_s = float(row['沽空比率'])
        avg_s = float(row['short_avg'])
        
        insight = "✅ 正常"
        if avg_s > 0 and curr_s < (avg_s * 0.75) and inflow > 1.5:
            insight = "⚠️ 空頭平倉"
        elif inflow > 10: 
            insight = "🔥 主力掃貨"
        
        insights_count[insight] += 1
        old_rank = old_ranks.get(code, i)

        final_results.append({
            "code": code,
            "name": name,
            "inflow": round(inflow, 2),
            "short_today": round(curr_s, 2),
            "short_avg": round(avg_s, 2),
            "insight": insight,
            "rank_change": old_rank - i
        })

    print(f"✅ 洞察統計: {insights_count}")

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
        print(f"❌ 儲存失敗: {e}")

    # 9. Telegram 推送
    if TELEGRAM_TOKEN and CHAT_ID:
        try:
            token = TELEGRAM_TOKEN if TELEGRAM_TOKEN.startswith('bot') else f"bot{TELEGRAM_TOKEN}"
            url = f"https://api.telegram.org/{token}/sendMessage"
            
            interesting = [s for s in final_results[:10] if s['insight'] != "✅ 正常"]
            if not interesting:
                interesting = final_results[:5]
            
            msg_lines = [
                f"📊 *港股 AI 看板*",
                f"⏰ {output['update_time']}\n",
                f"🎯 {insights_count['🔥 主力掃貨']} 隻主力股 · {insights_count['⚠️ 空頭平倉']} 隻空平\n"
            ]
            
            for s in interesting:
                emoji = {"🔥 主力掃貨": "🔥", "⚠️ 空頭平倉": "⚠️"}.get(s['insight'], "•")
                msg_lines.append(
                    f"{emoji} *{s['name']}* ({s['code']})\n"
                    f"   流入 {s['inflow']}億 | 沽空 {s['short_today']}%"
                )
            
            payload = {
                "chat_id": CHAT_ID,
                "text": "\n".join(msg_lines),
                "parse_mode": "Markdown"
            }
            
            res = requests.post(url, json=payload, timeout=10)
            print(f"{'✅' if res.status_code == 200 else '⚠️'} Telegram: {res.status_code}")
        except Exception as e:
            print(f"⚠️ Telegram 異常: {e}")

    print("\n" + "="*60)
    print("🎉 分析完成!")
    print("="*60)

if __name__ == "__main__":
    run_analysis()
