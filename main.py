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
                # 找到包含 '代' 字的列名 (股票代碼相關)
                c_col = [c for c in df.columns if '代' in c]
                if c_col:
                    df['股票代碼'] = df[c_col[0]].astype(str).str.zfill(5)
                    short_histories.append(df[['股票代碼', '沽空比率']])
                else:
                    print(f"  ⚠️ {date_str}: 找不到代碼列，列名: {df.columns.tolist()}")
            else:
                print(f"  ✗ {date_str}: 數據為空")
        except Exception as e:
            print(f"  ⚠️ {date_str} 失敗: {e}")
        
        target_date -= datetime.timedelta(days=1)
        attempts += 1
    
    if not short_histories: 
        print("❌ 無法獲取歷史沽空數據")
        return pd.DataFrame(columns=['股票代碼', 'short_avg'])
    
    # 合併所有歷史數據並計算平均值
    avg_df = pd.concat(short_histories, ignore_index=True)
    avg_df = avg_df.groupby('股票代碼')['沽空比率'].mean().reset_index()
    avg_df.columns = ['股票代碼', 'short_avg']
    print(f"✅ 歷史沽空平均值計算完成: {len(avg_df)} 隻股票")
    print(f"   樣本數據: {avg_df.head(3).to_dict('records')}")
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
        print(f"   列名: {df_all.columns.tolist()}")
    except Exception as e:
        print(f"❌ 獲取港股行情失敗: {e}")
        return

    # 找到成交額、代碼、名稱列
    t_col = [c for c in df_all.columns if '成交额' in c or '成交金额' in c or '成交額' in c]
    c_col = [c for c in df_all.columns if '代码' in c or '代碼' in c]
    n_col = [c for c in df_all.columns if '名称' in c or '名稱' in c]
    
    if not t_col or not c_col or not n_col:
        print(f"❌ 無法識別數據列名")
        print(f"   成交額列: {t_col}")
        print(f"   代碼列: {c_col}")
        print(f"   名稱列: {n_col}")
        return
    
    t_col, c_col, n_col = t_col[0], c_col[0], n_col[0]
    print(f"   使用列名 -> 成交額: {t_col}, 代碼: {c_col}, 名稱: {n_col}")
    
    # 轉換成交額為數值並排序
    df_all[t_col] = pd.to_numeric(df_all[t_col], errors='coerce')
    df_all = df_all.sort_values(by=t_col, ascending=False).head(40)
    df_all['股票代碼'] = df_all[c_col].astype(str).str.zfill(5)
    print(f"✅ 篩選成交額前 40 名股票")
    print(f"   樣本代碼: {df_all['股票代碼'].head(5).tolist()}")

    # 2. 獲取南向資金 (滬港通 + 深港通)
    print("\n💰 步驟 2: 獲取南向資金數據...")
    try:
        df_hgt = ak.stock_hk_ggt_board_em(symbol="滬港通")
        df_sgt = ak.stock_hk_ggt_board_em(symbol="深港通")
        df_gt_raw = pd.concat([df_hgt, df_sgt], ignore_index=True)
        
        print(f"   滬港通: {len(df_hgt)} 隻, 深港通: {len(df_sgt)} 隻")
        print(f"   列名: {df_gt_raw.columns.tolist()}")
        
        gc_col = [c for c in df_gt_raw.columns if '代' in c][0]
        gb_col = [c for c in df_gt_raw.columns if '买入' in c or '買入' in c][0]
        gs_col = [c for c in df_gt_raw.columns if '卖出' in c or '賣出' in c][0]
        
        print(f"   使用列名 -> 代碼: {gc_col}, 買入: {gb_col}, 賣出: {gs_col}")
        
        df_gt_raw['股票代碼'] = df_gt_raw[gc_col].astype(str).str.zfill(5)
        # 計算淨流入 (億)
        df_gt_raw['net_inflow'] = (
            pd.to_numeric(df_gt_raw[gb_col], errors='coerce') - 
            pd.to_numeric(df_gt_raw[gs_col], errors='coerce')
        ) / 1e8
        
        df_gt = df_gt_raw[['股票代碼', 'net_inflow']].drop_duplicates(subset=['股票代碼'])
        print(f"✅ 獲取南向資金數據: {len(df_gt)} 隻股票")
        print(f"   有淨流入數據 (非0): {(df_gt['net_inflow'] != 0).sum()} 隻")
        print(f"   樣本代碼: {df_gt['股票代碼'].head(5).tolist()}")
        print(f"   樣本數據: {df_gt.head(3).to_dict('records')}")
    except Exception as e:
        print(f"⚠️ 獲取南向資金失敗: {e}")
        df_gt = pd.DataFrame(columns=['股票代碼', 'net_inflow'])

    # 3. 獲取今日沽空數據
    print("\n📉 步驟 3: 獲取今日沽空數據...")
    try:
        df_short_raw = ak.stock_hksell_summary()
        print(f"   列名: {df_short_raw.columns.tolist()}")
        
        sc_col = [c for c in df_short_raw.columns if '代' in c][0]
        print(f"   使用列名 -> 代碼: {sc_col}")
        
        df_short_raw['股票代碼'] = df_short_raw[sc_col].astype(str).str.zfill(5)
        df_short_today = df_short_raw[['股票代碼', '沽空比率']]
        print(f"✅ 獲取今日沽空數據: {len(df_short_today)} 隻股票")
        print(f"   有沽空數據 (非0): {(df_short_today['沽空比率'] != 0).sum()} 隻")
        print(f"   樣本代碼: {df_short_today['股票代碼'].head(5).tolist()}")
        print(f"   樣本數據: {df_short_today.head(3).to_dict('records')}")
    except Exception as e:
        print(f"⚠️ 獲取今日沽空數據失敗: {e}")
        df_short_today = pd.DataFrame(columns=['股票代碼', '沽空比率'])

    # 4. 獲取歷史平均沽空率
    print("\n📊 步驟 4: 計算歷史平均沽空率...")
    df_avg = get_historical_short_avg(5)

    # 5. 數據整合
    print("\n🔗 步驟 5: 數據整合...")
    print(f"   初始股票數: {len(df_all)}")
    
    df_f = pd.merge(df_all[['股票代碼', n_col]], df_gt, on='股票代碼', how='left')
    print(f"   合併南向資金後: {len(df_f)} 隻")
    print(f"   有南向資金數據: {df_f['net_inflow'].notna().sum()} 隻")
    
    df_f = pd.merge(df_f, df_short_today, on='股票代碼', how='left')
    print(f"   合併今日沽空後: {len(df_f)} 隻")
    print(f"   有沽空數據: {df_f['沽空比率'].notna().sum()} 隻")
    
    if not df_avg.empty and '股票代碼' in df_avg.columns:
        df_f = pd.merge(df_f, df_avg, on='股票代碼', how='left')
        print(f"   合併歷史沽空後: {len(df_f)} 隻")
        print(f"   有歷史平均數據: {df_f['short_avg'].notna().sum()} 隻")
    else:
        df_f['short_avg'] = 0
        print(f"   ⚠️ 無歷史平均沽空數據，全部設為 0")

    # 填充空值為 0
    df_f['net_inflow'] = df_f['net_inflow'].fillna(0)
    df_f['沽空比率'] = df_f['沽空比率'].fillna(0)
    df_f['short_avg'] = df_f['short_avg'].fillna(0)
    df_f = df_f.head(30)

    print(f"\n✅ 數據整合完成: {len(df_f)} 隻股票")
    print(f"   非零淨流入: {(df_f['net_inflow'] != 0).sum()} 隻")
    print(f"   非零沽空比率: {(df_f['沽空比率'] != 0).sum()} 隻")
    print(f"   非零歷史平均: {(df_f['short_avg'] != 0).sum()} 隻")
    
    # 顯示前 5 名樣本數據
    print("\n📋 前 5 名股票樣本數據:")
    print(df_f[['股票代碼', n_col, 'net_inflow', '沽空比率', 'short_avg']].head().to_string(index=False))

    # 6. 讀取舊排名
    print("\n📂 步驟 6: 讀取歷史排名...")
    old_ranks = {}
    if os.path.exists('data.json'):
        try:
            with open('data.json', 'r', encoding='utf-8') as f:
                old_json = json.load(f)
                prev_stocks = old_json.get('stocks', [])
                old_ranks = {s['code']: i for i, s in enumerate(prev_stocks)}
            print(f"✅ 讀取到 {len(old_ranks)} 隻股票的歷史排名")
        except Exception as e:
            print(f"⚠️ 讀取舊數據失敗: {e}")
    else:
        print("ℹ️ 無歷史數據文件")

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
        
        # 市場洞察邏輯
        insight = "✅ 正常"
        if avg_s > 0 and curr_s < (avg_s * 0.75) and inflow > 1.5:
            insight = "⚠️ 空頭平倉"
        elif inflow > 10: 
            insight = "🔥 主力掃貨"
        
        insights_count[insight] += 1

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

    print(f"✅ 生成 {len(final_results)} 隻股票分析結果")
    print(f"   洞察統計: {insights_count}")

    # 8. 儲存 JSON
    print("\n💾 步驟 8: 儲存數據...")
    output = {
        "update_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
        "stocks": final_results
    }
    
    try:
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=4)
        print("✅ data.json 更新成功")
        print(f"   文件大小: {os.path.getsize('data.json')} bytes")
    except Exception as e:
        print(f"❌ 儲存 JSON 失敗: {e}")

    # 9. Telegram 推送
    print("\n📱 步驟 9: Telegram 推送...")
    if TELEGRAM_TOKEN and CHAT_ID:
        try:
            # 修正: 添加 bot 前綴
            token = TELEGRAM_TOKEN if TELEGRAM_TOKEN.startswith('bot') else f"bot{TELEGRAM_TOKEN}"
            url = f"https://api.telegram.org/{token}/sendMessage"
            
            # 生成訊息 - 只推送有洞察的股票
            interesting_stocks = [s for s in final_results[:10] if s['insight'] != "✅ 正常"]
            if not interesting_stocks:
                interesting_stocks = final_results[:5]  # 如果都正常，取前 5
            
            msg_lines = [
                f"📊 *港股 AI 看板更新*",
                f"⏰ {output['update_time']}\n",
                f"🎯 發現 {insights_count['🔥 主力掃貨']} 隻主力股, {insights_count['⚠️ 空頭平倉']} 隻空頭平倉\n"
            ]
            
            for s in interesting_stocks:
                emoji = "🔥" if s['insight'] == "🔥 主力掃貨" else "⚠️" if s['insight'] == "⚠️ 空頭平倉" else "•"
                msg_lines.append(
                    f"{emoji} *{s['name']}* ({s['code']})\n"
                    f"   淨流入: {s['inflow']}億 | 沽空: {s['short_today']}%"
                )
            
            msg = "\n".join(msg_lines)
            
            payload = {
                "chat_id": CHAT_ID,
                "text": msg,
                "parse_mode": "Markdown"
            }
            
            print(f"   推送 URL: {url[:50]}...")
            print(f"   Chat ID: {CHAT_ID}")
            
            res = requests.post(url, json=payload, timeout=10)
            
            if res.status_code == 200:
                print(f"✅ Telegram 推送成功")
            else:
                print(f"⚠️ Telegram 推送失敗: {res.status_code}")
                print(f"   響應: {res.text}")
        except Exception as e:
            print(f"⚠️ Telegram 推送異常: {e}")
    else:
        print(f"⚠️ 未配置 Telegram")
        print(f"   TELEGRAM_TOKEN: {'已設置' if TELEGRAM_TOKEN else '未設置'}")
        print(f"   CHAT_ID: {'已設置' if CHAT_ID else '未設置'}")

    print("\n" + "="*60)
    print("🎉 分析完成!")
    print("="*60)

if __name__ == "__main__":
    run_analysis()
