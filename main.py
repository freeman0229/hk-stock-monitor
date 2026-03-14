import os, json, time, logging, re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# =========================
# CONFIG
# =========================
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID          = os.getenv("CHAT_ID", "").strip()
TOP_OUTPUT       = 30
SHORT_DROP_RATIO = 0.75
MAX_LOOKBACK     = 30   # calendar days to look back when collecting history

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)",
    "Referer":    "https://www.hkex.com.hk/",
}

# =========================
# HELPERS
# =========================
def fmt_code(val):
    """Zero-pad a stock code to 5 digits."""
    return str(val).strip().lstrip("0").zfill(5)

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=15)
        if r.status_code == 200:
            log.info("Telegram sent")
        else:
            log.warning("Telegram error: %s", r.text)
    except Exception as e:
        log.error("Telegram failed: %s", e)

def to_num(s):
    """Strip commas/spaces and cast to float; return 0 on failure."""
    try:
        return float(str(s).replace(",", "").strip())
    except Exception:
        return 0.0

def load_json_store(path):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json_store(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# =========================
# SOURCE 1 – HKEX DAILY QUOTATION (fixed-width text)
# URL: https://www.hkex.com.hk/eng/stat/smstat/dayquot/d{YY}{MM}{DD}e.htm
# Relevant section: "SALES RECORDS FOR ALL STOCKS"
# Each stock line: CODE  NAME  CUR  SHARES  TURNOVER($)
# =========================
def get_daily_quotation(date: datetime = None) -> pd.DataFrame:
    """
    Returns Top-N stocks by HKD turnover.
    Columns: stock_code, name, turnover, shares
    """
    date = date or datetime.now()
    date_str = date.strftime("%y%m%d")
    url = f"https://www.hkex.com.hk/eng/stat/smstat/dayquot/d{date_str}e.htm"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        text = resp.text

        # The file contains multiple sections separated by dashes.
        # "SALES RECORDS FOR ALL STOCKS" has lines like:
        #   CODE  NAME                CUR   SHARES TRADED      TURNOVER ($)
        # Two-line format per stock:
        #   Line 1:  CODE  NAME  CUR  SHARES
        #   Line 2:  (blank CODE/NAME)  CUR  PREV_CLOSE  ... TURNOVER
        # More reliably: "SALES RECORDS OVER $500,000" subset uses same format.
        # We parse the 10 MOST ACTIVES block for robust column positions,
        # then scan the full SALES RECORDS block.

        # ── Locate the SALES RECORDS FOR ALL STOCKS section ──
        start_marker = "SALES RECORDS FOR ALL STOCKS"
        end_marker   = "SALES RECORDS OVER $500,000"
        start = text.find(start_marker)
        end   = text.find(end_marker, start)
        if start == -1:
            # Fallback: use 10 MOST ACTIVES block
            start_marker = "10 MOST ACTIVES (DOLLARS)"
            end_marker   = "10 MOST ACTIVES (SHARES)"
            start = text.find(start_marker)
            end   = text.find(end_marker, start)

        section = text[start:end] if start != -1 and end != -1 else text

        # ── Parse fixed-width lines ──
        # Pattern for a stock data line:
        #   leading spaces, 4-5 digit code, spaces, NAME (up to 16 chars),
        #   CUR (HKD/USD/CNY), spaces, SHARES, spaces, TURNOVER
        #
        # The SALES RECORDS section has two lines per stock.
        # Line 1 example:   "     700 TENCENT         HKD      47,623,240    26,800,185,739"
        # We capture: code, name, currency, shares_or_turnover_1, value_2
        #
        # Actually the 10 MOST ACTIVES block is already sorted by turnover and
        # has both SHARES and TURNOVER on one line — use that for simplicity.
        # For full list we use the SALES RECORDS which has:
        #   CODE NAME CUR PREV.CLO / CLOSING  ASK/BID  HIGH/LOW  SHARES  TURNOVER
        # Split across TWO lines per stock (second line has the turnover).

        records = []

        # Regex for the "10 MOST ACTIVES (DOLLARS)" block — one line per stock:
        # "  700 TENCENT         HKD      26,800,185,739         47,623,240  578.00   548.00"
        most_active_pattern = re.compile(
            r"^\s{1,6}(\d{1,6})\s+([A-Z0-9][A-Z0-9 \-&'.#/]{1,20}?)\s{2,}"
            r"(HKD|USD|CNY|EUR|GBP)\s+([\d,]+)\s+([\d,]+)",
            re.MULTILINE
        )

        # Locate the MOST ACTIVES (DOLLARS) block to get order
        ma_start = text.find("10 MOST ACTIVES (DOLLARS)")
        ma_end   = text.find("10 MOST ACTIVES (SHARES)", ma_start)
        most_active_text = text[ma_start:ma_end] if ma_start != -1 and ma_end != -1 else ""

        seen_codes = set()

        # Parse 10 MOST ACTIVES first (already ranked by turnover $)
        for m in most_active_pattern.finditer(most_active_text):
            code     = fmt_code(m.group(1))
            name     = m.group(2).strip()
            val_a    = to_num(m.group(4))
            val_b    = to_num(m.group(5))
            # In the MOST ACTIVES block: col order is TURNOVER($) then SHARES
            turnover = val_a
            shares   = val_b
            if code not in seen_codes:
                records.append({"stock_code": code, "name": name,
                                 "turnover": turnover, "shares": shares})
                seen_codes.add(code)

        # ── Also parse SALES RECORDS for ALL stocks (to cover full Top-30) ──
        # Format in SALES RECORDS FOR ALL STOCKS:
        #   CODE  NAME   CUR  PRV.CLO/CLOSE  ASK/BID  HIGH/LOW  SHARES / TURNOVER
        # Two lines per stock — second line has the actual TURNOVER.
        # Pattern for the FIRST line of each stock entry (has the code):
        sales_line_pat = re.compile(
            r"^\s{1,6}(\d{1,6})\s+([A-Z0-9][A-Z0-9 \-&'.#/]{1,20}?)\s{2,}"
            r"(HKD|USD|CNY|EUR|GBP)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,]+)$",
            re.MULTILINE
        )
        # Second line (no code, just values): turnover is the last big number
        # Simpler approach: collect all lines with a leading code, then grab
        # the turnover from the FOLLOWING line.
        lines = section.split("\n")
        i = 0
        while i < len(lines):
            line = lines[i]
            m = re.match(
                r"^\s{1,6}(\d{1,6})\s+([A-Z0-9][A-Z0-9 \-&'.#/]{1,20}?)\s{2,}"
                r"(HKD|USD|CNY)",
                line
            )
            if m:
                code = fmt_code(m.group(1))
                name = m.group(2).strip()
                # Turnover is on the NEXT line, last number
                if i + 1 < len(lines):
                    next_line = lines[i + 1]
                    nums = re.findall(r"[\d,]{4,}", next_line)
                    if nums and code not in seen_codes:
                        turnover = to_num(nums[-1])   # last big number = turnover $
                        shares_nums = re.findall(r"[\d,]{4,}", line)
                        shares = to_num(shares_nums[-1]) if shares_nums else 0
                        records.append({"stock_code": code, "name": name,
                                         "turnover": turnover, "shares": shares})
                        seen_codes.add(code)
                i += 2
                continue
            i += 1

        if not records:
            log.warning("Daily quotation: no records parsed for %s", date_str)
            return pd.DataFrame(columns=["stock_code", "name", "turnover", "shares"])

        df = pd.DataFrame(records)
        df = df[df["turnover"] > 0]
        df = df.sort_values("turnover", ascending=False).head(TOP_OUTPUT).reset_index(drop=True)
        log.info("Daily quotation: %d records for %s", len(df), date_str)
        return df

    except requests.HTTPError as e:
        log.warning("Daily quotation not available for %s: %s", date_str, e)
        return pd.DataFrame(columns=["stock_code", "name", "turnover", "shares"])
    except Exception as e:
        log.error("get_daily_quotation failed (%s): %s", date_str, e)
        return pd.DataFrame(columns=["stock_code", "name", "turnover", "shares"])


# =========================
# SOURCE 2 – HKEX SHORT SELLING (fixed-width text)
# Today's day-close: https://www.hkex.com.hk/eng/stat/smstat/ssturnover/ncms/ashtmain.htm
# Historical archive URL pattern (if it exists):
#   https://www.hkex.com.hk/eng/stat/smstat/ssturnover/ncms/ash{YYMMDD}main.htm
# Columns: CODE  NAME  SHARES(SH)  TURNOVER($)
# We calculate short_ratio = short_turnover / stock_turnover (from daily quotation)
# =========================

SHORT_SELL_TODAY_URL = "https://www.hkex.com.hk/eng/stat/smstat/ssturnover/ncms/ashtmain.htm"

def _parse_short_sell_text(text: str) -> pd.DataFrame:
    """Parse HKEX fixed-width short selling text into a DataFrame."""
    pat = re.compile(
        r"^\s{1,6}(\d{1,6})\s+([A-Z0-9][A-Z0-9 \-&'.#/]{1,20}?)\s{2,}"
        r"([\d,]+)\s+([\d,]+)\s*$",
        re.MULTILINE
    )
    rows = []
    for m in pat.finditer(text):
        rows.append({
            "stock_code":      fmt_code(m.group(1)),
            "name":            m.group(2).strip(),
            "short_volume":    to_num(m.group(3)),
            "short_turnover":  to_num(m.group(4)),
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["stock_code", "name", "short_volume", "short_turnover"]
    )

def get_short_sell_today() -> pd.DataFrame:
    try:
        resp = requests.get(SHORT_SELL_TODAY_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        df = _parse_short_sell_text(resp.text)
        log.info("Short sell today: %d records", len(df))
        return df
    except Exception as e:
        log.error("get_short_sell_today failed: %s", e)
        return pd.DataFrame(columns=["stock_code", "name", "short_volume", "short_turnover"])


# ── Short sell history storage ──────────────────────────────────────────────
# We persist each day's short sell data to short_history.json as:
# { "YYYYMMDD": { "XXXXX": {"short_volume": N, "short_turnover": N}, ... }, ... }

SHORT_HISTORY_FILE = "short_history.json"

def save_short_sell(date: datetime, df: pd.DataFrame):
    if df.empty:
        return
    store = load_json_store(SHORT_HISTORY_FILE)
    key = date.strftime("%Y%m%d")
    store[key] = {
        row.stock_code: {
            "short_volume":   row.short_volume,
            "short_turnover": row.short_turnover,
        }
        for row in df.itertuples()
    }
    # Keep only last MAX_LOOKBACK calendar days of data
    cutoff = (datetime.now() - timedelta(days=MAX_LOOKBACK)).strftime("%Y%m%d")
    store = {k: v for k, v in store.items() if k >= cutoff}
    save_json_store(SHORT_HISTORY_FILE, store)
    log.info("Saved short sell history for %s (%d stocks)", key, len(df))

def get_short_avg_ratio(stock_codes: list, days: int = 5,
                        daily_turnover_map: dict = None) -> pd.DataFrame:
    """
    Compute 5-day average short ratio = avg(short_turnover_day / stock_turnover_day).
    daily_turnover_map: {date_str: {stock_code: turnover}} loaded from daily_history.json
    Falls back to raw short_turnover average if daily turnover is unavailable.
    """
    store = load_json_store(SHORT_HISTORY_FILE)
    daily_tv = daily_turnover_map or {}

    # Collect up to `days` most recent trading dates present in history
    available_dates = sorted(store.keys(), reverse=True)[:days]

    ratios = {code: [] for code in stock_codes}

    for date_str in available_dates:
        day_data = store[date_str]
        for code in stock_codes:
            if code in day_data:
                short_tv = day_data[code]["short_turnover"]
                # Use the stock's own turnover that day if we have it
                stock_tv = daily_tv.get(date_str, {}).get(code, 0)
                if stock_tv > 0:
                    ratios[code].append(short_tv / stock_tv * 100)
                else:
                    # Fallback: store raw short_turnover; ratio will be 0
                    pass   # skip days without denominator

    rows = []
    for code in stock_codes:
        vals = ratios[code]
        rows.append({
            "stock_code": code,
            "short_ratio_today": 0.0,   # filled in main
            "short_ratio_avg5":  round(sum(vals) / len(vals), 2) if vals else 0.0,
        })
    return pd.DataFrame(rows)


# =========================
# SOURCE 3 – CCASS SOUTHBOUND SHAREHOLDING
# POST-based ASP.NET page
# URL: https://www3.hkexnews.hk/sdw/search/mutualmarket.aspx?t=hk
# =========================
def get_ccass_southbound(date: datetime = None) -> pd.DataFrame:
    """
    Returns CCASS southbound (HK stocks) shareholding for a given date.
    Columns: stock_code, name, shareholding, pct_listed
    """
    date = date or datetime.now()
    date_str = date.strftime("%Y/%m/%d")
    base_url = "https://www3.hkexnews.hk/sdw/search/mutualmarket.aspx"
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        resp = session.get(f"{base_url}?t=hk", timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        def hidden(name):
            tag = soup.find("input", {"name": name})
            return tag["value"] if tag else ""

        payload = {
            "__EVENTTARGET":        "btnSearch",
            "__EVENTARGUMENT":      "",
            "__VIEWSTATE":          hidden("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": hidden("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION":    hidden("__EVENTVALIDATION"),
            "txtShareholdingDate":  date_str,
            "t":                    "hk",
        }

        resp2 = session.post(f"{base_url}?t=hk", data=payload, timeout=30)
        resp2.raise_for_status()
        soup2 = BeautifulSoup(resp2.text, "html.parser")

        # The result table has class "table-mobile-list" or similar
        table = soup2.find("table", {"class": lambda c: c and "table" in c})
        if table is None:
            tables = soup2.find_all("table")
            table = next((t for t in tables if len(t.find_all("tr")) > 5), None)

        if table is None:
            log.warning("CCASS: no table found for %s", date_str)
            return pd.DataFrame(columns=["stock_code", "name", "shareholding", "pct_listed"])

        rows = []
        for tr in table.find_all("tr")[1:]:
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) >= 4:
                shareholding_str = tds[2].replace(",", "")
                rows.append({
                    "stock_code":  fmt_code(tds[0]),
                    "name":        tds[1],
                    "shareholding": int(shareholding_str) if shareholding_str.isdigit() else 0,
                    "pct_listed":  float(tds[3].replace("%", "").strip() or 0),
                })

        df = pd.DataFrame(rows)
        log.info("CCASS southbound: %d records for %s", len(df), date_str)
        return df

    except Exception as e:
        log.error("get_ccass_southbound failed (%s): %s", date_str, e)
        return pd.DataFrame(columns=["stock_code", "name", "shareholding", "pct_listed"])


# ── CCASS history storage ────────────────────────────────────────────────────
# { "YYYYMMDD": { "XXXXX": {"shareholding": N, "pct_listed": F}, ... }, ... }

CCASS_HISTORY_FILE = "ccass_history.json"

def save_ccass(date: datetime, df: pd.DataFrame):
    if df.empty:
        return
    store = load_json_store(CCASS_HISTORY_FILE)
    key = date.strftime("%Y%m%d")
    store[key] = {
        row.stock_code: {
            "shareholding": row.shareholding,
            "pct_listed":   row.pct_listed,
        }
        for row in df.itertuples()
    }
    cutoff = (datetime.now() - timedelta(days=MAX_LOOKBACK)).strftime("%Y%m%d")
    store = {k: v for k, v in store.items() if k >= cutoff}
    save_json_store(CCASS_HISTORY_FILE, store)
    log.info("Saved CCASS history for %s (%d stocks)", key, len(df))

def get_ccass_delta_and_avg(stock_codes: list, today_map: dict, days: int = 5):
    """
    For each stock, compute:
      ccass_delta   : today's shareholding - previous trading day's shareholding
      ccass_avg5    : average daily delta over last 5 available days
    Returns DataFrame with columns: stock_code, ccass_delta, ccass_avg5
    """
    store = load_json_store(CCASS_HISTORY_FILE)
    available_dates = sorted(store.keys(), reverse=True)  # newest first

    rows = []
    for code in stock_codes:
        today_sh = today_map.get(code, 0)

        # Delta vs previous day
        delta = 0
        if len(available_dates) >= 1:
            prev_day = store[available_dates[0]]  # most recent stored = yesterday
            prev_sh  = prev_day.get(code, {}).get("shareholding", 0)
            delta    = today_sh - prev_sh

        # 5-day average delta: differences between consecutive stored days
        deltas = []
        for i in range(min(days, len(available_dates) - 1)):
            d0 = store[available_dates[i]]
            d1 = store[available_dates[i + 1]]
            sh0 = d0.get(code, {}).get("shareholding", 0)
            sh1 = d1.get(code, {}).get("shareholding", 0)
            if sh0 > 0 or sh1 > 0:
                deltas.append(sh0 - sh1)

        avg5 = round(sum(deltas) / len(deltas), 0) if deltas else 0.0

        rows.append({
            "stock_code":   code,
            "ccass_delta":  delta,
            "ccass_avg5":   avg5,
        })

    return pd.DataFrame(rows)


# =========================
# DAILY TURNOVER HISTORY (needed as denominator for short ratio)
# =========================
DAILY_TV_FILE = "daily_turnover_history.json"

def save_daily_turnover(date: datetime, df: pd.DataFrame):
    if df.empty:
        return
    store = load_json_store(DAILY_TV_FILE)
    key = date.strftime("%Y%m%d")
    store[key] = {row.stock_code: row.turnover for row in df.itertuples()}
    cutoff = (datetime.now() - timedelta(days=MAX_LOOKBACK)).strftime("%Y%m%d")
    store = {k: v for k, v in store.items() if k >= cutoff}
    save_json_store(DAILY_TV_FILE, store)


# =========================
# MAIN ANALYSIS
# =========================
def run_analysis():
    today = datetime.now()
    log.info("=== Starting analysis for %s ===", today.strftime("%Y-%m-%d"))

    # ── 1. Fetch today's daily quotation → Top 30 by turnover ──
    df_quote = get_daily_quotation(today)
    if df_quote.empty:
        msg = "⚠️ 港股看板：今日日報表未能獲取，分析中止。"
        log.error(msg)
        send_telegram(msg)
        return

    # Save today's turnover for use as short ratio denominator in future runs
    save_daily_turnover(today, df_quote)
    stock_codes   = df_quote["stock_code"].tolist()
    turnover_map  = dict(zip(df_quote["stock_code"], df_quote["turnover"]))

    # ── 2. Fetch today's short selling ──
    df_short = get_short_sell_today()
    save_short_sell(today, df_short)

    # Build today's short ratio for the top-30 stocks
    short_map = {}
    if not df_short.empty:
        for row in df_short.itertuples():
            tv = turnover_map.get(row.stock_code, 0)
            if tv > 0:
                short_map[row.stock_code] = round(row.short_turnover / tv * 100, 2)

    # ── 3. Compute 5-day avg short ratio ──
    daily_tv_store = load_json_store(DAILY_TV_FILE)
    df_short_avg = get_short_avg_ratio(stock_codes, days=5,
                                       daily_turnover_map=daily_tv_store)
    short_avg_map = dict(zip(df_short_avg["stock_code"], df_short_avg["short_ratio_avg5"]))

    # ── 4. Fetch today's CCASS southbound ──
    df_ccass = get_ccass_southbound(today)
    today_ccass_map = {}
    if not df_ccass.empty:
        today_ccass_map = dict(zip(df_ccass["stock_code"], df_ccass["shareholding"]))
        today_pct_map   = dict(zip(df_ccass["stock_code"], df_ccass["pct_listed"]))
    else:
        today_pct_map = {}

    # Save today's CCASS *after* computing deltas (so history still has yesterday)
    df_ccass_stats = get_ccass_delta_and_avg(stock_codes, today_ccass_map, days=5)
    save_ccass(today, df_ccass)

    ccass_delta_map = dict(zip(df_ccass_stats["stock_code"], df_ccass_stats["ccass_delta"]))
    ccass_avg5_map  = dict(zip(df_ccass_stats["stock_code"], df_ccass_stats["ccass_avg5"]))

    # ── 5. Build result rows ──
    results = []
    for i, row in enumerate(df_quote.itertuples(), 1):
        code = row.stock_code

        short_ratio  = short_map.get(code, 0.0)
        short_avg5   = short_avg_map.get(code, 0.0)
        ccass_pct    = today_pct_map.get(code, 0.0)
        ccass_delta  = ccass_delta_map.get(code, 0)
        ccass_avg5   = ccass_avg5_map.get(code, 0.0)

        # ── Signal logic ──
        insight = "✅ 正常"
        if short_ratio > 15:
            insight = "🚨 高沽空比率"
        elif short_avg5 > 0 and short_ratio < short_avg5 * SHORT_DROP_RATIO:
            insight = "⚠️ 空頭平倉"
        if ccass_pct > 5:
            insight = "🔥 南向重倉"   # CCASS signal overrides short signal

        results.append({
            "rank":          i,
            "code":          code,
            "name":          row.name,
            "turnover":      int(row.turnover),
            "short_ratio":   round(short_ratio, 2),    # today's short % of turnover
            "short_avg5":    round(short_avg5, 2),     # 5-day avg short ratio %
            "ccass_pct":     round(ccass_pct, 2),      # % of listed shares held
            "ccass_delta":   int(ccass_delta),          # change vs previous day (shares)
            "ccass_avg5":    int(ccass_avg5),           # avg daily delta over 5 days
            "insight":       insight,
        })

    # ── 6. Persist output ──
    output = {
        "update_time": today.strftime("%Y-%m-%d %H:%M"),
        "stocks":      results,
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    log.info("data.json written with %d stocks", len(results))

    # ── 7. Telegram summary ──
    if results:
        flagged = [s for s in results if s["insight"] != "✅ 正常"]
        top = results[0]
        lines = [
            f"📊 港股 AI 看板更新",
            f"時間: {output['update_time']}",
            f"榜首: {top['name']} ({top['code']}) 成交額 {top['turnover']:,}",
            f"異動股: {len(flagged)} 隻",
        ]
        if flagged:
            lines.append("─────────────")
            for s in flagged[:5]:
                lines.append(f"{s['insight']} {s['name']} | 沽空率 {s['short_ratio']}% | CCASS {s['ccass_pct']}%")
        send_telegram("\n".join(lines))


if __name__ == "__main__":
    run_analysis()
