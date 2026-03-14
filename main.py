import os, json, time, logging, re
import pandas as pd
import requests
import holidays
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
MAX_LOOKBACK     = 30   # calendar days to look back when collecting history

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)",
    "Referer":    "https://www.hkex.com.hk/",
}

# =========================
# TRADING DAY CHECK
# =========================
HK_HOLIDAYS = holidays.HongKong()

def is_trading_day(date: datetime = None) -> bool:
    """Return True if date is a HK weekday that is not a public holiday."""
    date = date or datetime.now()
    return date.weekday() < 5 and date.date() not in HK_HOLIDAYS

def last_trading_day(date: datetime = None) -> datetime:
    """Return the most recent HK trading day on or before date."""
    d = date or datetime.now()
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d

def business_days_back(date: datetime, n: int) -> datetime:
    """Return the date that is n HK business days before `date`."""
    d = date
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if d.weekday() < 5 and d.date() not in HK_HOLIDAYS:
            count += 1
    return d

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
    Parses HKEX daily quotation Chinese file d{YY}{MM}{DD}c.htm.
    Single line per stock format:
      CODE  ENG_NAME  CHI_NAME  CUR  PRV  BID  ASK  HIGH  LOW  CLOSE  SHARES  TURNOVER
    Example:
      "     1 CKH HOLDINGS   長和　　　　　　 HKD  59.20  58.20 ... 5,899,810  344,059,907"
    Last two numbers = SHARES, TURNOVER.
    """
    date = date or datetime.now()
    date_str = date.strftime("%y%m%d")
    url = f"https://www.hkex.com.hk/chi/stat/smstat/dayquot/d{date_str}c.htm"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        # Decode raw bytes as cp950 (Windows Big5) — do NOT use apparent_encoding
        text = resp.content.decode("cp950", errors="replace")

        # Extract <pre> block
        soup = BeautifulSoup(text, "html.parser")
        pre  = soup.find("pre")
        body = pre.get_text() if pre else text

        log.info("Daily quotation c.htm: %d lines for %s", body.count("\n"), date_str)

        records    = []
        seen_codes = set()

        for line in body.splitlines():
            # Pattern: optional *, spaces, 1-5 digit code, English name, Chinese name,
            #          currency, 6 price fields, shares, turnover
            m = re.match(
                r"^[\*\s]{0,5}(\d{1,5})\s+"                      # code
                r"([A-Z][A-Z0-9 \-&'./#+]{1,20}?)\s{2,}"         # English name
                r"([\u4e00-\u9fff\uff01-\uffee\u3000-\u303f\s]{1,20}?)\s*"  # Chinese name
                r"(HKD|USD|CNY|EUR|GBP)\s+"                       # currency
                r"[\d,.]+\s+[\d,.]+\s+[\d,.]+\s+"                 # PRV BID ASK
                r"[\d,.]+\s+[\d,.]+\s+[\d,.]+\s+"                 # HIGH LOW CLOSE
                r"([\d,]+)\s+([\d,]+)\s*$",                       # SHARES  TURNOVER
                line
            )
            if m:
                code     = fmt_code(m.group(1))
                name_eng = m.group(2).strip()
                name_chi = m.group(3).strip().replace("\u3000", "").strip()
                shares   = to_num(m.group(5))
                turnover = to_num(m.group(6))
                if code not in seen_codes and turnover > 0:
                    records.append({
                        "stock_code": code,
                        "name":       name_eng,
                        "name_chi":   name_chi or name_eng,
                        "turnover":   turnover,
                        "shares":     shares,
                    })
                    seen_codes.add(code)
                    if len(records) <= 3:
                        log.info("Sample: %s %s %s tv=%d", code, name_eng, name_chi, int(turnover))

        if not records:
            log.warning("Daily quotation c.htm: 0 records for %s — check encoding/format", date_str)
            return pd.DataFrame(columns=["stock_code", "name", "name_chi", "turnover", "shares"])

        df = pd.DataFrame(records)
        df = df[df["turnover"] > 0]
        df = df.sort_values("turnover", ascending=False).head(TOP_OUTPUT).reset_index(drop=True)
        log.info("Daily quotation: %d records for %s", len(df), date_str)
        return df

    except requests.HTTPError as e:
        log.warning("Daily quotation not available for %s: %s", date_str, e)
        return pd.DataFrame(columns=["stock_code", "name", "name_chi", "turnover", "shares"])
    except Exception as e:
        log.error("get_daily_quotation failed (%s): %s", date_str, e)
        return pd.DataFrame(columns=["stock_code", "name", "name_chi", "turnover", "shares"])


def _get_daily_quotation_fallback(date_str: str) -> pd.DataFrame:
    """Unused placeholder."""
    return pd.DataFrame(columns=["stock_code", "name", "name_chi", "turnover", "shares"])


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
    """
    Parse HKEX fixed-width short selling text.

    Actual format observed:
      "      1  CKH HOLDINGS           2,837,000    183,215,000"
      "    388  HKEX                   1,248,500    509,260,860"

    Pattern: 4-6 spaces, code, 2 spaces, name (padded), shares(SH), turnover($)
    """
    # The name field is padded to ~22 chars, followed by the two numeric columns.
    # Use a permissive pattern: leading spaces, digits, 2+ spaces, name, 2+ spaces, nums
    pat = re.compile(
        r"^\s{2,8}(\d{1,6})\s{1,3}([A-Z][A-Z0-9 \-&'./#+]{1,24}?)\s{2,}"
        r"([\d,]+)\s+([\d,]+)\s*$",
        re.MULTILINE
    )
    rows = []
    for m in pat.finditer(text):
        code = fmt_code(m.group(1))
        name = m.group(2).strip()
        # Skip header-like lines
        if name in ("NAME OF STOCK", "CODE") or not name:
            continue
        rows.append({
            "stock_code":     code,
            "name":           name,
            "short_volume":   to_num(m.group(3)),
            "short_turnover": to_num(m.group(4)),
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

        # Consecutive days CCASS has been moving in the same direction as today
        # Positive = N days of consecutive increases; negative = consecutive decreases
        consec = 0
        if delta != 0:
            direction = 1 if delta > 0 else -1
            for i in range(min(days, len(available_dates) - 1)):
                d0 = store[available_dates[i]]
                d1 = store[available_dates[i + 1]]
                sh0 = d0.get(code, {}).get("shareholding", 0)
                sh1 = d1.get(code, {}).get("shareholding", 0)
                day_delta = sh0 - sh1
                if day_delta * direction > 0:
                    consec += direction
                else:
                    break

        avg5 = round(sum(deltas) / len(deltas), 0) if deltas else 0.0

        rows.append({
            "stock_code":    code,
            "ccass_delta":   delta,
            "ccass_avg5":    avg5,
            "ccass_consec":  consec,
        })

    return pd.DataFrame(rows)


# =========================
# DAILY TURNOVER HISTORY (needed as denominator for short ratio)
# =========================
DAILY_TV_FILE = "daily_turnover_history.json"
RANK_HISTORY_FILE = "rank_history.json"

def save_daily_turnover(date: datetime, df: pd.DataFrame):
    if df.empty:
        return
    store = load_json_store(DAILY_TV_FILE)
    key = date.strftime("%Y%m%d")
    store[key] = {row.stock_code: row.turnover for row in df.itertuples()}
    cutoff = (datetime.now() - timedelta(days=MAX_LOOKBACK)).strftime("%Y%m%d")
    store = {k: v for k, v in store.items() if k >= cutoff}
    save_json_store(DAILY_TV_FILE, store)


def save_rank_history(date: datetime, results: list):
    """Persist today's {code: rank} mapping for next day's comparison."""
    store = load_json_store(RANK_HISTORY_FILE)
    key = date.strftime("%Y%m%d")
    store[key] = {r["code"]: r["rank"] for r in results}
    cutoff = (datetime.now() - timedelta(days=MAX_LOOKBACK)).strftime("%Y%m%d")
    store = {k: v for k, v in store.items() if k >= cutoff}
    save_json_store(RANK_HISTORY_FILE, store)


def get_prev_ranks() -> dict:
    """Return {code: rank} from the most recent stored trading day."""
    store = load_json_store(RANK_HISTORY_FILE)
    if not store:
        return {}
    latest_key = sorted(store.keys())[-1]
    return store[latest_key]


# =========================
# STOCK TYPE CLASSIFICATION & SIGNAL LOGIC
# =========================

# Known ETFs and their normal short ratio range
ETF_CODES = {
    "02800", "02828", "03033", "03032", "03188", "02846",
    "03140", "03037", "03011", "02823",
}

# Known energy / bank / utility "stable" stocks
STABLE_CODES = {
    "00883",  # CNOOC
    "00386",  # Sinopec
    "00857",  # PetroChina
    "01398",  # ICBC
    "03988",  # Bank of China
    "02388",  # BOC HK
    "00939",  # CCB
    "01288",  # ABC
    "00002",  # CLP Holdings
    "00006",  # Power Assets
    "00003",  # HK & China Gas
    "00066",  # MTR
}

# Name-based fallback keywords for blue chips
BLUECHIP_KEYWORDS = (
    "TENCENT", "MEITUAN", "ALIBABA", "BABA", "XIAOMI",
    "HSBC", "AIA", "PING AN", "HKEX", "CK ", "HENDERSON",
    "SHK", "SWIRE", "GALAXY", "SANDS", "MELCO",
)

STABLE_KEYWORDS = (
    "BANK", "ENERGY", "POWER", "GAS", "PETRO",
    "SINOPEC", "CNOOC", "MTR", "UTILITY",
)

def classify_stock(code: str, name: str) -> str:
    """
    Returns one of: 'etf', 'stable', 'bluechip', 'general'
    ETF       – index ETFs; normal short range 40–70%
    Stable    – energy, banks, utilities; normal range 5–10%
    Bluechip  – large-cap names; normal range 10–20%
    General   – everything else; normal range 10–25%
    """
    c = code.zfill(5)
    n = name.upper()
    if c in ETF_CODES:
        return "etf"
    if c in STABLE_CODES or any(k in n for k in STABLE_KEYWORDS):
        return "stable"
    if any(k in n for k in BLUECHIP_KEYWORDS):
        return "bluechip"
    return "general"


# Per-type thresholds
# (normal_lo, normal_hi, warning_change_pct, cover_threshold_pct)
# cover_threshold_pct: short_ratio must DROP below this fraction of avg to signal covering
THRESHOLDS = {
    #           normal_lo  normal_hi  spike_warn  cover_drop
    "etf":      (40.0,      70.0,      15.0,       0.35),
    "stable":   ( 5.0,      10.0,      15.0,       0.50),
    "bluechip": (10.0,      20.0,      10.0,       0.40),
    "general":  (10.0,      25.0,      15.0,       0.40),
}


def _turnover_avg5(code: str, daily_tv_store: dict) -> float:
    """Average of the 5 most recent stored daily turnover values for a stock."""
    recent_days = sorted(daily_tv_store.keys(), reverse=True)[:5]
    vals = [daily_tv_store[d].get(code, 0) for d in recent_days]
    valid = [v for v in vals if v > 0]
    return sum(valid) / len(valid) if valid else 0.0

def _value_at(code: str, date_key: str, store: dict, field: str = None) -> float:
    """
    Look up a stored value for a stock on a specific date key (YYYYMMDD).
    If field is None, the store maps code -> float directly.
    If field is set, the store maps code -> {field: float, ...}.
    Returns 0.0 if not found.
    """
    day = store.get(date_key, {})
    val = day.get(code, {} if field else 0)
    if field:
        return float(val.get(field, 0)) if isinstance(val, dict) else 0.0
    return float(val)


def classify_insight(
    code: str,
    stock_type: str,
    short_ratio: float,       # today's short ratio (T)
    short_avg5: float,        # 5-day avg short ratio ending at T
    short_ratio_t2: float,    # short ratio at T-2 business days (aligned with today's CCASS)
    turnover: int,            # today's turnover (T)
    turnover_avg5: float,     # 5-day avg turnover ending at T
    turnover_t2: float,       # turnover at T-2 business days (aligned with today's CCASS)
    ccass_pct: float,
    ccass_delta: int,
    ccass_avg5: float,
    ccass_consec: int,
) -> str:
    """
    Priority (highest → lowest):

      1. 🔥 南向重倉       — CCASS % > 5%: persistent southbound conviction
      2. 🏦 機構增持        — Steady multi-day CCASS build (≥3 settlement days = ≥6 trading days)
                              + short ratio at T-2 stable + turnover at T-2 only mildly elevated
      3. 🚪 避險盤撤退      — Single-day CCASS spike (consec ≤ 1)
                              + short ratio at T-2 collapsed + turnover at T-2 exploded
      4. 🚨 異常高沽空      — Short ratio (today) spikes well above type's normal ceiling
      5. ⚠️ 空頭平倉信號    — Sharp short drop today + volume surge today (no CCASS confirmation)
      6. 📉 沽空偏高        — Elevated but not extreme for this stock type
      7. 📈 沽空偏低        — Unusually low short ratio for this type
      8. ✅ 正常

    T+2 alignment:
      CCASS today reflects trades settled 2 business days ago (T-2).
      For CCASS cross-signals (機構增持, 避險盤撤退) we compare CCASS delta against
      short_ratio_t2 and turnover_t2 — the short/volume data from the day the
      underlying trades actually occurred — rather than today's data.
      This prevents false signals from comparing stale CCASS against today's market.
    """
    lo, hi, spike_warn, cover_drop = THRESHOLDS.get(stock_type, THRESHOLDS["general"])

    # Today's turnover ratio (for signals 4/5 which are about today's market)
    turnover_ratio_today = turnover / turnover_avg5 if turnover_avg5 > 0 else 1.0

    # T-2 turnover ratio and short change (for CCASS-aligned signals 2/3)
    turnover_ratio_t2 = turnover_t2 / turnover_avg5 if turnover_avg5 > 0 else 1.0
    short_change_t2   = ((short_ratio_t2 - short_avg5) / short_avg5
                         if short_avg5 > 0 else 0.0)

    # ── 1. Persistent southbound conviction ──
    if ccass_pct > 5:
        return "🔥 南向重倉"

    # ── 2. 機構增持 — genuine institutional accumulation ──
    # Uses T-2 aligned short/turnover so we're comparing CCASS to the day trades happened.
    # Conditions:
    #   a) CCASS building steadily for ≥3 settlement days (each = ~2 trading days of actual buying)
    #   b) Short ratio at T-2 was stable — no panic covering on trade day
    #   c) Turnover at T-2 only mildly elevated — deliberate accumulation, not urgent
    #   d) Today's CCASS delta is meaningful
    if (ccass_consec >= 3
            and abs(short_change_t2) <= 0.20
            and 1.10 <= turnover_ratio_t2 <= 1.50
            and ccass_delta > 0
            and (ccass_avg5 == 0 or ccass_delta >= ccass_avg5 * 0.5)):
        return "🏦 機構增持"

    # ── 3. 避險盤撤退 — short covering / risk-off exit ──
    # Uses T-2 aligned short/turnover — checks what short sellers were doing
    # on the actual day the CCASS-visible trades settled from.
    # Conditions:
    #   a) CCASS spiked in one settlement day only (sudden, not a build)
    #   b) Short ratio at T-2 collapsed vs avg — panic covering on trade day
    #   c) Turnover at T-2 exploded — urgency / forced close-out on trade day
    if (ccass_delta > 0
            and ccass_consec <= 1
            and short_avg5 > lo
            and short_ratio_t2 < short_avg5 * cover_drop
            and turnover_ratio_t2 > 1.50):
        return "🚪 避險盤撤退"

    # ── 4. Extreme short spike (today's market) ──
    if short_ratio > hi + spike_warn:
        return "🚨 異常高沽空"

    # ── 5. Short covering without CCASS confirmation (today's market) ──
    if (short_avg5 > lo
            and short_ratio < short_avg5 * cover_drop
            and turnover_ratio_today > 1.30):
        return "⚠️ 空頭平倉信號"

    # ── 6. Elevated short for this type ──
    if short_ratio > hi:
        return "📉 沽空偏高"

    # ── 7. Unusually low short for this type ──
    if short_ratio > 0 and short_ratio < lo * 0.5:
        return "📈 沽空偏低"

    return "✅ 正常"


# =========================
# MAIN ANALYSIS
# =========================
def run_analysis():
    today = datetime.now()
    trading_day = last_trading_day(today)
    log.info("=== Starting analysis — trading day: %s ===", trading_day.strftime("%Y-%m-%d"))

    # ── 1. Fetch daily quotation → Top 30 by turnover ──
    df_quote = get_daily_quotation(trading_day)
    if df_quote.empty:
        msg = "⚠️ 港股看板：今日日報表未能獲取，分析中止。"
        log.error(msg)
        send_telegram(msg)
        return

    # Save today's turnover for use as short ratio denominator in future runs
    save_daily_turnover(trading_day, df_quote)
    stock_codes   = df_quote["stock_code"].tolist()
    turnover_map  = dict(zip(df_quote["stock_code"], df_quote["turnover"]))

    # ── 2. Fetch short selling ──
    df_short = get_short_sell_today()
    save_short_sell(trading_day, df_short)

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

    # ── T-2 alignment ──
    # CCASS today reflects trades that settled 2 HK business days ago.
    # We look up the short ratio and turnover from that date so cross-signals
    # (機構增持 / 避險盤撤退) compare CCASS against data from when trades happened.
    t2_date     = business_days_back(trading_day, 2)
    t2_date_key = t2_date.strftime("%Y%m%d")
    short_history_store = load_json_store(SHORT_HISTORY_FILE)
    log.info("T-2 date for CCASS alignment: %s", t2_date_key)

    # ── 4. Fetch CCASS southbound ──
    df_ccass = get_ccass_southbound(trading_day)
    today_ccass_map = {}
    if not df_ccass.empty:
        today_ccass_map = dict(zip(df_ccass["stock_code"], df_ccass["shareholding"]))
        today_pct_map   = dict(zip(df_ccass["stock_code"], df_ccass["pct_listed"]))
    else:
        today_pct_map = {}

    # Save CCASS *after* computing deltas (so history still has yesterday)
    df_ccass_stats = get_ccass_delta_and_avg(stock_codes, today_ccass_map, days=5)
    save_ccass(trading_day, df_ccass)

    ccass_delta_map  = dict(zip(df_ccass_stats["stock_code"], df_ccass_stats["ccass_delta"]))
    ccass_avg5_map   = dict(zip(df_ccass_stats["stock_code"], df_ccass_stats["ccass_avg5"]))
    ccass_consec_map = dict(zip(df_ccass_stats["stock_code"], df_ccass_stats["ccass_consec"]))

    # ── 5. Load yesterday's rankings for comparison ──
    prev_ranks = get_prev_ranks()

    # ── 6. Build result rows ──
    results = []
    for i, row in enumerate(df_quote.itertuples(), 1):
        code = row.stock_code

        short_ratio  = short_map.get(code, 0.0)
        short_avg5   = short_avg_map.get(code, 0.0)
        ccass_pct    = today_pct_map.get(code, 0.0)
        ccass_delta  = ccass_delta_map.get(code, 0)
        ccass_avg5   = ccass_avg5_map.get(code, 0.0)
        ccass_consec = ccass_consec_map.get(code, 0)
        tv_avg5      = _turnover_avg5(code, daily_tv_store)

        # T-2 values: short ratio and turnover from the day CCASS trades actually occurred
        tv_t2_raw      = _value_at(code, t2_date_key, daily_tv_store)
        short_tv_t2    = _value_at(code, t2_date_key, short_history_store, field="short_turnover")
        short_ratio_t2 = round(short_tv_t2 / tv_t2_raw * 100, 2) if tv_t2_raw > 0 else short_ratio
        turnover_t2    = tv_t2_raw if tv_t2_raw > 0 else float(row.turnover)

        # ── Stock type + signal ──
        stock_type = classify_stock(code, row.name)
        insight = classify_insight(
            code            = code,
            stock_type      = stock_type,
            short_ratio     = short_ratio,
            short_avg5      = short_avg5,
            short_ratio_t2  = short_ratio_t2,
            turnover        = int(row.turnover),
            turnover_avg5   = tv_avg5,
            turnover_t2     = turnover_t2,
            ccass_pct       = ccass_pct,
            ccass_delta     = int(ccass_delta),
            ccass_avg5      = float(ccass_avg5),
            ccass_consec    = int(ccass_consec),
        )

        # ── Rank change vs previous trading day ──
        prev_rank  = prev_ranks.get(code)          # None if not in yesterday's Top 30
        rank_new   = prev_rank is None             # True = new entry today
        rank_change = 0 if rank_new else prev_rank - i  # positive = moved up

        results.append({
            "rank":           i,
            "rank_change":    rank_change,
            "rank_new":       rank_new,
            "code":           code,
            "name":           row.name,
            "name_chi":       getattr(row, 'name_chi', row.name),
            "stock_type":     stock_type,
            "turnover":       int(row.turnover),
            "short_ratio":    round(short_ratio, 2),
            "short_avg5":     round(short_avg5, 2),
            "short_ratio_t2": round(short_ratio_t2, 2),
            "ccass_trade_date": t2_date.strftime("%Y-%m-%d"),
            "ccass_pct":      round(ccass_pct, 2),
            "ccass_delta":    int(ccass_delta),
            "ccass_avg5":     int(ccass_avg5),
            "ccass_consec":   int(ccass_consec),
            "insight":        insight,
        })

    # ── 7. Persist output ──
    output = {
        "update_time": trading_day.strftime("%Y-%m-%d %H:%M"),
        "stocks":      results,
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    log.info("data.json written with %d stocks", len(results))

    # Save rankings for tomorrow's comparison
    save_rank_history(trading_day, results)

    # ── 8. Telegram summary ──
    if results:
        flagged  = [s for s in results if s["insight"] != "✅ 正常"]
        new_entries = [s for s in results if s["rank_new"]]
        big_movers  = [s for s in results if not s["rank_new"] and s["rank_change"] >= 5]
        top = results[0]
        top_rc = f" [↑{top['rank_change']}]" if top['rank_change'] > 0 else (" [new]" if top['rank_new'] else "")
        lines = [
            f"📊 港股 AI 看板更新",
            f"時間: {output['update_time']}",
            f"榜首: {top['name']} ({top['code']}){top_rc} 成交額 {top['turnover']:,}",
            f"異動股: {len(flagged)} 隻 | 新進榜: {len(new_entries)} 隻",
        ]
        if new_entries:
            lines.append("⭐ 新進 Top30: " + "、".join(f"{s['name']}({s['code']})" for s in new_entries[:3]))
        if big_movers:
            lines.append("🔺 大幅上升: " + "、".join(f"{s['name']} ↑{s['rank_change']}" for s in big_movers[:3]))
        if flagged:
            lines.append("─────────────")
            for s in flagged[:5]:
                rc = f" [↑{s['rank_change']}]" if s['rank_change'] > 0 else (" [new]" if s['rank_new'] else "")
                lines.append(f"{s['insight']} {s['name']}{rc} | 沽空率 {s['short_ratio']}% | CCASS Δ {s['ccass_delta']:,}")
        send_telegram("\n".join(lines))


if __name__ == "__main__":
    run_analysis()
