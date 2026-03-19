"""
sc_top10_library.py — HKEX Stock Connect Southbound Top 10 Library
===================================================================
Fetches daily top 10 most actively traded stocks by southbound investors
from HKEX Stock Connect Historical Daily statistics.

Source URL pattern:
  https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{YYYYMMDD}c.js

Data: SSE Southbound + SZSE Southbound top 10 each (combined up to 20 unique stocks)

Library files: sc_top10_{YYYY}.json — one per year

Structure:
{
  "meta": {"year": 2026, "last_updated": "...", "total_days": N},
  "by_date": {
    "2026-03-04": {
      "sse_summary":  {"total": 89251.46, "buy": 44080.13, "sell": 45171.33,
                       "trades": 1658014, "etf": 2626.27},
      "szse_summary": {"total": 52874.61, "buy": 26749.89, "sell": 26124.72,
                       "trades":  990929, "etf": 1661.55},
      "top10": [
        {"code": "09988", "name": "阿里巴巴－Ｗ",
         "sse_buy": 2752383420, "sse_sell": 4389977600, "sse_total": 7142361020,
         "szse_buy":1983633040, "szse_sell":3805295831, "szse_total":5788928871,
         "buy":  4736016460, "sell": 8195273431, "total": 12931289891,
         "rank_sse": 1, "rank_szse": 1},
        ...
      ]
    }
  }
}

Usage:
  python sc_top10_library.py              # full build 2018 to today
  python sc_top10_library.py --update     # only new dates
  python sc_top10_library.py --query 2026-03-04
  python sc_top10_library.py --export     # export all to CSV
"""

import os, sys, json, time, re, logging, argparse
import requests
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL   = "https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{date}c.js"
START_DATE = date(2022, 1, 1)
SLEEP_SEC  = 1.2
CACHE_DIR  = "sc_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)",
    "Referer":    "https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=zh-HK",
}

# ── Trading day helpers ───────────────────────────────────────────────────────

try:
    import holidays as _hol
    _HK = _hol.HongKong()
except Exception:
    _HK = set()

def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in _HK

def last_trading_day(d: date) -> date:
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d

def all_trading_days(start: date, end: date) -> list:
    days, d = [], start
    while d <= end:
        if is_trading_day(d): days.append(d)
        d += timedelta(days=1)
    return days


# ── File I/O ──────────────────────────────────────────────────────────────────

def lib_path(year: int) -> str:
    return f"sc_top10_{year}.json"

def all_years() -> list:
    return list(range(START_DATE.year, date.today().year + 1))

def load_year(year: int) -> dict:
    p = lib_path(year)
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return {"meta": {"year": year}, "by_date": {}}

def save_year(year: int, lib: dict):
    lib["meta"] = {
        "year":         year,
        "last_updated": date.today().isoformat(),
        "total_days":   len(lib["by_date"]),
    }
    with open(lib_path(year), "w", encoding="utf-8") as f:
        json.dump(lib, f, ensure_ascii=False, separators=(",", ":"))
    mb = os.path.getsize(lib_path(year)) / 1e6
    log.info("Saved sc_top10_%d.json: %d days, %.2f MB",
             year, len(lib["by_date"]), mb)

def all_stored_dates() -> set:
    stored = set()
    for year in all_years():
        if os.path.exists(lib_path(year)):
            with open(lib_path(year), encoding="utf-8") as f:
                stored.update(json.load(f).get("by_date", {}).keys())
    return stored


# ── Parse ─────────────────────────────────────────────────────────────────────

def _to_f(s: str) -> float:
    try:    return float(str(s).replace(",", "").strip())
    except: return 0.0

def _to_i(s: str) -> int:
    try:    return int(str(s).replace(",", "").strip())
    except: return 0

def _clean_name(s: str) -> str:
    return s.strip().rstrip("　").strip()

def _parse_summary(table: dict) -> dict:
    """Parse style:1 tradingTable — aggregate figures."""
    schema = table.get("schema", [[]])[0]
    trs    = table.get("tr", [])
    vals   = [tr["td"][0][0] for tr in trs if tr.get("td")]
    result = {}
    for key, val in zip(schema, vals):
        k = key.lower().replace(" ", "_")
        # trade counts are integers, turnover is float
        result[k] = _to_i(val) if "count" in k or "dqb" in k else _to_f(val)
    # Normalise key names
    return {
        "total":  result.get("total_turnover",    0.0),
        "buy":    result.get("buy_turnover",      0.0),
        "sell":   result.get("sell_turnover",     0.0),
        "trades": _to_i(str(int(result.get("total_trade_count", 0)))),
        "etf":    result.get("etf_turnover",      0.0),
    }

def _parse_top10(table: dict, is_southbound: bool) -> list:
    """Parse style:2 top10Table — per-stock rows."""
    stocks = []
    for tr in table.get("tr", []):
        tds = tr.get("td", [])
        if not tds: continue
        row = tds[0]
        if not is_southbound:
            # Northbound: rank, code, name, total
            if len(row) < 4: continue
            stocks.append({
                "rank":  _to_i(row[0]),
                "code":  row[1].strip().zfill(6),
                "name":  _clean_name(row[2]),
                "total": _to_i(row[3]),
            })
        else:
            # Southbound: rank, code, name, buy, sell, total
            if len(row) < 6: continue
            stocks.append({
                "rank":  _to_i(row[0]),
                "code":  row[1].strip().zfill(5),
                "name":  _clean_name(row[2]),
                "buy":   _to_i(row[3]),
                "sell":  _to_i(row[4]),
                "total": _to_i(row[5]),
            })
    return stocks

def parse_js(text: str) -> dict | None:
    """
    Parse the tabData JS variable.
    Returns a dict with sse_summary, szse_summary, top10 combined.
    """
    # Strip JS variable assignment to get pure JSON
    m = re.search(r'tabData\s*=\s*(\[.*\])', text, re.DOTALL)
    if not m:
        log.warning("tabData not found in JS")
        return None

    try:
        tab = json.loads(m.group(1))
    except json.JSONDecodeError as e:
        log.warning("JSON parse error: %s", e)
        return None

    # Index by market name
    markets = {item["market"]: item for item in tab}

    result = {}

    # SSE Southbound
    sse_sb = markets.get("SSE Southbound")
    sse_stocks = {}
    if sse_sb:
        for content in sse_sb.get("content", []):
            tbl = content.get("table", {})
            if content["style"] == 1:
                result["sse_summary"] = _parse_summary(tbl)
            elif content["style"] == 2:
                for s in _parse_top10(tbl, is_southbound=True):
                    sse_stocks[s["code"]] = s

    # SZSE Southbound
    szse_sb = markets.get("SZSE Southbound")
    szse_stocks = {}
    if szse_sb:
        for content in szse_sb.get("content", []):
            tbl = content.get("table", {})
            if content["style"] == 1:
                result["szse_summary"] = _parse_summary(tbl)
            elif content["style"] == 2:
                for s in _parse_top10(tbl, is_southbound=True):
                    szse_stocks[s["code"]] = s

    # Merge SSE + SZSE top10 into combined ranking by total turnover
    all_codes = set(sse_stocks) | set(szse_stocks)
    combined  = []
    for code in all_codes:
        e = sse_stocks.get(code, {})
        z = szse_stocks.get(code, {})
        name     = _clean_name(e.get("name") or z.get("name") or "")
        sse_buy  = e.get("buy",   0)
        sse_sell = e.get("sell",  0)
        sse_tot  = e.get("total", 0)
        sz_buy   = z.get("buy",   0)
        sz_sell  = z.get("sell",  0)
        sz_tot   = z.get("total", 0)
        combined.append({
            "code":       code,
            "name":       name,
            "sse_buy":    sse_buy,
            "sse_sell":   sse_sell,
            "sse_total":  sse_tot,
            "szse_buy":   sz_buy,
            "szse_sell":  sz_sell,
            "szse_total": sz_tot,
            "buy":        sse_buy  + sz_buy,
            "sell":       sse_sell + sz_sell,
            "total":      sse_tot  + sz_tot,
            "rank_sse":   e.get("rank", 0),
            "rank_szse":  z.get("rank", 0),
        })

    # Sort by combined total descending
    combined.sort(key=lambda x: x["total"], reverse=True)
    result["top10"] = combined
    return result


# ── Fetch ─────────────────────────────────────────────────────────────────────

def _cache_path(d: date) -> str:
    return os.path.join(CACHE_DIR, f"sc_{d.strftime('%Y%m%d')}.json")

def fetch_day(d: date) -> dict | None:
    """Fetch and parse southbound top 10 data for one trading day."""
    cp = _cache_path(d)
    if os.path.exists(cp):
        with open(cp, encoding="utf-8") as f:
            return json.load(f)

    url = BASE_URL.format(date=d.strftime("%Y%m%d"))
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 404:
            log.warning("  404: %s", url)
            return None
        r.raise_for_status()
        parsed = parse_js(r.text)
        if parsed:
            with open(cp, "w", encoding="utf-8") as f:
                json.dump(parsed, f, ensure_ascii=False, separators=(",", ":"))
        return parsed
    except Exception as e:
        log.error("  fetch failed (%s): %s", d.isoformat(), e)
        return None


# ── Build / update ────────────────────────────────────────────────────────────

def build(update_only: bool = False):
    stored  = all_stored_dates()
    end     = last_trading_day(date.today() - timedelta(days=1))
    trading = all_trading_days(last_trading_day(START_DATE), end)

    if update_only and stored:
        last    = date.fromisoformat(max(stored))
        trading = [d for d in trading if d > last]
        log.info("Update: %d new trading days after %s", len(trading), last.isoformat())
    else:
        trading = [d for d in trading if d.isoformat() not in stored]
        log.info("Build: %d trading days to fetch", len(trading))

    if not trading:
        log.info("Already up to date"); return

    by_year: dict = {}
    for d in trading:
        by_year.setdefault(d.year, []).append(d)

    missing = []
    consec_404 = 0
    MAX_CONSEC_404 = 30   # stop if 30 consecutive days 404 — hit archive limit

    for year, days in sorted(by_year.items()):
        lib = load_year(year)
        log.info("── Year %d: %d days ──", year, len(days))
        for i, d in enumerate(days, 1):
            rec = fetch_day(d)
            if rec:
                lib["by_date"][d.isoformat()] = rec
                top = rec.get("top10", [])
                names = ", ".join(s["code"] for s in top[:3])
                log.info("  [%d/%d] %s  %d stocks  top: %s",
                         i, len(days), d.isoformat(), len(top), names)
                consec_404 = 0
            else:
                missing.append(d)
                consec_404 += 1
                log.warning("  [%d/%d] %s  no data (%d consec)", i, len(days), d.isoformat(), consec_404)
                if consec_404 >= MAX_CONSEC_404:
                    log.warning("  %d consecutive 404s — HKEX archive limit reached, stopping", MAX_CONSEC_404)
                    save_year(year, lib)
                    break
            time.sleep(SLEEP_SEC)
            if i % 20 == 0:
                save_year(year, lib)
        else:
            save_year(year, lib)
            continue
        save_year(year, lib)
        break   # break outer loop too

    log.info("── Done ──")
    if missing:
        log.warning("%d dates missing: %s%s",
                    len(missing),
                    [d.isoformat() for d in missing[:5]],
                    "..." if len(missing) > 5 else "")


# ── Query ─────────────────────────────────────────────────────────────────────

def query_date(ds: str):
    year = int(ds[:4])
    if not os.path.exists(lib_path(year)):
        print(f"No library for {year}"); return
    with open(lib_path(year), encoding="utf-8") as f:
        rec = json.load(f).get("by_date", {}).get(ds)
    if not rec:
        print(f"No data for {ds}"); return

    def fmt(n): return f"{n/1e8:>8.2f}億"

    ss = rec.get("sse_summary",  {})
    zs = rec.get("szse_summary", {})
    print(f"\n港股通十大活躍證券 — {ds}")
    print(f"{'':4} {'SSE 南向':>12}  {'SZSE 南向':>12}")
    print(f"  Total  {fmt(ss.get('total',0)*1e6)}  {fmt(zs.get('total',0)*1e6)}")
    print(f"  Buy    {fmt(ss.get('buy',0)*1e6)}  {fmt(zs.get('buy',0)*1e6)}")
    print(f"  Sell   {fmt(ss.get('sell',0)*1e6)}  {fmt(zs.get('sell',0)*1e6)}")
    print(f"\n{'Rank':<5} {'Code':<7} {'Name':<14} {'Buy':>12} {'Sell':>12} {'Total':>12} {'SSE':>4} {'SZ':>4}")
    print("─" * 72)
    for i, s in enumerate(rec.get("top10", []), 1):
        print(f"{i:<5} {s['code']:<7} {s['name']:<14} "
              f"{fmt(s['buy']):>12} {fmt(s['sell']):>12} {fmt(s['total']):>12} "
              f"{'#'+str(s['rank_sse']) if s['rank_sse'] else '-':>4} "
              f"{'#'+str(s['rank_szse']) if s['rank_szse'] else '-':>4}")


# ── API for main.py ───────────────────────────────────────────────────────────

def get_top10(ds: str) -> list:
    """Return top10 list for a date string YYYY-MM-DD. Empty list if not found."""
    year = int(ds[:4])
    p    = lib_path(year)
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8") as f:
        rec = json.load(f).get("by_date", {}).get(ds, {})
    return rec.get("top10", [])

def get_top10_codes(ds: str) -> set:
    """Return set of stock codes in top10 for a date."""
    return {s["code"] for s in get_top10(ds)}

def get_top10_history(code: str, n: int, before: str) -> list:
    """
    Return last n days where code appeared in top10, before date `before`.
    Each entry: {"date": "YYYY-MM-DD", "buy": int, "sell": int, "total": int}
    """
    code5  = code.zfill(5)
    result = []
    for year in sorted(all_years(), reverse=True):
        p = lib_path(year)
        if not os.path.exists(p): continue
        with open(p, encoding="utf-8") as f:
            by_date = json.load(f).get("by_date", {})
        for ds in sorted(by_date.keys(), reverse=True):
            if ds >= before: continue
            for s in by_date[ds].get("top10", []):
                if s["code"] == code5:
                    result.append({"date": ds, "buy": s["buy"],
                                   "sell": s["sell"], "total": s["total"]})
                    break
            if len(result) >= n: return result
    return result


# ── Export ────────────────────────────────────────────────────────────────────

def export_csv():
    rows = []
    for year in all_years():
        if not os.path.exists(lib_path(year)): continue
        with open(lib_path(year), encoding="utf-8") as f:
            by_date = json.load(f).get("by_date", {})
        for ds, rec in sorted(by_date.items()):
            for s in rec.get("top10", []):
                rows.append({"date": ds, **s})
    if not rows:
        print("No data to export"); return
    import csv
    path = "sc_top10_history.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)
    print(f"Exported {len(rows)} rows to {path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="HKEX Southbound Top 10 Library")
    ap.add_argument("--update", action="store_true",  help="Fetch only new dates")
    ap.add_argument("--query",  metavar="YYYY-MM-DD", help="Show data for a date")
    ap.add_argument("--export", action="store_true",  help="Export all data to CSV")
    args = ap.parse_args()

    if   args.query:  query_date(args.query)
    elif args.export: export_csv()
    else:             build(update_only=args.update)
