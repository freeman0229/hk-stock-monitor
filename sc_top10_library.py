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
         "buy":  4736016460, "sell": 8195273431, "total": 12931289891,
         "rank": 1, "rank_sse": 1, "rank_szse": 1},
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
START_DATE = date(2025, 1, 1)   # match ccass_library START_DATE; Jan-Aug 2025 data exists
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
            rank = _to_i(row[0])
            if rank <= 0: continue       # skip header/summary rows
            code_raw = row[1].strip()
            if not code_raw.isdigit(): continue   # skip invalid/placeholder rows
            stocks.append({
                "rank":  rank,
                "code":  code_raw.zfill(6),
                "name":  _clean_name(row[2]),
                "total": _to_i(row[3]),
            })
        else:
            # Southbound: rank, code, name, buy, sell, total
            if len(row) < 6: continue
            rank = _to_i(row[0])
            if rank <= 0: continue       # skip header/summary rows
            code_raw = row[1].strip()
            if not code_raw.isdigit(): continue   # skip invalid/placeholder rows (e.g. "-0")
            stocks.append({
                "rank":  rank,
                "code":  code_raw.zfill(5),
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

    # Combine SSE + SZSE: sum buy/sell/total per code, preserve per-exchange ranks.
    # Iterate each exchange separately so we know exactly which list each stock
    # came from — avoids the broken `is` identity check.
    merged = {}

    for code, s in sse_stocks.items():
        if code not in merged:
            merged[code] = {"code": code, "name": s["name"],
                            "buy": 0, "sell": 0, "total": 0, "rank": 99,
                            "rank_sse": None, "rank_szse": None}
        merged[code]["buy"]      += s["buy"]
        merged[code]["sell"]     += s["sell"]
        merged[code]["total"]    += s["total"]
        merged[code]["rank"]      = min(merged[code]["rank"], s["rank"])
        merged[code]["rank_sse"]  = s["rank"]   # always set — this IS an SSE stock

    for code, s in szse_stocks.items():
        if code not in merged:
            merged[code] = {"code": code, "name": s["name"],
                            "buy": 0, "sell": 0, "total": 0, "rank": 99,
                            "rank_sse": None, "rank_szse": None}
        merged[code]["buy"]       += s["buy"]
        merged[code]["sell"]      += s["sell"]
        merged[code]["total"]     += s["total"]
        merged[code]["rank"]       = min(merged[code]["rank"], s["rank"])
        merged[code]["rank_szse"]  = s["rank"]  # always set — this IS a SZSE stock

    # Minimum sanity check — HKEX publishes exactly 10 per exchange so we
    # should always have 10–20 unique stocks. Fewer means parse dropped rows.
    n_sse, n_szse, n_merged = len(sse_stocks), len(szse_stocks), len(merged)
    if n_sse < 10:
        log.warning("parse_js: only %d SSE stocks parsed (expected 10) — table structure may have changed", n_sse)
    if n_szse < 10:
        log.warning("parse_js: only %d SZSE stocks parsed (expected 10) — table structure may have changed", n_szse)
    if n_merged < 10:
        log.warning("parse_js: merged list has only %d stocks (expected ≥10)", n_merged)
    log.info("parse_js: SSE=%d SZSE=%d merged=%d (overlap=%d)",
             n_sse, n_szse, n_merged, n_sse + n_szse - n_merged)

    # Set the compat `rank` field = best single-exchange rank (for main.py sb_rank).
    # Must be done AFTER both exchange loops so both rank_sse and rank_szse are set.
    for v in merged.values():
        sse_r  = v["rank_sse"]  if v["rank_sse"]  is not None else 99
        szse_r = v["rank_szse"] if v["rank_szse"] is not None else 99
        v["rank"] = min(sse_r, szse_r)

    # Sort by combined total turnover descending.
    result["top10"] = sorted(merged.values(), key=lambda x: x["total"], reverse=True)
    return result


# ── Fetch ─────────────────────────────────────────────────────────────────────

def _cache_path(d: date) -> str:
    return os.path.join(CACHE_DIR, f"sc_{d.strftime('%Y%m%d')}.json")

def fetch_day(d: date) -> dict | None:
    """Fetch and parse southbound top 10 data for one trading day.
    Cache stores raw JS text so parse_js always runs fresh.
    If the cached file is stale (old JSON format), it is replaced."""
    cp = _cache_path(d)
    if os.path.exists(cp):
        with open(cp, encoding="utf-8") as f:
            raw = f.read()
        parsed = parse_js(raw)
        if parsed is not None:
            return parsed
        # Cache file is stale (old JSON format) — delete and re-fetch
        log.info("  stale cache for %s — re-fetching", d.isoformat())
        os.remove(cp)

    url = BASE_URL.format(date=d.strftime("%Y%m%d"))
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 404:
            log.warning("  404: %s", url)
            return None
        r.raise_for_status()
        with open(cp, "w", encoding="utf-8") as f:
            f.write(r.text)
        return parse_js(r.text)
    except Exception as e:
        log.error("  fetch failed (%s): %s", d.isoformat(), e)
        return None


# ── Build / update ────────────────────────────────────────────────────────────

def _is_valid_top10(rec: dict) -> bool:
    """Return True if the top10 record looks complete and valid."""
    top10 = rec.get("top10", [])
    if len(top10) < 10:
        return False
    # Check for garbage codes like "-0000" (placeholder rows from holiday stubs)
    valid_codes = [s for s in top10 if s.get("code", "").isdigit()]
    return len(valid_codes) >= 10

def _incomplete_dates() -> set:
    """Return stored dates where top10 has fewer than 10 stocks or contains invalid codes."""
    incomplete = set()
    for year in all_years():
        p = lib_path(year)
        if not os.path.exists(p): continue
        with open(p, encoding="utf-8") as f:
            by_date = json.load(f).get("by_date", {})
        for ds, rec in by_date.items():
            if not _is_valid_top10(rec):
                incomplete.add(ds)
    if incomplete:
        log.info("Incomplete dates (< 10 valid stocks): %d — will re-fetch", len(incomplete))
    return incomplete

def build(update_only: bool = False):
    stored  = all_stored_dates()
    end     = last_trading_day(date.today())   # include today — HKEX publishes same-day SC data
    trading = all_trading_days(last_trading_day(START_DATE), end)

    # Always include incomplete dates (stored but < 10 stocks) for re-fetch
    incomplete = _incomplete_dates()

    if update_only and stored:
        last    = date.fromisoformat(max(stored))
        new_days = [d for d in trading if d > last]
        # Add incomplete historical dates to the fetch list
        repair   = [d for d in trading if d.isoformat() in incomplete]
        trading  = repair + new_days
        log.info("Update: %d new + %d incomplete to repair", len(new_days), len(repair))
    else:
        trading = [d for d in trading if d.isoformat() not in stored or d.isoformat() in incomplete]
        log.info("Build: %d trading days to fetch (%d repairs)", len(trading), len(incomplete & {d.isoformat() for d in trading}))

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
        r_sse  = s.get("rank_sse")
        r_szse = s.get("rank_szse")
        print(f"{i:<5} {s['code']:<7} {s['name']:<14} "
              f"{fmt(s['buy']):>12} {fmt(s['sell']):>12} {fmt(s['total']):>12} "
              f"{'#'+str(r_sse)  if r_sse  else '-':>4} "
              f"{'#'+str(r_szse) if r_szse else '-':>4}")


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
    ap.add_argument("--update",      action="store_true",  help="Fetch new dates + repair incomplete entries")
    ap.add_argument("--query",       metavar="YYYY-MM-DD", help="Show data for a date")
    ap.add_argument("--export",      action="store_true",  help="Export all data to CSV")
    ap.add_argument("--clear-cache", action="store_true",  help="Delete sc_cache/ so JS is re-fetched and re-parsed")
    ap.add_argument("--reparse",     action="store_true",  help="Delete all incomplete year-JSON entries and re-fetch from HKEX")
    args = ap.parse_args()

    if args.clear_cache:
        import shutil
        if os.path.exists(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)
            os.makedirs(CACHE_DIR)
            log.info("sc_cache cleared — re-run without --clear-cache to rebuild")
    elif args.reparse:
        # Remove incomplete entries from year JSON so build() re-fetches them
        removed = 0
        for year in all_years():
            p = lib_path(year)
            if not os.path.exists(p): continue
            with open(p, encoding="utf-8") as f:
                lib = json.load(f)
            before = len(lib.get("by_date", {}))
            lib["by_date"] = {ds: rec for ds, rec in lib.get("by_date", {}).items()
                              if len(rec.get("top10", [])) >= 10}
            after = len(lib["by_date"])
            if before != after:
                save_year(year, lib)
                removed += before - after
                log.info("Year %d: removed %d incomplete entries, kept %d", year, before-after, after)
        log.info("Reparse: removed %d incomplete entries total — run --update to re-fetch", removed)
        build(update_only=False)
    elif args.query:  query_date(args.query)
    elif args.export: export_csv()
    else:             build(update_only=args.update)
