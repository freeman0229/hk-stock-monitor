"""
ccass_sdw_library.py — CCASS Per-Stock Participant Holdings Library
====================================================================
Fetches weekly CCASS participant-level shareholding for stocks with
turnover > 20,000,000 HKD. This is the "大戶持倉" data.

Schedule: every Friday. If Friday is a HK public holiday → use Thursday.
          If both Thursday and Friday are holidays → skip that week.
Start:    2025-03-21

Source (holdings): https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx
Source (filter):   https://www.hkex.com.hk/chi/stat/smstat/dayquot/d{YYMMDD}c.htm

Columns captured (1, 2, 4, 5 — col 3 address skipped):
  1. 中央結算系統參與者編號  — Participant ID
  2. 中央結算系統參與者名稱  — Participant Name
  4. 持股量                  — Shareholding (shares)
  5. 佔已發行股份%            — % of issued shares

Library files: ccass_sdw_{YYYY}.json — one per year

Structure:
{
  "meta": {"year": 2026, "last_updated": "...", "total_dates": N, "total_stocks": N},
  "by_date": {
    "2026-03-19": {
      "00700": [
        {"pid": "B01234", "name": "中信証券經紀(香港)有限公司",
         "sh": 123456789, "pct": 1.23},
        ...
      ]
    }
  }
}

Usage:
  python ccass_sdw_library.py                    # full backfill from 2025-03-21
  python ccass_sdw_library.py --update           # only new dates not yet stored
  python ccass_sdw_library.py --date 2026-03-19  # fetch one specific date
  python ccass_sdw_library.py --query 00700      # show top 20 holders
  python ccass_sdw_library.py --query 00700 --top 10

API:
  from ccass_sdw_library import get_holders, get_holders_history, save_day
"""

import os, json, re, time, logging, argparse
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SDW_URL  = "https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx"
QUOT_URL = "https://www.hkex.com.hk/chi/stat/smstat/dayquot/d{date}c.htm"
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)",
    "Referer":    "https://www3.hkexnews.hk/",
}
QUOT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)",
    "Referer":    "https://www.hkex.com.hk/",
}
START_DATE    = date(2025, 3, 21)
MIN_TURNOVER  = 66_600_000
SLEEP_SEC     = 1.5

# HK public holidays for the library range
_HK_HOLIDAYS = {
    date(2025,4,4),  date(2025,4,18), date(2025,4,19), date(2025,4,21),
    date(2025,5,1),  date(2025,5,5),  date(2025,7,1),  date(2025,10,1),
    date(2025,10,7), date(2025,12,25),date(2025,12,26),
    date(2026,1,1),  date(2026,1,28), date(2026,1,29), date(2026,1,30),
    date(2026,2,2),  date(2026,2,3),  date(2026,2,4),
    date(2026,3,20), date(2026,4,3),  date(2026,4,6),
    date(2026,5,1),  date(2026,5,4),  date(2026,5,5),
    date(2026,6,19), date(2026,7,1),  date(2026,10,1),
    date(2026,10,5), date(2026,10,6), date(2026,10,7), date(2026,10,8),
    date(2026,12,25),date(2026,12,26),
}
try:
    import holidays as _hol
    _HK_HOLIDAYS = _HK_HOLIDAYS | set(_hol.HongKong())
except ImportError:
    pass


# ── Schedule ──────────────────────────────────────────────────────────────────

def weekly_fetch_date(friday: date) -> date | None:
    """Friday → use it. Friday holiday → try Thursday. Both holiday → None (skip)."""
    thu = friday - timedelta(days=1)
    if friday not in _HK_HOLIDAYS: return friday
    if thu    not in _HK_HOLIDAYS: return thu
    return None

def all_fetch_dates(up_to: date = None) -> list:
    up_to  = up_to or date.today()
    result = []
    d = START_DATE
    while d.weekday() != 4:
        d += timedelta(days=1)
    while d <= up_to:
        fd = weekly_fetch_date(d)
        if fd:
            result.append(fd)
        d += timedelta(weeks=1)
    return result


# ── File I/O ──────────────────────────────────────────────────────────────────

def lib_path(year: int) -> str:
    return f"ccass_sdw_{year}.json"

def load_year(year: int) -> dict:
    p = lib_path(year)
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return {"meta": {"year": year}, "by_date": {}}

def save_year(year: int, lib: dict):
    n_dates  = len(lib["by_date"])
    n_stocks = sum(len(v) for v in lib["by_date"].values())
    lib["meta"] = {
        "year":         year,
        "last_updated": date.today().isoformat(),
        "total_dates":  n_dates,
        "total_stocks": n_stocks,
    }
    with open(lib_path(year), "w", encoding="utf-8") as f:
        json.dump(lib, f, ensure_ascii=False, separators=(",", ":"))
    kb = os.path.getsize(lib_path(year)) / 1024
    log.info("Saved ccass_sdw_%d.json  %d dates  %d stocks  %.0f KB",
             year, n_dates, n_stocks, kb)

def all_stored_dates() -> set:
    stored = set()
    for year in range(START_DATE.year, date.today().year + 1):
        p = lib_path(year)
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                stored.update(json.load(f).get("by_date", {}).keys())
    return stored

def save_day(d: date, stock_code: str, records: list):
    if not records:
        return
    ds  = d.isoformat()
    lib = load_year(d.year)
    lib["by_date"].setdefault(ds, {})[stock_code.zfill(5)] = records
    save_year(d.year, lib)


# ── Turnover filter ───────────────────────────────────────────────────────────

def get_qualifying_stocks(d: date) -> list:
    """Return stock codes with turnover >= MIN_TURNOVER on date d."""
    date_str = d.strftime("%y%m%d")
    url = QUOT_URL.format(date=date_str)
    try:
        r = requests.get(url, headers=QUOT_HEADERS, timeout=30)
        r.raise_for_status()
        try:
            text = r.content.decode("big5", errors="replace")
        except Exception:
            text = r.content.decode("latin-1", errors="replace")

        pre  = BeautifulSoup(text, "html.parser").find("pre")
        body = pre.get_text() if pre else text

        PAT = re.compile(
            r"^[\*\s]{0,5}(\d{1,5})\s+([A-Z][A-Z0-9 \-&'./#+]{1,22}?)\s{2,}"
            r"(.{1,30}?)\s*(?:HKD|USD|CNY|EUR|GBP)\s+"
            r"[\d,.]+\s+[\d,.]+\s+[\d,.]+\s+[\d,.]+\s+[\d,.]+\s+[\d,.]+\s+"
            r"[\d,.]+\s+[\d,]{5,}\s+([\d,]{8,})\s*$"
        )
        codes = []
        for line in body.splitlines():
            m = PAT.match(line)
            if not m:
                continue
            code_int = int(m.group(1))
            if code_int > 9999:
                continue
            tv = float(m.group(4).replace(",", ""))
            if tv >= MIN_TURNOVER:
                codes.append(str(code_int).zfill(5))

        log.info("Qualifying stocks %s: %d (tv ≥ %s HKD)",
                 d.isoformat(), len(codes), f"{MIN_TURNOVER:,}")
        return codes

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            log.warning("Quotation 404 for %s", date_str)
        else:
            log.error("get_qualifying_stocks (%s): %s", date_str, e)
        return []
    except Exception as e:
        log.error("get_qualifying_stocks (%s): %s", date_str, e)
        return []


# ── SDW fetch for one stock ───────────────────────────────────────────────────

def fetch_stock(stock_code: str, d: date) -> list | None:
    """Fetch CCASS participant holdings for one stock on one date.
    Returns [{pid, name, sh, pct}] sorted by sh desc, or None on failure."""
    code5    = stock_code.zfill(5)
    date_str = d.strftime("%Y/%m/%d")
    try:
        sess = requests.Session()
        sess.headers.update(HEADERS)

        r1 = sess.get(SDW_URL, timeout=30)
        r1.raise_for_status()
        soup1 = BeautifulSoup(r1.text, "html.parser")

        def hv(name):
            tag = soup1.find("input", {"name": name})
            return tag["value"] if tag else ""

        r2 = sess.post(SDW_URL, data={
            "__EVENTTARGET":        "btnSearch",
            "__EVENTARGUMENT":      "",
            "__VIEWSTATE":          hv("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": hv("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION":    hv("__EVENTVALIDATION"),
            "txtShareholdingDate":  date_str,
            "txtStockCode":         code5,
            "txtParticipantID":     "",
            "txtParticipantName":   "",
        }, timeout=60)
        r2.raise_for_status()

        records = []
        for tr in BeautifulSoup(r2.text, "html.parser").find_all("tr"):
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 5:
                continue

            def clean(s):
                return re.sub(r'^[^:：]+[:：]\s*', '', s).strip()

            pid_raw = clean(tds[0])
            sh_raw  = clean(tds[3]).replace(",", "")
            pct_raw = clean(tds[4]).replace("%", "").strip()

            if not pid_raw or not sh_raw.isdigit():
                continue
            if pid_raw.lower() in ("參與者編號", "id", "participant id"):
                continue
            try:
                records.append({
                    "pid":  pid_raw,
                    "name": clean(tds[1]),
                    "sh":   int(sh_raw),
                    "pct":  float(pct_raw) if pct_raw else 0.0,
                })
            except (ValueError, TypeError):
                continue

        if not records:
            log.warning("SDW: 0 records for %s on %s", code5, date_str)
            return None

        records.sort(key=lambda x: -x["sh"])
        return records

    except Exception as e:
        log.error("fetch_stock (%s %s): %s", code5, date_str, e)
        return None


# ── Build / update ────────────────────────────────────────────────────────────

def build(update_only: bool = False, specific_date: date = None):
    if specific_date:
        dates_to_fetch = [specific_date]
    else:
        all_dates = all_fetch_dates()
        stored    = all_stored_dates()
        dates_to_fetch = [d for d in all_dates if d.isoformat() not in stored]
        log.info("%s: %d dates to fetch (%d already stored, %d total in schedule)",
                 "Update" if update_only else "Build",
                 len(dates_to_fetch), len(stored), len(all_dates))

    if not dates_to_fetch:
        log.info("Already up to date")
        return

    for di, d in enumerate(dates_to_fetch, 1):
        log.info("── [%d/%d] %s ──", di, len(dates_to_fetch), d.isoformat())

        codes = get_qualifying_stocks(d)
        if not codes:
            log.warning("No qualifying stocks for %s — skipping", d.isoformat())
            continue

        lib = load_year(d.year)
        ds  = d.isoformat()
        already = set(lib["by_date"].get(ds, {}).keys())
        todo    = [c for c in codes if c not in already]
        log.info("  %d stocks to fetch (%d already stored)", len(todo), len(already))

        fetched = 0
        for ci, code in enumerate(todo, 1):
            records = fetch_stock(code, d)
            if records:
                lib["by_date"].setdefault(ds, {})[code] = records
                fetched += 1
            time.sleep(SLEEP_SEC)
            if ci % 20 == 0:
                save_year(d.year, lib)
                log.info("    [%d/%d] %d saved so far", ci, len(todo), fetched)

        save_year(d.year, lib)
        log.info("  %s done: %d/%d stocks saved", ds, fetched, len(todo))

    log.info("Build complete")


# ── API ───────────────────────────────────────────────────────────────────────

def get_holders(stock_code: str, ds: str) -> list:
    """Return [{pid, name, sh, pct}] for a stock on YYYY-MM-DD, or []."""
    year = int(ds[:4])
    p    = lib_path(year)
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8") as f:
        return json.load(f).get("by_date", {}).get(ds, {}).get(stock_code.zfill(5), [])

def get_holders_history(stock_code: str, n: int, before: str) -> list:
    """Last n weekly snapshots before `before` (YYYY-MM-DD), newest-first.
    Returns [{"date": ds, "holders": [...]}, ...]"""
    code5  = stock_code.zfill(5)
    result = []
    for year in sorted(range(START_DATE.year, date.today().year + 1), reverse=True):
        p = lib_path(year)
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8") as f:
            by_date = json.load(f).get("by_date", {})
        for ds in sorted(by_date.keys(), reverse=True):
            if ds >= before:
                continue
            holders = by_date[ds].get(code5)
            if holders:
                result.append({"date": ds, "holders": holders})
            if len(result) >= n:
                return result
    return result


# ── CLI ───────────────────────────────────────────────────────────────────────

def _query(code: str, top: int, ds: str = None):
    code5 = code.zfill(5)
    if not ds:
        for year in sorted(range(START_DATE.year, date.today().year + 1), reverse=True):
            p = lib_path(year)
            if not os.path.exists(p): continue
            with open(p, encoding="utf-8") as f:
                by_date = json.load(f).get("by_date", {})
            dates = [d for d, s in by_date.items() if code5 in s]
            if dates: ds = max(dates); break
    if not ds:
        print(f"No data for {code5}"); return
    holders = get_holders(code5, ds)
    if not holders:
        print(f"No holders for {code5} on {ds}"); return
    print(f"\n{code5}  {ds}  ({len(holders)} participants)")
    print(f"{'#':<4} {'ID':<12} {'Name':<40} {'Shares':>16} {'%':>8}")
    print("─" * 84)
    for i, h in enumerate(holders[:top], 1):
        print(f"{i:<4} {h['pid']:<12} {h['name'][:39]:<40} {h['sh']:>16,} {h['pct']:>7.2f}%")
    hist = get_holders_history(code5, 6, (date.today()+timedelta(1)).isoformat())
    if hist:
        print(f"\nAvailable dates: {[h['date'] for h in hist]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--update", action="store_true", help="Only fetch missing dates")
    ap.add_argument("--date",   metavar="YYYY-MM-DD", help="Fetch one specific date")
    ap.add_argument("--query",  metavar="CODE",       help="Show stored holders")
    ap.add_argument("--top",    type=int, default=20)
    args = ap.parse_args()

    if args.query:
        _query(args.query, args.top, args.date)
    else:
        build(update_only=args.update,
              specific_date=date.fromisoformat(args.date) if args.date else None)
