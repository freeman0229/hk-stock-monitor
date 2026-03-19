"""
SFC Short Position Library Builder
====================================
Builds and maintains year-split JSON files of SFC weekly aggregated short
position data. One file per year keeps each under GitHub's 100MB limit.

Library files: sfc_{YYYY}.json  (one per year)
Structure:
{
    "meta": {
        "year": 2026,
        "last_updated": "2026-03-14",
        "total_weeks": 12,
        "total_records": 15000
    },
    "by_date": {
        "2026-03-14": {
            "00700": {
                "name": "TENCENT HOLDINGS LIMITED",
                "short_shares": 123456789,
                "short_value_hkd": 9876543210,
                "pct_issued": 1.23
            },
            ...
        }
    }
}

Usage:
  python sfc_library.py              # full build 2018 to today
  python sfc_library.py --update     # only fetch dates newer than last stored
  python sfc_library.py --query 00700           # full history across all years
  python sfc_library.py --query 00700 --weeks 52
  python sfc_library.py --date 2026-03-14
  python sfc_library.py --export 00700
"""

import os, json, time, logging, argparse
import requests
import pandas as pd
from datetime import date, timedelta
from io import StringIO

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

HEADERS    = {"User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)"}
CACHE_DIR  = "sfc_cache"
START_DATE = date(2018, 3, 1)
SLEEP_SEC  = 0.5

os.makedirs(CACHE_DIR, exist_ok=True)


# ── File paths ────────────────────────────────────────────────────────────────

def lib_path(year: int) -> str:
    return f"sfc_{year}.json"

def all_years() -> list[int]:
    return list(range(START_DATE.year, date.today().year + 1))


# ── Date helpers ──────────────────────────────────────────────────────────────

def all_fridays(start: date, end: date):
    d = start
    while d.weekday() != 4:
        d += timedelta(days=1)
    while d <= end:
        yield d
        d += timedelta(weeks=1)

def fmt(d: date) -> str:
    return d.isoformat()


# ── Fetch ─────────────────────────────────────────────────────────────────────

def _urls(d: date):
    base = f"https://www.sfc.hk/-/media/EN/pdf/spr/{d.year}/{d.month:02d}/{d.day:02d}/"
    ds   = d.strftime("%Y%m%d")
    return [
        base + f"Short_Position_Reporting_Aggregated_Data_{ds}.csv",
        base + f"Short_Position_Reporting_Aggregated_Data_Eng_{ds}.csv",
    ]

def fetch_raw(d: date) -> bytes | None:
    cache = os.path.join(CACHE_DIR, f"{d.strftime('%Y%m%d')}.csv")
    if os.path.exists(cache):
        with open(cache, "rb") as f:
            data = f.read()
        if len(data) > 100:
            return data
    for url in _urls(d):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and len(r.content) > 100:
                with open(cache, "wb") as f:
                    f.write(r.content)
                log.info("  fetched %s (%d bytes)", fmt(d), len(r.content))
                return r.content
        except Exception:
            pass
        time.sleep(0.2)
    return None


# ── Parse ─────────────────────────────────────────────────────────────────────

def parse_csv(raw: bytes, d: date) -> dict | None:
    try:
        text = raw.decode("utf-8-sig", errors="replace")
        df   = pd.read_csv(StringIO(text))
    except Exception as e:
        log.warning("  parse error %s: %s", fmt(d), e)
        return None

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    rename = {}
    for c in df.columns:
        if "stock_code" in c or c == "stockcode":                   rename[c] = "code"
        elif "stock_name" in c or c in ("stockname","name"):        rename[c] = "name"
        elif "shares" in c and "position" in c and "hk" not in c:  rename[c] = "ss"
        elif any(x in c for x in ["hk$","hkd","value"]) and "position" in c: rename[c] = "sv"
        elif "%" in c or "percent" in c or "pct" in c or "issued" in c: rename[c] = "pct"
    df = df.rename(columns=rename)

    if "code" not in df.columns or "ss" not in df.columns:
        log.warning("  missing columns %s: %s", fmt(d), list(df.columns))
        return None

    def to_num(val):
        try:    return float(str(val).replace(",","").strip())
        except: return None

    records = {}
    for _, row in df.iterrows():
        raw_code = str(row.get("code","")).strip()
        if not raw_code or raw_code.lower() in ("nan","stock code","code"):
            continue
        try:    code = str(int(float(raw_code))).zfill(5)
        except: code = raw_code.zfill(5)
        # Compact keys: n=name, s=short_shares, v=value_hkd, p=pct_issued
        records[code] = {
            "n": str(row.get("name","")).strip(),
            "s": to_num(row.get("ss")),
            "v": to_num(row.get("sv")),
            "p": to_num(row.get("pct")),
        }
    return records or None


# ── Year file I/O ─────────────────────────────────────────────────────────────

def load_year(year: int) -> dict:
    path = lib_path(year)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"meta": {"year": year}, "by_date": {}}

def save_year(year: int, lib: dict):
    dates = sorted(lib["by_date"].keys())
    total = sum(len(v) for v in lib["by_date"].values())
    lib["meta"] = {
        "year":          year,
        "last_updated":  date.today().isoformat(),
        "total_weeks":   len(dates),
        "total_records": total,
    }
    path = lib_path(year)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(lib, f, ensure_ascii=False, separators=(",",":"))
    size_mb = os.path.getsize(path) / 1e6
    log.info("Saved %s: %d weeks, %d records, %.1f MB",
             path, len(dates), total, size_mb)

def all_stored_dates() -> set:
    stored = set()
    for year in all_years():
        path = lib_path(year)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                lib = json.load(f)
            stored.update(lib.get("by_date", {}).keys())
    return stored

def last_stored_date() -> date | None:
    stored = all_stored_dates()
    if not stored:
        return None
    return date.fromisoformat(max(stored))


# ── Build / update ────────────────────────────────────────────────────────────

def build(update_only: bool = False):
    stored  = all_stored_dates()
    fridays = list(all_fridays(START_DATE, date.today()))

    if update_only:
        last = last_stored_date()
        if last:
            fridays = [d for d in fridays if d > last]
            log.info("Update mode: %d new Fridays after %s", len(fridays), fmt(last))
        else:
            log.info("No existing library — running full build")
    else:
        fridays = [d for d in fridays if fmt(d) not in stored]
        log.info("Build mode: %d Fridays to fetch", len(fridays))

    if not fridays:
        log.info("Nothing to fetch — library is up to date")
        return

    # Group by year so we load/save each year file once
    from itertools import groupby
    fridays_by_year = {}
    for d in fridays:
        fridays_by_year.setdefault(d.year, []).append(d)

    missing = []
    for year, year_dates in sorted(fridays_by_year.items()):
        lib = load_year(year)
        log.info("── Year %d: %d dates to fetch ──", year, len(year_dates))

        for i, d in enumerate(year_dates, 1):
            log.info("  [%d/%d] %s", i, len(year_dates), fmt(d))
            raw = fetch_raw(d)
            if raw is None:
                missing.append(d)
                continue
            records = parse_csv(raw, d)
            if records is None:
                missing.append(d)
                continue
            log.info("    %d stocks", len(records))
            lib["by_date"][fmt(d)] = records
            time.sleep(SLEEP_SEC)

        save_year(year, lib)

    # Print file sizes summary
    log.info("── File sizes ──")
    total_size = 0
    for year in all_years():
        path = lib_path(year)
        if os.path.exists(path):
            mb = os.path.getsize(path) / 1e6
            total_size += mb
            log.info("  %s  %.1f MB", path, mb)
    log.info("  Total: %.1f MB", total_size)

    if missing:
        log.warning("%d missing: %s%s",
                    len(missing),
                    ", ".join(fmt(d) for d in missing[:5]),
                    "..." if len(missing) > 5 else "")


# ── Query helpers ─────────────────────────────────────────────────────────────

def stock_history(code: str) -> list[tuple]:
    """Return [(date_str, {n,s,v,p})] sorted ascending across all year files."""
    code5 = code.strip().zfill(5)
    rows  = []
    for year in all_years():
        path = lib_path(year)
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as f:
            lib = json.load(f)
        for ds, stocks in lib.get("by_date", {}).items():
            if code5 in stocks:
                rows.append((ds, stocks[code5]))
    return sorted(rows)

def query_stock(code: str, weeks: int = None):
    hist = stock_history(code)
    if not hist:
        print(f"Stock {code.zfill(5)} not found.")
        return
    if weeks:
        hist = hist[-weeks:]
    name = hist[-1][1].get("n","") if hist else ""
    print(f"\n{code.zfill(5)} — {name}")
    print(f"{'Date':<12} {'Short Shares':>18} {'Value HKD':>20} {'% Issued':>10}")
    print("-" * 64)
    for ds, data in hist:
        shares = f"{data['s']:,.0f}"  if data.get('s') else "—"
        value  = f"{data['v']:,.0f}"  if data.get('v') else "—"
        pct    = f"{data['p']:.2f}%"  if data.get('p') else "—"
        print(f"{ds:<12} {shares:>18} {value:>20} {pct:>10}")

def query_date(ds: str):
    year = int(ds[:4])
    path = lib_path(year)
    if not os.path.exists(path):
        print(f"No library file for {year}."); return
    with open(path, encoding="utf-8") as f:
        lib = json.load(f)
    if ds not in lib["by_date"]:
        print(f"Date {ds} not in library."); return
    records = lib["by_date"][ds]
    rows = sorted(records.items(), key=lambda x: -(x[1].get("v") or 0))
    print(f"\n{ds} — {len(records)} stocks (top 50 by value)")
    print(f"{'Code':<8} {'Name':<40} {'Short Shares':>16} {'Value HKD':>18} {'%':>8}")
    print("-" * 95)
    for code, data in rows[:50]:
        shares = f"{data['s']:,.0f}"  if data.get('s') else "—"
        value  = f"{data['v']:,.0f}"  if data.get('v') else "—"
        pct    = f"{data['p']:.2f}%"  if data.get('p') else "—"
        print(f"{code:<8} {data.get('n','')[:39]:<40} {shares:>16} {value:>18} {pct:>8}")

def export_stock_csv(code: str):
    hist = stock_history(code)
    if not hist:
        print(f"Stock {code.zfill(5)} not found."); return
    rows = [{"date": ds, "stock_code": code.zfill(5),
             "name": d.get("n",""),
             "short_shares": d.get("s"),
             "short_value_hkd": d.get("v"),
             "pct_issued": d.get("p")} for ds, d in hist]
    path = f"{code.zfill(5)}_short_history.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    print(f"Exported {len(rows)} rows to {path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="SFC Short Position Library")
    p.add_argument("--update", action="store_true", help="Fetch only new dates")
    p.add_argument("--query",  metavar="CODE",       help="Stock history e.g. 00700")
    p.add_argument("--date",   metavar="YYYY-MM-DD", help="All stocks for a date")
    p.add_argument("--weeks",  type=int,             help="Limit query to last N weeks")
    p.add_argument("--export", metavar="CODE",       help="Export stock history to CSV")
    args = p.parse_args()

    if   args.query:  query_stock(args.query, args.weeks)
    elif args.date:   query_date(args.date)
    elif args.export: export_stock_csv(args.export)
    else:             build(update_only=args.update)
