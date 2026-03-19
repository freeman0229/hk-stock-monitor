"""
turnover_library.py — HKEX Daily Turnover & Volume Library
===========================================================
Stores daily turnover (HKD) and volume (shares) for the top 100
stocks by turnover from the HKEX daily quotation file.

Library files: turnover_{YYYY}.json — one per year

Structure:
{
  "meta": {"year": 2026, "last_updated": "...", "total_days": N},
  "by_date": {
    "2026-03-19": {
      "00700": {"tv": 26800000000, "vol": 62450000},
      ...
    }
  }
}

API for main.py:
  from turnover_library import save_day, get_tv, get_tv_history, all_stored_dates
"""

import os, json, logging
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── File I/O ──────────────────────────────────────────────────────────────────

def lib_path(year: int) -> str:
    return f"turnover_{year}.json"

def all_years() -> list:
    years = set()
    for f in os.listdir("."):
        if f.startswith("turnover_") and f.endswith(".json"):
            try: years.add(int(f[9:13]))
            except: pass
    years.add(date.today().year)
    return sorted(years)

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
    log.info("Saved turnover_%d.json: %d days, %.2f MB",
             year, len(lib["by_date"]), mb)

def all_stored_dates() -> set:
    stored = set()
    for year in all_years():
        p = lib_path(year)
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                stored.update(json.load(f).get("by_date", {}).keys())
    return stored


# ── Migration from daily_turnover_history.json ────────────────────────────────

def migrate_from_flat(flat_path: str = "daily_turnover_history.json"):
    """
    One-time migration: reads the flat daily_turnover_history.json
    (keys are YYYYMMDD) and writes into year-split turnover_{YYYY}.json.
    Handles both old format {code: number} and new format {code: {tv, vol}}.
    """
    if not os.path.exists(flat_path):
        log.info("No %s to migrate", flat_path)
        return
    with open(flat_path, encoding="utf-8") as f:
        flat = json.load(f)

    by_year: dict = {}
    for yyyymmdd, stocks in flat.items():
        try:
            ds   = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
            year = int(yyyymmdd[:4])
        except Exception:
            continue
        converted = {}
        for code, val in stocks.items():
            if isinstance(val, dict):
                converted[code] = val   # already {"tv": N, "vol": N}
            else:
                converted[code] = {"tv": val, "vol": 0}   # old format had no vol
        by_year.setdefault(year, {})[ds] = converted

    for year, by_date in sorted(by_year.items()):
        lib = load_year(year)
        lib["by_date"].update(by_date)
        save_year(year, lib)
        log.info("Migrated %d days into turnover_%d.json", len(by_date), year)

    log.info("Migration complete — %d total dates across %d years",
             sum(len(v) for v in by_year.values()), len(by_year))


# ── API for main.py ───────────────────────────────────────────────────────────

def save_day(d: datetime, records: dict):
    """
    Save one day's turnover/volume into the library.
    records: {code: {"tv": int, "vol": int}}
    """
    if not records:
        return
    ds   = d.strftime("%Y-%m-%d")
    year = d.year
    lib  = load_year(year)
    lib["by_date"][ds] = records
    save_year(year, lib)


def get_tv(code: str, ds_yyyymmdd: str) -> float:
    """
    Return turnover for a stock on a given date (YYYYMMDD format).
    Returns 0.0 if not found.
    """
    year = int(ds_yyyymmdd[:4])
    ds   = f"{ds_yyyymmdd[:4]}-{ds_yyyymmdd[4:6]}-{ds_yyyymmdd[6:8]}"
    p    = lib_path(year)
    if not os.path.exists(p):
        return 0.0
    with open(p, encoding="utf-8") as f:
        rec = json.load(f).get("by_date", {}).get(ds, {}).get(code, {})
    if isinstance(rec, dict):
        return float(rec.get("tv", 0))
    return float(rec)


def get_tv_history(code: str, n: int, before: str) -> list:
    """
    Return last n turnover values (HKD) for a stock before date `before`
    (YYYY-MM-DD), newest-first. Skips days with no data.
    """
    code5  = code.zfill(5)
    result = []
    for year in sorted(all_years(), reverse=True):
        p = lib_path(year)
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8") as f:
            by_date = json.load(f).get("by_date", {})
        for ds in sorted(by_date.keys(), reverse=True):
            if ds >= before:
                continue
            rec = by_date[ds].get(code5, {})
            tv  = rec.get("tv", 0) if isinstance(rec, dict) else rec
            if tv > 0:
                result.append(float(tv))
            if len(result) >= n:
                return result
    return result


def get_day_records(ds: str) -> dict:
    """
    Return full {code: {tv, vol}} dict for a date (YYYY-MM-DD).
    Used to load the full store for avg computations.
    """
    year = int(ds[:4])
    p    = lib_path(year)
    if not os.path.exists(p):
        return {}
    with open(p, encoding="utf-8") as f:
        return json.load(f).get("by_date", {}).get(ds, {})


def load_recent(n_days: int, before: str) -> dict:
    """
    Return a dict of {YYYYMMDD: {code: {tv, vol}}} for the last n_days
    before `before` (YYYY-MM-DD). Used as a drop-in for the old
    daily_turnover_history store in _turnover_avg and _value_at.
    """
    result = {}
    count  = 0
    for year in sorted(all_years(), reverse=True):
        p = lib_path(year)
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8") as f:
            by_date = json.load(f).get("by_date", {})
        for ds in sorted(by_date.keys(), reverse=True):
            if ds >= before:
                continue
            yyyymmdd = ds.replace("-", "")
            result[yyyymmdd] = by_date[ds]
            count += 1
            if count >= n_days:
                return result
    return result


# ── CLI: migrate ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Turnover Library")
    ap.add_argument("--migrate", action="store_true",
                    help="Migrate daily_turnover_history.json to year-split files")
    args = ap.parse_args()
    if args.migrate:
        migrate_from_flat()
    else:
        print("Usage: python turnover_library.py --migrate")
