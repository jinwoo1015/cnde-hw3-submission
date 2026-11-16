"""
Assignment 3 — Part 1a: live REST API ingestion (Bronze landing + data-quality checks)
Domain: Kitchen-Aware Restaurant Ranking (Chicago food delivery, Assignments 1-2)

Two live, public, key-free REST APIs enrich the Assignment 2 orders (Aug 1 - Oct 24, 2026):

  1. Open-Meteo Historical Weather API   https://archive-api.open-meteo.com/v1/archive
     Hourly weather for the centroid of each of the 8 delivery zones.
     "Pagination": the API has no page tokens, so the script pages through the date range in
     7-day windows per zone (8 zones x 13 windows = 104 requests, ~16,000 hourly records).

  2. City of Chicago Data Portal (Socrata SODA API)   https://data.cityofchicago.org/resource/<id>.json
     Chicago Traffic Tracker congestion estimates. True offset pagination ($limit / $offset).
     The script first PROBES the candidate datasets and uses the first one that covers the window.
     Both datasets end on 2026-04-30, so traffic is pulled for Aug 1 - Oct 24, 2025 (same calendar window,
     one year earlier) and joined in Silver as a typical-congestion profile (zone x day-of-week x hour).

Robustness
  - Retries with exponential backoff + jitter on timeouts, connection errors, 429 and 5xx.
  - Honors the Retry-After header on 429/503.
  - Non-retryable 4xx, invalid JSON, empty payloads and missing keys are logged and skipped, never crash.
  - Polite pacing between requests (time.sleep).

Data-quality checks (before any transformation), each logged with row-level pass/fail counts:
  weather: required fields not null | no duplicate (zone_id, time) | values in physical ranges | ISO time format
  traffic: required fields not null | no duplicate (area, time) | speed in 0-80 mph | ISO time format

Outputs (Bronze = raw, non-destructive, plus audit metadata ingested_at / source_name):
  data/bronze/weather/*.json         one file per API response (raw payload + request metadata)
  data/bronze/traffic/*.json         one file per API page
  data/bronze/bronze_weather.csv     flattened rows, original values, + ingested_at, source_name
  data/bronze/bronze_traffic.csv     flattened rows, original values, + ingested_at, source_name
  data/bronze/quality_report.json    pass/fail counts per check
  logs/ingest_<timestamp>.log

Run:   python3 ingest_api.py             (both sources)
       python3 ingest_api.py --probe     (only check which traffic dataset covers the dates)
       python3 ingest_api.py --only weather|traffic
Optional: export SOCRATA_APP_TOKEN=<token> for higher Socrata rate limits (never stored in files).
Requirements: pip install requests pandas
"""
import argparse
import datetime as dt
import json
import logging
import os
import random
import sys
import time

import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
BRONZE = os.path.join(HERE, "data", "bronze")
LOGS = os.path.join(HERE, "logs")
START, END = dt.date(2026, 8, 1), dt.date(2026, 10, 24)      # Assignment 2 order date range
# The Chicago traffic datasets stop at 2026-04-30, so traffic is pulled for the same calendar window one
# year earlier and used in Silver as a typical-congestion profile by zone x day-of-week x hour.
T_START, T_END = dt.date(2025, 8, 1), dt.date(2025, 10, 24)
TZ = "America/Chicago"

# Delivery zones from Assignment 2 (zone_id -> name, centroid latitude, longitude)
ZONES = {
    "Z01": ("Hyde Park",    41.7943, -87.5907),
    "Z02": ("Loop",         41.8837, -87.6289),
    "Z03": ("South Loop",   41.8580, -87.6270),
    "Z04": ("West Loop",    41.8827, -87.6533),
    "Z05": ("Lincoln Park", 41.9214, -87.6513),
    "Z06": ("Wicker Park",  41.9088, -87.6796),
    "Z07": ("Logan Square", 41.9234, -87.7083),
    "Z08": ("Pilsen",       41.8525, -87.6631),
}

WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_VARS = ["temperature_2m", "precipitation", "rain", "snowfall", "weather_code",
                "wind_speed_10m", "cloud_cover"]
WEATHER_RANGES = {"temperature_2m": (-40, 50), "precipitation": (0, 100), "rain": (0, 100),
                  "snowfall": (0, 50), "wind_speed_10m": (0, 200), "cloud_cover": (0, 100),
                  "weather_code": (0, 99)}

SODA = "https://data.cityofchicago.org/resource/{}.json"
TRAFFIC_CANDIDATES = [                      # tried in order; first one covering the order dates wins
    ("kf7e-cur8", "region"),                # Historical Congestion Estimates by Region (2018-current)
    ("4g9f-3jbs", "segment"),               # Historical Congestion Estimates by Segment (2024-current)
]
PAGE_SIZE = 1000

session = requests.Session()
session.headers.update({"User-Agent": "hw3-kitchen-ranking-pipeline/1.0 (student project)",
                        "Accept": "application/json"})
if os.environ.get("SOCRATA_APP_TOKEN"):
    session.headers["X-App-Token"] = os.environ["SOCRATA_APP_TOKEN"]

log = logging.getLogger("ingest")


# ----------------------------------------------------------------------------- HTTP with retries
def get_json(url, params, max_retries=6, timeout=30):
    """GET -> parsed JSON, or None after logging. Never raises for network/HTTP problems."""
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            wait = backoff + random.uniform(0, 0.5)
            log.warning("attempt %d: %s -> retry in %.1fs", attempt, type(e).__name__, wait)
            time.sleep(wait); backoff = min(backoff * 2, 60)
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                log.warning("attempt %d: response was not valid JSON (%d bytes)", attempt, len(r.content))
                time.sleep(backoff); backoff = min(backoff * 2, 60)
                continue
        if r.status_code in (429, 500, 502, 503, 504):
            ra = r.headers.get("Retry-After")
            wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else backoff + random.uniform(0, 0.5)
            log.warning("attempt %d: HTTP %d (Retry-After=%s) -> waiting %.1fs", attempt, r.status_code, ra, wait)
            time.sleep(wait); backoff = min(backoff * 2, 60)
            continue
        log.error("HTTP %d (not retryable) for %s : %s", r.status_code, r.url, r.text[:300])
        return None
    log.error("giving up after %d attempts: %s %s", max_retries, url, params)
    return None


def save_raw(folder, name, source, url, params, payload, ingested_at):
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, name), "w", encoding="utf-8") as f:
        json.dump({"source_name": source, "ingested_at": ingested_at, "request_url": url,
                   "params": params, "payload": payload}, f)


# ----------------------------------------------------------------------------- quality checks
def run_checks(df, name, required, key, ranges, time_col):
    """Log and return row-level pass/fail counts for each check. Does not modify df."""
    results, n = [], len(df)
    present = [c for c in required if c in df.columns]
    missing_cols = [c for c in required if c not in df.columns]
    nn_fail = df[present].isna().any(axis=1) if present else pd.Series(False, index=df.index)
    if missing_cols:
        nn_fail = pd.Series(True, index=df.index)
    results.append(("not_null", f"required fields {required} present and not null", int((~nn_fail).sum()), int(nn_fail.sum())))
    dup = df.duplicated(subset=[c for c in key if c in df.columns], keep="first") if set(key) <= set(df.columns) else pd.Series(False, index=df.index)
    results.append(("no_duplicates", f"unique on {key}", int((~dup).sum()), int(dup.sum())))
    rng_fail = pd.Series(False, index=df.index)
    for col, (lo, hi) in ranges.items():
        if col in df.columns:
            v = pd.to_numeric(df[col], errors="coerce")
            rng_fail |= v.notna() & ((v < lo) | (v > hi))
    results.append(("in_range", "values within " + ", ".join(f"{c}[{lo},{hi}]" for c, (lo, hi) in ranges.items()),
                    int((~rng_fail).sum()), int(rng_fail.sum())))
    t = pd.to_datetime(df[time_col], errors="coerce", format="ISO8601") if time_col in df.columns else pd.Series(pd.NaT, index=df.index)
    fmt_fail = t.isna()
    results.append(("time_format", f"{time_col} parses as ISO-8601 timestamp", int((~fmt_fail).sum()), int(fmt_fail.sum())))
    out = []
    for check, rule, passed, failed in results:
        log.info("[QC %s] %-14s pass=%6d fail=%5d  (%s)", name, check, passed, failed, rule)
        out.append({"dataset": name, "check": check, "rule": rule, "rows": n, "pass": passed, "fail": failed})
    flags = pd.DataFrame({"qc_not_null": ~nn_fail, "qc_unique": ~dup, "qc_in_range": ~rng_fail, "qc_time_format": ~fmt_fail})
    return out, flags


# ----------------------------------------------------------------------------- weather (Open-Meteo)
def weekly_windows(start, end, days=7):
    d = start
    while d <= end:
        yield d, min(d + dt.timedelta(days=days - 1), end)
        d += dt.timedelta(days=days)


def ingest_weather(ingested_at):
    rows, folder, calls = [], os.path.join(BRONZE, "weather"), 0
    for zid, (zname, lat, lon) in ZONES.items():
        for w0, w1 in weekly_windows(START, END):
            params = {"latitude": lat, "longitude": lon, "start_date": w0.isoformat(), "end_date": w1.isoformat(),
                      "hourly": ",".join(WEATHER_VARS), "timezone": TZ}
            data = get_json(WEATHER_URL, params); calls += 1
            time.sleep(0.25)                                        # polite pacing (limit: 10k calls/day)
            if not data:
                continue
            save_raw(folder, f"{zid}_{w0}.json", "open-meteo-archive", WEATHER_URL, params, data, ingested_at)
            hourly = data.get("hourly") if isinstance(data, dict) else None
            if not hourly or not hourly.get("time"):
                log.warning("empty payload for %s %s..%s (keys: %s)", zid, w0, w1, list(data)[:8]); continue
            times = hourly["time"]
            missing = [v for v in WEATHER_VARS if v not in hourly]
            if missing:
                log.warning("%s %s: missing keys %s (stored as null)", zid, w0, missing)
            for i, t in enumerate(times):
                rec = {"zone_id": zid, "zone_name": zname, "latitude": data.get("latitude", lat),
                       "longitude": data.get("longitude", lon), "time": t}
                for v in WEATHER_VARS:
                    vals = hourly.get(v) or []
                    rec[v] = vals[i] if i < len(vals) else None
                rec.update({"ingested_at": ingested_at, "source_name": "open-meteo-archive"})
                rows.append(rec)
        log.info("weather %s (%s): %d records so far", zid, zname, len(rows))
    log.info("weather: %d API calls, %d hourly records", calls, len(rows))
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------- traffic (Chicago SODA)
def soda(dataset, params):
    return get_json(SODA.format(dataset), params)


def probe_traffic():
    """Return (dataset_id, kind, sample_row) for the first candidate covering the traffic window."""
    lo, hi = f"{T_START}T00:00:00", f"{T_END}T23:59:59"
    for ds, kind in TRAFFIC_CANDIDATES:
        sample = soda(ds, {"$limit": 1, "$order": "time DESC"})
        if not sample:
            log.info("probe %s (%s): no response", ds, kind); continue
        log.info("probe %s (%s): fields = %s", ds, kind, sorted(sample[0].keys()))
        cov = soda(ds, {"$select": "min(time) AS min_t, max(time) AS max_t, count(*) AS n",
                        "$where": f"time between '{lo}' and '{hi}'"})
        info = cov[0] if cov else {}
        log.info("probe %s (%s): rows in %s..%s = %s (min=%s, max=%s); latest record overall = %s",
                 ds, kind, T_START, T_END, info.get("n"), info.get("min_t"), info.get("max_t"), sample[0].get("time"))
        if info.get("n") and int(info["n"]) > 0:
            return ds, kind, sample[0]
    return None, None, None


def page_through(dataset, params, folder, prefix, ingested_at, max_pages=500):
    """Offset pagination until a short/empty page. Saves each raw page to Bronze."""
    out, offset, page = [], 0, 0
    while page < max_pages:
        p = dict(params, **{"$limit": PAGE_SIZE, "$offset": offset})
        data = soda(dataset, p)
        if data is None:
            log.error("page %d failed permanently; stopping pagination with %d rows", page, len(out)); break
        if not isinstance(data, list):
            log.warning("unexpected payload type %s on page %d", type(data).__name__, page); break
        save_raw(folder, f"{prefix}_p{page:04d}.json", f"chicago-soda-{dataset}", SODA.format(dataset), p, data, ingested_at)
        out.extend(data)
        log.info("traffic page %d: %d rows (total %d)", page, len(data), len(out))
        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE; page += 1
        time.sleep(0.5)                                             # polite pacing for anonymous SODA use
    return out


def zone_regions(dataset):
    """Map each zone centroid to the traffic region whose bounding box contains it."""
    regs = soda(dataset, {"$select": "region_id, region, west, east, south, north",
                          "$group": "region_id, region, west, east, south, north", "$limit": 200})
    mapping = {}
    for zid, (zname, lat, lon) in ZONES.items():
        for r in regs or []:
            try:
                if float(r["west"]) <= lon <= float(r["east"]) and float(r["south"]) <= lat <= float(r["north"]):
                    mapping[zid] = (str(r["region_id"]), r.get("region")); break
            except (KeyError, TypeError, ValueError):
                continue
        log.info("zone %s %-12s -> traffic region %s", zid, zname, mapping.get(zid))
    return mapping


def ingest_traffic(ingested_at):
    ds, kind, sample = probe_traffic()
    if not ds:
        log.error("no candidate traffic dataset covers %s..%s; skipping traffic", T_START, T_END)
        return pd.DataFrame()
    folder, lo, hi = os.path.join(BRONZE, "traffic"), f"{T_START}T00:00:00", f"{T_END}T23:59:59"
    rows = []
    if kind == "region":
        mapping = zone_regions(ds)
        ids = sorted({rid for rid, _ in mapping.values()})
        if not ids:
            log.warning("no zone matched a traffic region bounding box; falling back to the segment dataset")
            ds, kind = "4g9f-3jbs", "segment"
    if kind == "region":
        for quote in ("", "'"):                                     # region_id may be a number or text column
            where = f"time between '{lo}' and '{hi}' AND region_id in ({', '.join(quote + i + quote for i in ids)})"
            test = soda(ds, {"$select": "count(*) AS n", "$where": where})
            if test:
                break
        raw = page_through(ds, {"$where": where, "$order": "time, region_id, record_id"}, folder, ds, ingested_at)
        zones_by_region = {}
        for zid, (rid, _) in mapping.items():
            zones_by_region.setdefault(rid, []).append(zid)
        for r in raw:
            for zid in zones_by_region.get(str(r.get("region_id")), []):
                rows.append(dict(r, zone_id=zid, area_id=str(r.get("region_id"))))
    else:                                                           # segment fallback: hourly aggregate per zone box
        for zid, (zname, lat, lon) in ZONES.items():
            box = (f"start_latitude between {lat - 0.012} and {lat + 0.012} AND "
                   f"start_longitude between {lon - 0.016} and {lon + 0.016}")
            raw = page_through(ds, {"$select": "date_trunc_ymdh(time) AS time, avg(speed) AS speed, count(*) AS samples",
                                    "$where": f"time between '{lo}' and '{hi}' AND speed >= 0 AND {box}",
                                    "$group": "date_trunc_ymdh(time)", "$order": "time"},
                               folder, f"{ds}_{zid}", ingested_at)
            rows += [dict(r, zone_id=zid, area_id=zid) for r in raw]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["ingested_at"], df["source_name"] = ingested_at, f"chicago-soda-{ds}"
    log.info("traffic: dataset %s (%s), %d records", ds, kind, len(df))
    return df


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["weather", "traffic"])
    ap.add_argument("--probe", action="store_true")
    args = ap.parse_args()

    os.makedirs(BRONZE, exist_ok=True); os.makedirs(LOGS, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s",
                        handlers=[logging.FileHandler(os.path.join(LOGS, f"ingest_{stamp}.log")), logging.StreamHandler(sys.stdout)])
    ingested_at = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    if args.probe:
        probe_traffic(); return

    report = []
    if args.only in (None, "weather"):
        w = ingest_weather(ingested_at)
        if not w.empty:
            qc, flags = run_checks(w, "weather", ["zone_id", "time", "temperature_2m", "precipitation", "weather_code"],
                                   ["zone_id", "time"], WEATHER_RANGES, "time")
            pd.concat([w, flags], axis=1).to_csv(os.path.join(BRONZE, "bronze_weather.csv"), index=False)
            report += qc
    if args.only in (None, "traffic"):
        t = ingest_traffic(ingested_at)
        if not t.empty:
            qc, flags = run_checks(t, "traffic", ["zone_id", "time", "speed"], ["zone_id", "area_id", "time"],
                                   {"speed": (0, 80)}, "time")
            pd.concat([t, flags], axis=1).to_csv(os.path.join(BRONZE, "bronze_traffic.csv"), index=False)
            report += qc

    qpath = os.path.join(BRONZE, "quality_report.json")
    if args.only and os.path.exists(qpath):                        # keep the other source's earlier results
        done = {r["dataset"] for r in report}
        with open(qpath) as f:
            report = [r for r in json.load(f).get("checks", []) if r["dataset"] not in done] + report
    with open(qpath, "w") as f:
        json.dump({"ingested_at": ingested_at, "checks": report}, f, indent=2)
    print("\nQUALITY SUMMARY")
    for r in report:
        print(f"  {r['dataset']:<8} {r['check']:<14} rows={r['rows']:>6}  pass={r['pass']:>6}  fail={r['fail']:>5}")
    for name in ("weather", "traffic"):
        n = next((r["rows"] for r in report if r["dataset"] == name), 0)
        print(f"  {name}: {n:,} records {'(>= 500 OK)' if n >= 500 else '(BELOW 500!)'}")


if __name__ == "__main__":
    main()
