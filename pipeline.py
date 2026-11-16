"""
Assignment 3 — Part 1b: Medallion pipeline (Bronze -> Silver -> Gold) with PII masking
Domain: Kitchen-Aware Restaurant Ranking (Chicago food delivery)

Inputs (all produced earlier, nothing is re-downloaded here)
  data/bronze/weather/*.json, data/bronze/traffic/*.json   raw API responses   (ingest_api.py)
  data/bronze/bronze_weather.csv, bronze_traffic.csv        flattened API rows  (ingest_api.py)
  data/source/food_delivery_flat.csv                        Assignment 2 flat dataset (5,486 rows)

Layers
  BRONZE  raw, non-destructive: raw JSON payloads + flat staging rows, all values kept as text,
          plus audit columns ingested_at and source_name.
  SILVER  typed, cleaned, conformed; rows that fail a rule go to silver_quarantine with a reason.
          PII is masked here, BEFORE anything is written to Silver:
            customer_email   -> SHA-256(salt + lower(email))  (64-hex token; email domain kept)
            customer_name    -> initials only ("Olivia Nguyen" -> "O. N.")
            customer_phone   -> last 4 digits only ("(***) ***-4952")
            customer_address -> street number and name dropped; SHA-256 token of the full address kept
            driver_name      -> initials only
          Weather (exact zone + hour) and traffic (zone x day-of-week x hour profile) are joined to orders.
          SIMULATED EFFECT: because the Assignment 2 orders are synthetic, a documented, rule-based delay
          is added to delivered orders (rain severity and congestion), and is_late is recomputed.
  GOLD    3 curated tables: daily delivery performance by zone x weather, hourly congestion vs lateness,
          and kitchen load x weather severity.

Modes
  python3 pipeline.py --local    build every layer locally into data/silver and data/gold (no database)
  python3 pipeline.py            build, then load all layers into RDS MySQL (schema kitchen_dw)
                                 needs RDS_HOST, RDS_USER env vars; password is prompted, never stored
  PII_SALT env var sets the hashing salt (keep it secret); if unset, a random salt is generated per run.
"""
import argparse
import datetime as dt
import getpass
import glob
import hashlib
import json
import logging
import os
import re
import secrets
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
D = lambda *p: os.path.join(HERE, "data", *p)
DB = "kitchen_dw"
log = logging.getLogger("pipeline")

ZONES = {"Z01": "Hyde Park", "Z02": "Loop", "Z03": "South Loop", "Z04": "West Loop",
         "Z05": "Lincoln Park", "Z06": "Wicker Park", "Z07": "Logan Square", "Z08": "Pilsen"}

# ---- documented simulation rules (Silver) -------------------------------------------------
WEATHER_DELAY_MIN = {0: 0, 1: 3, 2: 7, 3: 12}         # by weather severity
WIND_DELAY_MIN = 2                                     # extra minutes when wind >= 40 km/h
CONGESTION = [(19.0, "heavy", 5), (22.0, "moderate", 2), (float("inf"), "free", 0)]   # typical mph -> level, minutes
LATE_BUFFER_MIN = 10                                   # Assignment 1 definition


def weather_condition(code, precip):
    """WMO weather code -> (condition, severity 0-3)."""
    c = int(code) if pd.notna(code) else -1
    if c in (0, 1):                return "Clear", 0
    if c in (2, 3):                return "Cloudy", 0
    if c in (45, 48):              return "Fog", 1
    if 51 <= c <= 57:              return "Drizzle", 1
    if c == 61 or c == 80:         return "Light rain", 1
    if c in (63, 66, 81):          return "Rain", 2
    if c in (65, 67, 82):          return "Heavy rain", 3
    if 71 <= c <= 77 or c in (85, 86): return "Snow", 3
    if c >= 95:                    return "Thunderstorm", 3
    return ("Rain", 2) if (precip or 0) > 0 else ("Clear", 0)


def congestion_level(speed):
    for limit, level, minutes in CONGESTION:
        if speed < limit:
            return level, minutes
    return "free", 0


# ---- PII masking ---------------------------------------------------------------------------
SALT = os.environ.get("PII_SALT") or secrets.token_hex(16)

def sha256(value):
    return None if pd.isna(value) or value == "" else hashlib.sha256((SALT + str(value).strip().lower()).encode()).hexdigest()

def initials(name):
    if pd.isna(name):
        return None
    parts = [p for p in re.split(r"\s+", str(name).strip()) if p]
    return " ".join(p[0].upper() + "." for p in parts) or None

def phone_last4(phone):
    if pd.isna(phone):
        return None
    digits = re.sub(r"\D", "", str(phone))
    return f"(***) ***-{digits[-4:]}" if len(digits) >= 4 else None

def address_masked(addr):
    """'1560 E 57th St, Chicago, IL' -> 'Chicago, IL' (street line removed)."""
    if pd.isna(addr):
        return None
    parts = [p.strip() for p in str(addr).split(",")]
    return ", ".join(parts[1:]) if len(parts) > 1 else None


# =============================================================================== BRONZE
def build_bronze(ingested_at):
    raw = []
    for path in sorted(glob.glob(D("bronze", "weather", "*.json")) + glob.glob(D("bronze", "traffic", "*.json"))):
        with open(path, encoding="utf-8") as f:
            rec = json.load(f)
        p = rec.get("payload")
        n = len(p) if isinstance(p, list) else len((p or {}).get("hourly", {}).get("time", []))
        raw.append({"source_name": rec["source_name"], "file_name": os.path.basename(path), "request_url": rec["request_url"],
                    "request_params": json.dumps(rec["params"]), "payload": json.dumps(p), "record_count": n,
                    "ingested_at": rec["ingested_at"]})
    bronze_api_raw = pd.DataFrame(raw)

    bw = pd.read_csv(D("bronze", "bronze_weather.csv"), dtype=str, keep_default_na=False)
    bt = pd.read_csv(D("bronze", "bronze_traffic.csv"), dtype=str, keep_default_na=False)
    bt = bt.drop(columns=[c for c in ("nw_location", "se_location") if c in bt.columns])   # GeoJSON duplicates of the bbox columns
    bo = pd.read_csv(D("source", "food_delivery_flat.csv"), dtype=str, keep_default_na=False)
    bo.insert(0, "csv_line", range(2, len(bo) + 2))
    bo["ingested_at"], bo["source_name"] = ingested_at, "assignment2-food_delivery_flat.csv"
    log.info("BRONZE  api_raw=%d payloads  weather=%d  traffic=%d  orders_flat=%d",
             len(bronze_api_raw), len(bw), len(bt), len(bo))
    return {"bronze_api_raw": bronze_api_raw, "bronze_weather_hourly": bw, "bronze_traffic_10min": bt, "bronze_orders_flat": bo}


# =============================================================================== SILVER
def _json_safe(v):
    if v is None or v is pd.NaT or v is pd.NA or (isinstance(v, float) and v != v):
        return None
    return str(v)


def quarantine(df, mask, source, reason, key_cols):
    q = df[mask]
    keys = ["|".join(_json_safe(v) or "" for v in row) for row in q[key_cols].itertuples(index=False, name=None)]
    recs = [json.dumps({k: _json_safe(v) for k, v in zip(q.columns, row)}, allow_nan=False)
            for row in q.itertuples(index=False, name=None)]
    return pd.DataFrame({"source_table": source, "reason": reason, "record_key": keys, "record_json": recs})


def build_silver(b):
    quar = []

    # ---- weather: type-cast, quarantine failures, derive condition + severity
    w = b["bronze_weather_hourly"].copy()
    num = ["temperature_2m", "precipitation", "rain", "snowfall", "weather_code", "wind_speed_10m", "cloud_cover"]
    for c in num:
        w[c] = pd.to_numeric(w[c].replace("", np.nan), errors="coerce")
    w["obs_hour"] = pd.to_datetime(w["time"], errors="coerce")
    bad = (w[[f"qc_{k}" for k in ("not_null", "unique", "in_range", "time_format")]] != "True").any(axis=1) | w.obs_hour.isna()
    quar.append(quarantine(w, bad, "bronze_weather_hourly", "failed ingestion quality check", ["zone_id", "time"]))
    w = w[~bad]
    cond = [weather_condition(c, p) for c, p in zip(w.weather_code, w.precipitation)]
    silver_weather = pd.DataFrame({
        "zone_id": w.zone_id, "obs_hour": w.obs_hour, "temperature_c": w.temperature_2m, "precipitation_mm": w.precipitation,
        "snowfall_cm": w.snowfall, "wind_speed_kmh": w.wind_speed_10m, "cloud_cover_pct": w.cloud_cover,
        "weather_code": w.weather_code.astype("Int64"), "weather_condition": [c for c, _ in cond],
        "weather_severity": [s for _, s in cond], "grid_latitude": pd.to_numeric(w.latitude), "grid_longitude": pd.to_numeric(w.longitude)})

    # ---- traffic: quarantine speed <= 0 (no estimate) and failed checks, then zone x day-of-week x hour profile
    t = b["bronze_traffic_10min"].copy()
    t["speed_mph"] = pd.to_numeric(t.speed, errors="coerce")
    t["reads"] = pd.to_numeric(t.num_reads, errors="coerce")
    t["ts"] = pd.to_datetime(t.time, errors="coerce")
    failed_qc = (t[[f"qc_{k}" for k in ("not_null", "unique", "in_range", "time_format")]] != "True").any(axis=1)
    no_est = ~failed_qc & ((t.speed_mph <= 0) | (t.reads <= 0))
    quar.append(quarantine(t, failed_qc, "bronze_traffic_10min", "failed ingestion quality check", ["zone_id", "record_id"]))
    quar.append(quarantine(t, no_est, "bronze_traffic_10min", "speed = 0 or no GPS reads (no congestion estimate)", ["zone_id", "record_id"]))
    t = t[~failed_qc & ~no_est]
    t["day_of_week"] = (t.ts.dt.dayofweek + 1) % 7 + 1          # MySQL DAYOFWEEK convention: 1 = Sunday
    t["hour_of_day"] = t.ts.dt.hour
    prof = (t.groupby(["zone_id", "area_id", "region", "day_of_week", "hour_of_day"])
              .agg(typical_speed_mph=("speed_mph", "mean"), samples=("speed_mph", "size"), avg_reads=("reads", "mean"))
              .reset_index().rename(columns={"area_id": "traffic_region_id", "region": "traffic_region"}))
    lv = [congestion_level(s) for s in prof.typical_speed_mph]
    prof["congestion_level"], prof["congestion_delay_min"] = [l for l, _ in lv], [m for _, m in lv]
    prof["typical_speed_mph"] = prof.typical_speed_mph.round(2)
    prof["avg_reads"] = prof.avg_reads.round(1)
    prof["profile_window"] = "2025-08-01..2025-10-24"

    # ---- orders (Assignment 2 flat file): clean like HW2, mask PII, enrich, simulate
    o = b["bronze_orders_flat"].drop(columns=["ingested_at", "source_name"]).copy()
    data_cols = [c for c in o.columns if c != "csv_line"]
    dup = o.duplicated(subset=data_cols, keep="first")
    masked = o.assign(customer_name=o.customer_name.map(initials), customer_email=o.customer_email.map(sha256),
                      customer_phone=o.customer_phone.map(phone_last4), customer_address=o.customer_address.map(sha256),
                      driver_name=o.driver_name.map(initials))          # quarantine lives in Silver, so mask PII here too
    quar.append(quarantine(masked, dup, "bronze_orders_flat", "exact duplicate row", ["order_id", "order_line_no"]))
    o = o[~dup].copy()
    for c in data_cols:
        o[c] = o[c].str.strip().replace("", np.nan)

    def canonical(frame, key, col):                      # most common properly-cased spelling per entity
        def pick(s):
            s = s.dropna()
            if s.empty:
                return None
            proper = s[~(s.str.isupper() | s.str.islower())]
            return proper.mode().iloc[0] if len(proper) else s.iloc[0].title()
        frame[col] = frame[key].map(frame.groupby(key)[col].agg(pick))
    for key, col in [("restaurant_id", "restaurant_name"), ("restaurant_zone_id", "restaurant_zone"), ("customer_id", "customer_name")]:
        canonical(o, key, col)
    zone_lookup = {v.lower(): v for v in ZONES.values()}
    o["customer_zone"] = o.customer_zone.str.lower().map(zone_lookup)
    o = o.sort_values(["order_timestamp", "order_id", "order_line_no"])
    latest = o.groupby("customer_id").tail(1).set_index("customer_id")
    for c in ["customer_email", "customer_address", "customer_phone", "customer_zone"]:
        o[c] = o.customer_id.map(latest[c])                # business rule: most recent order wins
    o["restaurant_phone"] = o.restaurant_id.map(o.groupby("restaurant_id").restaurant_phone.agg(lambda s: s.dropna().mode().iloc[0] if s.notna().any() else None))
    o["payment_method"] = o.order_id.map(o.groupby("order_id").payment_method.agg(lambda s: s.dropna().mode().iloc[0] if s.notna().any() else None))
    zone_id_by_name = {v: k for k, v in ZONES.items()}

    # conformed entities (PII masked before Silver)
    cust = o.drop_duplicates("customer_id")
    silver_customers = pd.DataFrame({
        "customer_id": cust.customer_id, "customer_name_masked": cust.customer_name.map(initials),
        "email_sha256": cust.customer_email.map(sha256), "email_domain": cust.customer_email.str.split("@").str[-1].str.lower(),
        "phone_masked": cust.customer_phone.map(phone_last4), "address_city_state": cust.customer_address.map(address_masked),
        "address_sha256": cust.customer_address.map(sha256), "zone_id": cust.customer_zone.map(zone_id_by_name),
        "signup_date": pd.to_datetime(cust.customer_signup_date).dt.date}).sort_values("customer_id")
    rest = o.drop_duplicates("restaurant_id")
    silver_restaurants = pd.DataFrame({
        "restaurant_id": rest.restaurant_id, "restaurant_name": rest.restaurant_name, "zone_id": rest.restaurant_zone_id,
        "cuisine": rest.cuisine, "cuisine_path": rest.cuisine_path,
        "kitchen_capacity": pd.to_numeric(rest.kitchen_capacity).astype(int)}).sort_values("restaurant_id")
    drv = o.dropna(subset=["driver_id"]).drop_duplicates("driver_id")
    silver_drivers = pd.DataFrame({"driver_id": drv.driver_id, "driver_name_masked": drv.driver_name.map(initials),
                                   "vehicle_type": drv.driver_vehicle, "driver_rating": pd.to_numeric(drv.driver_rating)}).sort_values("driver_id")
    silver_order_items = pd.DataFrame({
        "order_id": o.order_id, "order_line_no": pd.to_numeric(o.order_line_no).astype(int), "item_id": o.item_id,
        "item_name": o.item_name, "quantity": pd.to_numeric(o.quantity).astype(int), "unit_price": pd.to_numeric(o.unit_price)})

    # order-level grain + enrichment
    o["line_total"] = pd.to_numeric(o.unit_price) * pd.to_numeric(o.quantity)
    g = o.groupby("order_id")
    orders = g.first()[["customer_id", "restaurant_id", "restaurant_zone_id", "driver_id", "order_timestamp", "order_status",
                        "payment_method", "delivery_fee", "tip_amount", "kitchen_queue_at_order", "kitchen_capacity",
                        "quoted_eta_min", "actual_delivery_min", "order_rating"]].reset_index()
    orders["item_count"] = g.quantity.apply(lambda s: pd.to_numeric(s).sum()).values
    orders["food_revenue"] = g.line_total.sum().round(2).values
    orders["order_ts"] = pd.to_datetime(orders.order_timestamp)
    orders["order_hour"] = orders.order_ts.dt.floor("h")
    orders["day_of_week"] = (orders.order_ts.dt.dayofweek + 1) % 7 + 1
    orders["hour_of_day"] = orders.order_ts.dt.hour
    for c in ["delivery_fee", "tip_amount", "actual_delivery_min", "order_rating"]:
        orders[c] = pd.to_numeric(orders[c])
    for c in ["kitchen_queue_at_order", "kitchen_capacity", "quoted_eta_min"]:
        orders[c] = pd.to_numeric(orders[c]).astype(int)

    wx = silver_weather[["zone_id", "obs_hour", "temperature_c", "precipitation_mm", "wind_speed_kmh", "weather_condition", "weather_severity"]]
    orders = orders.merge(wx, left_on=["restaurant_zone_id", "order_hour"], right_on=["zone_id", "obs_hour"], how="left").drop(columns=["zone_id", "obs_hour"])
    orders = orders.merge(prof[["zone_id", "day_of_week", "hour_of_day", "typical_speed_mph", "congestion_level", "congestion_delay_min"]],
                          left_on=["restaurant_zone_id", "day_of_week", "hour_of_day"], right_on=["zone_id", "day_of_week", "hour_of_day"],
                          how="left").drop(columns=["zone_id"])
    no_enrich = orders.weather_condition.isna() | orders.typical_speed_mph.isna()
    quar.append(quarantine(orders, no_enrich, "silver_orders", "no weather or traffic match for zone/hour", ["order_id"]))
    orders = orders[~no_enrich].copy()

    # SIMULATED weather + congestion effect on delivered orders (documented rules above)
    delivered = orders.order_status == "delivered"
    wdelay = orders.weather_severity.map(WEATHER_DELAY_MIN).fillna(0) + np.where(orders.wind_speed_kmh >= 40, WIND_DELAY_MIN, 0)
    orders["sim_weather_delay_min"] = np.where(delivered, wdelay, 0).astype(int)
    orders["sim_traffic_delay_min"] = np.where(delivered, orders.congestion_delay_min, 0).astype(int)
    orders["original_delivery_min"] = orders.actual_delivery_min
    orders["delivery_min"] = np.where(delivered, orders.actual_delivery_min + orders.sim_weather_delay_min + orders.sim_traffic_delay_min, np.nan)
    orders["delivered_ts"] = (orders.order_ts + pd.to_timedelta(orders.delivery_min.fillna(0), unit="m")).where(delivered)
    orders["is_late"] = (orders.delivery_min > orders.quoted_eta_min + LATE_BUFFER_MIN).astype("Int64").where(delivered)
    orders["is_cancelled"] = (orders.order_status == "cancelled").astype(int)
    silver_orders = orders[["order_id", "customer_id", "restaurant_id", "restaurant_zone_id", "driver_id", "order_ts", "order_status",
                            "payment_method", "item_count", "food_revenue", "delivery_fee", "tip_amount", "kitchen_queue_at_order",
                            "kitchen_capacity", "quoted_eta_min", "original_delivery_min", "sim_weather_delay_min",
                            "sim_traffic_delay_min", "delivery_min", "delivered_ts", "is_late", "is_cancelled", "order_rating",
                            "temperature_c", "precipitation_mm", "wind_speed_kmh", "weather_condition", "weather_severity",
                            "typical_speed_mph", "congestion_level"]].rename(columns={"restaurant_zone_id": "zone_id"}).sort_values("order_id")
    silver_order_items = silver_order_items[silver_order_items.order_id.isin(silver_orders.order_id)]

    q = pd.concat([x for x in quar if len(x)], ignore_index=True) if any(len(x) for x in quar) else pd.DataFrame(columns=["source_table", "reason", "record_key", "record_json"])
    q.insert(0, "quarantine_id", range(1, len(q) + 1))
    for (src, reason), n in q.groupby(["source_table", "reason"]).size().items():
        log.info("QUARANTINE  %-22s %-50s %6d rows", src, reason, n)
    log.info("SILVER  weather=%d  traffic_profile=%d  customers=%d  restaurants=%d  drivers=%d  orders=%d  order_items=%d",
             len(silver_weather), len(prof), len(silver_customers), len(silver_restaurants), len(silver_drivers), len(silver_orders), len(silver_order_items))
    return {"silver_weather_hourly": silver_weather, "silver_traffic_profile": prof, "silver_customers": silver_customers,
            "silver_restaurants": silver_restaurants, "silver_drivers": silver_drivers, "silver_orders": silver_orders,
            "silver_order_items": silver_order_items, "silver_quarantine": q}


# =============================================================================== GOLD
def build_gold(s):
    o = s["silver_orders"].copy()
    o["order_date"] = o.order_ts.dt.date
    o["zone_name"] = o.zone_id.map(ZONES)
    o["day_of_week"] = (o.order_ts.dt.dayofweek + 1) % 7 + 1
    o["hour_of_day"] = o.order_ts.dt.hour

    def perf(frame, keys):
        g = frame.groupby(keys)
        out = g.agg(orders=("order_id", "size"), cancelled=("is_cancelled", "sum"), food_revenue=("food_revenue", "sum")).reset_index()
        dl = frame[frame.order_status == "delivered"].groupby(keys).agg(
            delivered=("order_id", "size"), late=("is_late", "sum"), avg_delivery_min=("delivery_min", "mean")).reset_index()
        out = out.merge(dl, on=keys, how="left").fillna({"delivered": 0, "late": 0})
        out["late_rate_pct"] = (100 * out.late / out.delivered.replace(0, np.nan)).round(1)
        out["cancel_rate_pct"] = (100 * out.cancelled / out.orders).round(1)
        out["avg_delivery_min"] = out.avg_delivery_min.round(1)
        out["food_revenue"] = out.food_revenue.round(2)
        for c in ("delivered", "late", "cancelled"):
            out[c] = out[c].astype(int)
        return out

    g1 = perf(o, ["order_date", "zone_id", "zone_name", "weather_condition", "weather_severity"])
    wx = o.groupby(["order_date", "zone_id", "weather_condition"]).agg(avg_precip_mm=("precipitation_mm", "mean"), avg_temp_c=("temperature_c", "mean")).round(2).reset_index()
    gold_daily = g1.merge(wx, on=["order_date", "zone_id", "weather_condition"])

    g2 = perf(o, ["zone_id", "zone_name", "day_of_week", "hour_of_day"])
    prof = s["silver_traffic_profile"][["zone_id", "day_of_week", "hour_of_day", "typical_speed_mph", "congestion_level"]]
    gold_hourly = prof.merge(g2, on=["zone_id", "day_of_week", "hour_of_day"], how="inner")

    o["load_band"] = pd.cut(o.kitchen_queue_at_order / o.kitchen_capacity, [-1, 0.5, 0.8, 1.0, 99],
                            labels=["1. under 50%", "2. 50-80%", "3. 80-100%", "4. at/over capacity"]).astype(str)
    g3 = perf(o, ["load_band", "weather_severity"])
    gold_load_weather = g3
    log.info("GOLD    daily_zone_weather=%d  hourly_congestion=%d  load_x_weather=%d", len(gold_daily), len(gold_hourly), len(gold_load_weather))
    return {"gold_daily_zone_weather": gold_daily, "gold_hourly_congestion": gold_hourly, "gold_kitchen_load_weather": gold_load_weather}


# =============================================================================== LOAD to MySQL
def ddl_statements(section):
    sql = open(os.path.join(HERE, "warehouse.sql"), encoding="utf-8").read()
    body = sql.split(f"-- @@{section}_START")[1].split(f"-- @@{section}_END")[0]
    body = re.sub(r"--[^\n]*", "", body)
    return [x.strip() for x in body.split(";") if x.strip()]


def _db_value(v):
    """Python value MySQL accepts: NaN / NA / NaT -> NULL, numpy -> Python, Timestamp -> datetime."""
    if v is None or v is pd.NaT or v is pd.NA:
        return None
    if isinstance(v, pd.Timestamp):
        return None if pd.isna(v) else v.to_pydatetime()
    if isinstance(v, np.generic):
        v = v.item()
    if isinstance(v, float) and v != v:              # NaN
        return None
    return v


def to_db_values(df):
    return [tuple(_db_value(v) for v in row) for row in df.itertuples(index=False, name=None)]


def load_mysql(layers):
    import pymysql
    conn = pymysql.connect(host=os.environ.get("RDS_HOST") or input("RDS endpoint: ").strip(),
                           user=os.environ.get("RDS_USER") or input("Master username: ").strip(),
                           password=os.environ.get("RDS_PASSWORD") or getpass.getpass("Password: "),
                           port=3306, charset="utf8mb4", autocommit=False, max_allowed_packet=256 * 1024 * 1024)
    with conn.cursor() as cur:
        for stmt in ddl_statements("MEDALLION"):
            cur.execute(stmt)
        for name, df in layers.items():
            cols = ", ".join(f"`{c}`" for c in df.columns)
            sql = f"INSERT INTO `{name}` ({cols}) VALUES ({', '.join(['%s'] * len(df.columns))})"
            rows = to_db_values(df)
            batch = 50 if name == "bronze_api_raw" else 5000
            for i in range(0, len(rows), batch):
                cur.executemany(sql, rows[i:i + batch])
            conn.commit()
            log.info("loaded %-28s %7d rows", name, len(rows))
        log.info("row counts in %s:", DB)
        for name in layers:
            cur.execute(f"SELECT COUNT(*) FROM `{name}`")
            log.info("  %-28s %7d", name, cur.fetchone()[0])
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true", help="build layers locally only (no database)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s",
                        handlers=[logging.FileHandler(os.path.join(HERE, "logs", f"pipeline_{dt.datetime.now():%Y%m%d_%H%M%S}.log")),
                                  logging.StreamHandler(sys.stdout)])
    if not os.environ.get("PII_SALT"):
        log.warning("PII_SALT not set: using a random salt for this run (tokens will differ between runs)")
    ingested_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    bronze = build_bronze(ingested_at)
    silver = build_silver(bronze)
    gold = build_gold(silver)
    for layer, frames in (("silver", silver), ("gold", gold)):
        os.makedirs(D(layer), exist_ok=True)
        for name, df in frames.items():
            df.to_csv(D(layer, f"{name}.csv"), index=False)
    log.info("wrote data/silver/*.csv and data/gold/*.csv")
    if not args.local:
        load_mysql({**bronze, **silver, **gold})


if __name__ == "__main__":
    main()
