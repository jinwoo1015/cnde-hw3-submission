-- =====================================================================
-- Assignment 3 — Warehouse SQL (Amazon RDS for MySQL 8.4, schema kitchen_dw)
-- Kitchen-Aware Restaurant Ranking · Jinwoo Choi
--
--   PART 1  Medallion staging tables: Bronze (raw) / Silver (clean, masked) / Gold (curated)
--           Created by pipeline.py from the @@MEDALLION block below, then loaded by pipeline.py.
--   PART 2  Star schema DDL, SCD Type 2, ELT from Silver, analytical queries, optimization
--   PART 4  Showcase queries
-- No credentials are stored in this file.
-- =====================================================================

-- @@MEDALLION_START
CREATE DATABASE IF NOT EXISTS kitchen_dw CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE kitchen_dw;

DROP TABLE IF EXISTS gold_kitchen_load_weather, gold_hourly_congestion, gold_daily_zone_weather;
DROP TABLE IF EXISTS silver_quarantine, silver_order_items, silver_orders, silver_drivers, silver_restaurants,
                     silver_customers, silver_traffic_profile, silver_weather_hourly;
DROP TABLE IF EXISTS bronze_orders_flat, bronze_traffic_10min, bronze_weather_hourly, bronze_api_raw;

-- ---------------------------------------------------------------------
-- BRONZE: raw and non-destructive. API values are kept exactly as text;
-- every row carries ingested_at and source_name.
-- ---------------------------------------------------------------------
CREATE TABLE bronze_api_raw (                      -- one row per raw API response / page
    raw_id          INT AUTO_INCREMENT PRIMARY KEY,
    source_name     VARCHAR(40)  NOT NULL,
    file_name       VARCHAR(80)  NOT NULL,
    request_url     VARCHAR(255) NOT NULL,
    request_params  JSON         NOT NULL,
    payload         JSON         NOT NULL,
    record_count    INT          NOT NULL,
    ingested_at     VARCHAR(32)  NOT NULL
) ENGINE = InnoDB;

CREATE TABLE bronze_weather_hourly (               -- Open-Meteo, flattened, values as received
    zone_id VARCHAR(8), zone_name VARCHAR(40), latitude VARCHAR(20), longitude VARCHAR(20), `time` VARCHAR(25),
    temperature_2m VARCHAR(20), precipitation VARCHAR(20), rain VARCHAR(20), snowfall VARCHAR(20),
    weather_code VARCHAR(20), wind_speed_10m VARCHAR(20), cloud_cover VARCHAR(20),
    ingested_at VARCHAR(32) NOT NULL, source_name VARCHAR(40) NOT NULL,
    qc_not_null VARCHAR(5), qc_unique VARCHAR(5), qc_in_range VARCHAR(5), qc_time_format VARCHAR(5)
) ENGINE = InnoDB;

CREATE TABLE bronze_traffic_10min (                -- Chicago Traffic Tracker regions, 10-minute records
    `time` VARCHAR(30), region_id VARCHAR(10), speed VARCHAR(20), region VARCHAR(60), bus_count VARCHAR(10),
    num_reads VARCHAR(10), `hour` VARCHAR(5), day_of_week VARCHAR(5), `month` VARCHAR(5), description VARCHAR(255),
    record_id VARCHAR(30), west VARCHAR(20), east VARCHAR(20), south VARCHAR(20), north VARCHAR(20),
    zone_id VARCHAR(8), area_id VARCHAR(10), ingested_at VARCHAR(32) NOT NULL, source_name VARCHAR(40) NOT NULL,
    qc_not_null VARCHAR(5), qc_unique VARCHAR(5), qc_in_range VARCHAR(5), qc_time_format VARCHAR(5)
) ENGINE = InnoDB;

CREATE TABLE bronze_orders_flat (                  -- Assignment 2 flat file, all 40 columns as text
    csv_line INT NOT NULL PRIMARY KEY,
    order_id VARCHAR(20), order_line_no VARCHAR(10), order_timestamp VARCHAR(30), order_status VARCHAR(20),
    payment_method VARCHAR(20), delivery_fee VARCHAR(20), tip_amount VARCHAR(20), special_instructions VARCHAR(255),
    customer_id VARCHAR(20), customer_name VARCHAR(100), customer_email VARCHAR(150), customer_phone VARCHAR(30),
    customer_address VARCHAR(200), customer_zone VARCHAR(50), customer_signup_date VARCHAR(20),
    restaurant_id VARCHAR(20), restaurant_name VARCHAR(100), restaurant_address VARCHAR(200), restaurant_phone VARCHAR(30),
    restaurant_zone_id VARCHAR(10), restaurant_zone VARCHAR(50), kitchen_capacity VARCHAR(10), cuisine VARCHAR(50),
    cuisine_path VARCHAR(100), item_id VARCHAR(20), item_name VARCHAR(100), item_dietary_tags VARCHAR(100),
    unit_price VARCHAR(20), quantity VARCHAR(10), line_total VARCHAR(20), kitchen_queue_at_order VARCHAR(10),
    quoted_eta_min VARCHAR(10), driver_id VARCHAR(20), driver_name VARCHAR(100), driver_vehicle VARCHAR(20),
    driver_rating VARCHAR(10), actual_delivery_min VARCHAR(10), delivered_timestamp VARCHAR(30), is_late VARCHAR(5),
    order_rating VARCHAR(5),
    ingested_at VARCHAR(32) NOT NULL, source_name VARCHAR(60) NOT NULL
) ENGINE = InnoDB;

-- ---------------------------------------------------------------------
-- SILVER: typed, cleaned, conformed, PII masked; rejects go to silver_quarantine
-- ---------------------------------------------------------------------
CREATE TABLE silver_weather_hourly (
    zone_id CHAR(3) NOT NULL, obs_hour DATETIME NOT NULL,
    temperature_c DECIMAL(5,1), precipitation_mm DECIMAL(5,2), snowfall_cm DECIMAL(5,2), wind_speed_kmh DECIMAL(5,1),
    cloud_cover_pct TINYINT UNSIGNED, weather_code TINYINT UNSIGNED, weather_condition VARCHAR(20) NOT NULL,
    weather_severity TINYINT UNSIGNED NOT NULL, grid_latitude DECIMAL(9,6), grid_longitude DECIMAL(9,6),
    PRIMARY KEY (zone_id, obs_hour)
) ENGINE = InnoDB;

CREATE TABLE silver_traffic_profile (              -- typical congestion: zone x day-of-week x hour (Aug-Oct 2025)
    zone_id CHAR(3) NOT NULL, traffic_region_id SMALLINT NOT NULL, traffic_region VARCHAR(60) NOT NULL,
    day_of_week TINYINT NOT NULL, hour_of_day TINYINT NOT NULL,
    typical_speed_mph DECIMAL(5,2) NOT NULL, samples INT NOT NULL, avg_reads DECIMAL(8,1),
    congestion_level VARCHAR(10) NOT NULL, congestion_delay_min TINYINT NOT NULL, profile_window VARCHAR(30) NOT NULL,
    PRIMARY KEY (zone_id, day_of_week, hour_of_day)
) ENGINE = InnoDB;

CREATE TABLE silver_customers (                    -- PII masked: no raw name, email, phone or street address
    customer_id CHAR(7) PRIMARY KEY, customer_name_masked VARCHAR(20), email_sha256 CHAR(64), email_domain VARCHAR(60),
    phone_masked VARCHAR(20), address_city_state VARCHAR(60), address_sha256 CHAR(64), zone_id CHAR(3), signup_date DATE
) ENGINE = InnoDB;

CREATE TABLE silver_restaurants (
    restaurant_id CHAR(4) PRIMARY KEY, restaurant_name VARCHAR(100) NOT NULL, zone_id CHAR(3) NOT NULL,
    cuisine VARCHAR(50) NOT NULL, cuisine_path VARCHAR(100) NOT NULL, kitchen_capacity TINYINT UNSIGNED NOT NULL
) ENGINE = InnoDB;

CREATE TABLE silver_drivers (
    driver_id CHAR(5) PRIMARY KEY, driver_name_masked VARCHAR(20), vehicle_type VARCHAR(10) NOT NULL, driver_rating DECIMAL(3,2)
) ENGINE = InnoDB;

CREATE TABLE silver_orders (                       -- one row per order, enriched with weather + traffic
    order_id CHAR(7) PRIMARY KEY, customer_id CHAR(7) NOT NULL, restaurant_id CHAR(4) NOT NULL, zone_id CHAR(3) NOT NULL,
    driver_id CHAR(5), order_ts DATETIME NOT NULL, order_status VARCHAR(10) NOT NULL, payment_method VARCHAR(20),
    item_count SMALLINT NOT NULL, food_revenue DECIMAL(8,2) NOT NULL, delivery_fee DECIMAL(5,2) NOT NULL,
    tip_amount DECIMAL(5,2) NOT NULL, kitchen_queue_at_order SMALLINT NOT NULL, kitchen_capacity TINYINT UNSIGNED NOT NULL,
    quoted_eta_min SMALLINT NOT NULL,
    original_delivery_min SMALLINT,               -- Assignment 2 value, before simulation
    sim_weather_delay_min TINYINT NOT NULL,       -- SIMULATED: rain severity / wind rule
    sim_traffic_delay_min TINYINT NOT NULL,       -- SIMULATED: congestion rule
    delivery_min SMALLINT, delivered_ts DATETIME, is_late TINYINT, is_cancelled TINYINT NOT NULL, order_rating TINYINT,
    temperature_c DECIMAL(5,1), precipitation_mm DECIMAL(5,2), wind_speed_kmh DECIMAL(5,1),
    weather_condition VARCHAR(20) NOT NULL, weather_severity TINYINT NOT NULL,
    typical_speed_mph DECIMAL(5,2) NOT NULL, congestion_level VARCHAR(10) NOT NULL,
    INDEX ix_silver_orders_zone_ts (zone_id, order_ts)
) ENGINE = InnoDB;

CREATE TABLE silver_order_items (
    order_id CHAR(7) NOT NULL, order_line_no TINYINT NOT NULL, item_id CHAR(5) NOT NULL, item_name VARCHAR(100) NOT NULL,
    quantity TINYINT NOT NULL, unit_price DECIMAL(6,2) NOT NULL,
    PRIMARY KEY (order_id, order_line_no)
) ENGINE = InnoDB;

CREATE TABLE silver_quarantine (                   -- rejected rows with the rule they failed
    quarantine_id INT PRIMARY KEY, source_table VARCHAR(40) NOT NULL, reason VARCHAR(120) NOT NULL,
    record_key VARCHAR(80) NOT NULL, record_json JSON NOT NULL
) ENGINE = InnoDB;

-- ---------------------------------------------------------------------
-- GOLD: curated analytics combining domain records with API enrichments
-- ---------------------------------------------------------------------
CREATE TABLE gold_daily_zone_weather (             -- daily delivery performance by zone and weather condition
    order_date DATE NOT NULL, zone_id CHAR(3) NOT NULL, zone_name VARCHAR(40) NOT NULL,
    weather_condition VARCHAR(20) NOT NULL, weather_severity TINYINT NOT NULL,
    orders INT NOT NULL, cancelled INT NOT NULL, food_revenue DECIMAL(10,2) NOT NULL, delivered INT NOT NULL, late INT NOT NULL,
    avg_delivery_min DECIMAL(6,1), late_rate_pct DECIMAL(5,1), cancel_rate_pct DECIMAL(5,1),
    avg_precip_mm DECIMAL(6,2), avg_temp_c DECIMAL(5,2),
    PRIMARY KEY (order_date, zone_id, weather_condition)
) ENGINE = InnoDB;

CREATE TABLE gold_hourly_congestion (              -- lateness vs typical congestion by zone x day-of-week x hour
    zone_id CHAR(3) NOT NULL, day_of_week TINYINT NOT NULL, hour_of_day TINYINT NOT NULL,
    typical_speed_mph DECIMAL(5,2) NOT NULL, congestion_level VARCHAR(10) NOT NULL, zone_name VARCHAR(40) NOT NULL,
    orders INT NOT NULL, cancelled INT NOT NULL, food_revenue DECIMAL(10,2) NOT NULL, delivered INT NOT NULL, late INT NOT NULL,
    avg_delivery_min DECIMAL(6,1), late_rate_pct DECIMAL(5,1), cancel_rate_pct DECIMAL(5,1),
    PRIMARY KEY (zone_id, day_of_week, hour_of_day)
) ENGINE = InnoDB;

CREATE TABLE gold_kitchen_load_weather (           -- kitchen load band x weather severity
    load_band VARCHAR(25) NOT NULL, weather_severity TINYINT NOT NULL,
    orders INT NOT NULL, cancelled INT NOT NULL, food_revenue DECIMAL(10,2) NOT NULL, delivered INT NOT NULL, late INT NOT NULL,
    avg_delivery_min DECIMAL(6,1), late_rate_pct DECIMAL(5,1), cancel_rate_pct DECIMAL(5,1),
    PRIMARY KEY (load_band, weather_severity)
) ENGINE = InnoDB;
-- @@MEDALLION_END


-- =====================================================================
-- PART 2a: STAR SCHEMA (built from Silver by SQL ELT)
-- Grain of fact_order: ONE ROW PER CUSTOMER ORDER (delivered or cancelled).
-- Dimensions: dim_date, dim_hour, dim_zone, dim_restaurant (SCD Type 2),
--             dim_customer (PII-masked), dim_driver, dim_conditions (weather x congestion)
-- =====================================================================
-- @@STAR_START
USE kitchen_dw;
DROP TABLE IF EXISTS fact_order;
DROP TABLE IF EXISTS dim_date, dim_hour, dim_zone, dim_restaurant, dim_customer, dim_driver, dim_conditions, stg_restaurant_changes;

CREATE TABLE dim_date (
    date_key      INT          PRIMARY KEY,          -- yyyymmdd
    full_date     DATE         NOT NULL UNIQUE,
    day_of_week   TINYINT      NOT NULL,             -- 1 = Sunday (MySQL DAYOFWEEK)
    day_name      VARCHAR(10)  NOT NULL,
    is_weekend    TINYINT      NOT NULL,
    week_start    DATE         NOT NULL,             -- Monday of the week
    month_num     TINYINT      NOT NULL,
    month_name    VARCHAR(10)  NOT NULL,
    year_num      SMALLINT     NOT NULL
) ENGINE = InnoDB;

CREATE TABLE dim_hour (
    hour_key      TINYINT      PRIMARY KEY,          -- 0..23
    hour_label    CHAR(5)      NOT NULL,             -- '18:00'
    daypart       VARCHAR(12)  NOT NULL
) ENGINE = InnoDB;

CREATE TABLE dim_zone (
    zone_key       SMALLINT     AUTO_INCREMENT PRIMARY KEY,
    zone_id        CHAR(3)      NOT NULL UNIQUE,     -- business key
    zone_name      VARCHAR(40)  NOT NULL,
    traffic_region VARCHAR(60)  NOT NULL
) ENGINE = InnoDB;

CREATE TABLE dim_restaurant (                        -- SCD TYPE 2 on kitchen_capacity
    restaurant_key   INT          AUTO_INCREMENT PRIMARY KEY,   -- surrogate key
    restaurant_id    CHAR(4)      NOT NULL,                     -- business key
    restaurant_name  VARCHAR(100) NOT NULL,
    cuisine          VARCHAR(50)  NOT NULL,
    cuisine_family   VARCHAR(50)  NOT NULL,
    zone_id          CHAR(3)      NOT NULL,
    kitchen_capacity TINYINT UNSIGNED NOT NULL,                 -- tracked attribute
    valid_from       DATE         NOT NULL,
    valid_to         DATE         NOT NULL,                     -- 9999-12-31 for the current row
    is_current       TINYINT      NOT NULL,
    UNIQUE KEY uq_restaurant_version (restaurant_id, valid_from)
) ENGINE = InnoDB;

CREATE TABLE dim_customer (
    customer_key         INT          AUTO_INCREMENT PRIMARY KEY,
    customer_id          CHAR(7)      NOT NULL UNIQUE,
    customer_name_masked VARCHAR(20),
    email_domain         VARCHAR(60),
    home_zone_id         CHAR(3),
    signup_date          DATE,
    tenure_band          VARCHAR(12)  NOT NULL
) ENGINE = InnoDB;

CREATE TABLE dim_driver (
    driver_key         INT          AUTO_INCREMENT PRIMARY KEY,
    driver_id          CHAR(5)      NOT NULL UNIQUE,            -- 'NONE' = unknown member (cancelled orders)
    driver_name_masked VARCHAR(20),
    vehicle_type       VARCHAR(10)  NOT NULL,
    driver_rating      DECIMAL(3,2)
) ENGINE = InnoDB;

CREATE TABLE dim_conditions (                        -- junk dimension: weather x congestion at order time
    conditions_key    SMALLINT     AUTO_INCREMENT PRIMARY KEY,
    weather_condition VARCHAR(20)  NOT NULL,
    weather_severity  TINYINT      NOT NULL,
    congestion_level  VARCHAR(10)  NOT NULL,
    UNIQUE KEY uq_conditions (weather_condition, congestion_level)
) ENGINE = InnoDB;

CREATE TABLE fact_order (
    order_key            INT          AUTO_INCREMENT PRIMARY KEY,
    order_id             CHAR(7)      NOT NULL UNIQUE,          -- degenerate dimension
    date_key             INT          NOT NULL,
    hour_key             TINYINT      NOT NULL,
    zone_key             SMALLINT     NOT NULL,
    restaurant_key       INT          NOT NULL,                 -- version valid on the order date (SCD2)
    customer_key         INT          NOT NULL,
    driver_key           INT          NOT NULL,
    conditions_key       SMALLINT     NOT NULL,
    item_count           SMALLINT     NOT NULL,
    food_revenue         DECIMAL(8,2) NOT NULL,
    delivery_fee         DECIMAL(5,2) NOT NULL,
    tip_amount           DECIMAL(5,2) NOT NULL,
    kitchen_queue        SMALLINT     NOT NULL,
    quoted_eta_min       SMALLINT     NOT NULL,
    delivery_min         SMALLINT     NULL,                     -- NULL for cancelled orders
    weather_delay_min    TINYINT      NOT NULL,                 -- simulated (see Silver)
    traffic_delay_min    TINYINT      NOT NULL,                 -- simulated (see Silver)
    is_delivered         TINYINT      NOT NULL,
    is_late              TINYINT      NULL,
    is_cancelled         TINYINT      NOT NULL,
    order_rating         TINYINT      NULL,
    precipitation_mm     DECIMAL(5,2) NULL,
    temperature_c        DECIMAL(5,1) NULL,
    typical_speed_mph    DECIMAL(5,2) NULL,
    CONSTRAINT fk_fact_date       FOREIGN KEY (date_key)       REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_hour       FOREIGN KEY (hour_key)       REFERENCES dim_hour (hour_key),
    CONSTRAINT fk_fact_zone       FOREIGN KEY (zone_key)       REFERENCES dim_zone (zone_key),
    CONSTRAINT fk_fact_restaurant FOREIGN KEY (restaurant_key) REFERENCES dim_restaurant (restaurant_key),
    CONSTRAINT fk_fact_customer   FOREIGN KEY (customer_key)   REFERENCES dim_customer (customer_key),
    CONSTRAINT fk_fact_driver     FOREIGN KEY (driver_key)     REFERENCES dim_driver (driver_key),
    CONSTRAINT fk_fact_conditions FOREIGN KEY (conditions_key) REFERENCES dim_conditions (conditions_key)
) ENGINE = InnoDB;

-- ---------------------------------------------------------------------
-- ELT: populate dimensions from Silver
-- ---------------------------------------------------------------------
INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, is_weekend, week_start, month_num, month_name, year_num)
WITH RECURSIVE d AS (
    SELECT DATE('2026-08-01') AS dt
    UNION ALL SELECT dt + INTERVAL 1 DAY FROM d WHERE dt < '2026-10-31'
)
SELECT CAST(DATE_FORMAT(dt, '%Y%m%d') AS UNSIGNED), dt, DAYOFWEEK(dt), DAYNAME(dt),
       CASE WHEN DAYOFWEEK(dt) IN (1, 7) THEN 1 ELSE 0 END,
       dt - INTERVAL WEEKDAY(dt) DAY, MONTH(dt), MONTHNAME(dt), YEAR(dt)
FROM d;

INSERT INTO dim_hour (hour_key, hour_label, daypart)
WITH RECURSIVE h AS (SELECT 0 AS hr UNION ALL SELECT hr + 1 FROM h WHERE hr < 23)
SELECT hr, CONCAT(LPAD(hr, 2, '0'), ':00'),
       CASE WHEN hr BETWEEN 6 AND 10 THEN 'Breakfast' WHEN hr BETWEEN 11 AND 14 THEN 'Lunch'
            WHEN hr BETWEEN 15 AND 16 THEN 'Afternoon' WHEN hr BETWEEN 17 AND 21 THEN 'Dinner'
            ELSE 'Late night' END
FROM h;

INSERT INTO dim_zone (zone_id, zone_name, traffic_region)
SELECT p.zone_id,
       CASE p.zone_id WHEN 'Z01' THEN 'Hyde Park' WHEN 'Z02' THEN 'Loop' WHEN 'Z03' THEN 'South Loop'
                      WHEN 'Z04' THEN 'West Loop' WHEN 'Z05' THEN 'Lincoln Park' WHEN 'Z06' THEN 'Wicker Park'
                      WHEN 'Z07' THEN 'Logan Square' ELSE 'Pilsen' END,
       MIN(p.traffic_region)
FROM silver_traffic_profile p GROUP BY p.zone_id ORDER BY p.zone_id;

-- SCD2 initial load: one current version per restaurant, valid from the start of the data
INSERT INTO dim_restaurant (restaurant_id, restaurant_name, cuisine, cuisine_family, zone_id, kitchen_capacity,
                            valid_from, valid_to, is_current)
SELECT restaurant_id, restaurant_name, cuisine, SUBSTRING_INDEX(cuisine_path, ' > ', 1), zone_id, kitchen_capacity,
       '2026-08-01', '9999-12-31', 1
FROM silver_restaurants ORDER BY restaurant_id;

INSERT INTO dim_customer (customer_id, customer_name_masked, email_domain, home_zone_id, signup_date, tenure_band)
SELECT customer_id, customer_name_masked, email_domain, zone_id, signup_date,
       CASE WHEN signup_date >= '2026-05-01' THEN 'New (<3 mo)' WHEN signup_date >= '2025-08-01' THEN '3-12 months'
            ELSE '1 year+' END
FROM silver_customers ORDER BY customer_id;

INSERT INTO dim_driver (driver_id, driver_name_masked, vehicle_type, driver_rating) VALUES ('NONE', NULL, 'none', NULL);
INSERT INTO dim_driver (driver_id, driver_name_masked, vehicle_type, driver_rating)
SELECT driver_id, driver_name_masked, vehicle_type, driver_rating FROM silver_drivers ORDER BY driver_id;

INSERT INTO dim_conditions (weather_condition, weather_severity, congestion_level)
SELECT DISTINCT weather_condition, weather_severity, congestion_level FROM silver_orders
ORDER BY weather_severity, weather_condition, congestion_level;

-- ---------------------------------------------------------------------
-- SCD TYPE 2 change (simulated business event): on 2026-09-15 Thai Garden (R011) expands its
-- kitchen from 15 to 20 concurrent orders, and Tacos Pilsen (R037), one of the busiest kitchens, from 12 to 16.
-- Step 1 expires the current version; step 2 inserts the new current version.
-- ---------------------------------------------------------------------
CREATE TABLE stg_restaurant_changes (
    restaurant_id CHAR(4) PRIMARY KEY, new_kitchen_capacity TINYINT UNSIGNED NOT NULL, effective_date DATE NOT NULL
) ENGINE = InnoDB;
INSERT INTO stg_restaurant_changes VALUES ('R011', 20, '2026-09-15'), ('R037', 16, '2026-09-15');

UPDATE dim_restaurant r
JOIN stg_restaurant_changes c ON c.restaurant_id = r.restaurant_id
SET r.valid_to = c.effective_date - INTERVAL 1 DAY, r.is_current = 0
WHERE r.is_current = 1 AND r.kitchen_capacity <> c.new_kitchen_capacity;

INSERT INTO dim_restaurant (restaurant_id, restaurant_name, cuisine, cuisine_family, zone_id, kitchen_capacity,
                            valid_from, valid_to, is_current)
SELECT r.restaurant_id, r.restaurant_name, r.cuisine, r.cuisine_family, r.zone_id, c.new_kitchen_capacity,
       c.effective_date, '9999-12-31', 1
FROM dim_restaurant r
JOIN stg_restaurant_changes c ON c.restaurant_id = r.restaurant_id
WHERE r.valid_to = c.effective_date - INTERVAL 1 DAY;

-- ---------------------------------------------------------------------
-- ELT: populate the fact table; each order joins the restaurant VERSION valid on its order date
-- ---------------------------------------------------------------------
INSERT INTO fact_order (order_id, date_key, hour_key, zone_key, restaurant_key, customer_key, driver_key, conditions_key,
                        item_count, food_revenue, delivery_fee, tip_amount, kitchen_queue, quoted_eta_min, delivery_min,
                        weather_delay_min, traffic_delay_min, is_delivered, is_late, is_cancelled, order_rating,
                        precipitation_mm, temperature_c, typical_speed_mph)
SELECT o.order_id, CAST(DATE_FORMAT(o.order_ts, '%Y%m%d') AS UNSIGNED), HOUR(o.order_ts), z.zone_key, r.restaurant_key,
       c.customer_key, COALESCE(d.driver_key, dn.driver_key), k.conditions_key,
       o.item_count, o.food_revenue, o.delivery_fee, o.tip_amount, o.kitchen_queue_at_order, o.quoted_eta_min, o.delivery_min,
       o.sim_weather_delay_min, o.sim_traffic_delay_min, CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END,
       o.is_late, o.is_cancelled, o.order_rating, o.precipitation_mm, o.temperature_c, o.typical_speed_mph
FROM silver_orders o
JOIN dim_zone       z ON z.zone_id = o.zone_id
JOIN dim_restaurant r ON r.restaurant_id = o.restaurant_id AND DATE(o.order_ts) BETWEEN r.valid_from AND r.valid_to
JOIN dim_customer   c ON c.customer_id = o.customer_id
LEFT JOIN dim_driver d ON d.driver_id = o.driver_id
JOIN dim_driver    dn ON dn.driver_id = 'NONE'
JOIN dim_conditions k ON k.weather_condition = o.weather_condition AND k.congestion_level = o.congestion_level
ORDER BY o.order_ts;
-- @@STAR_END

-- @@CHECKS_START
-- @@QUERY C1: row counts
SELECT 'fact_order' AS tbl, COUNT(*) AS n FROM fact_order UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_hour', COUNT(*) FROM dim_hour UNION ALL SELECT 'dim_zone', COUNT(*) FROM dim_zone
UNION ALL SELECT 'dim_restaurant', COUNT(*) FROM dim_restaurant UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_driver', COUNT(*) FROM dim_driver UNION ALL SELECT 'dim_conditions', COUNT(*) FROM dim_conditions;

-- @@QUERY C2: SCD2 proof (expired and current versions of the changed restaurants)
SELECT restaurant_key, restaurant_id, restaurant_name, kitchen_capacity, valid_from, valid_to, is_current
FROM dim_restaurant WHERE restaurant_id IN (SELECT restaurant_id FROM stg_restaurant_changes)
ORDER BY restaurant_id, valid_from;

-- @@QUERY C3: orders joined to each SCD2 version
SELECT r.restaurant_name, r.kitchen_capacity, r.valid_from, r.valid_to, r.is_current, COUNT(*) AS orders
FROM fact_order f JOIN dim_restaurant r ON r.restaurant_key = f.restaurant_key
WHERE r.restaurant_id IN ('R011', 'R037') GROUP BY r.restaurant_key ORDER BY r.restaurant_id, r.valid_from;
-- @@CHECKS_END


-- =====================================================================
-- PART 2b: ANALYTICAL QUERIES (fact + dimensions)
-- =====================================================================
-- @@ANALYTICS_START
-- @@QUERY A1: How does the late-delivery rate change with weather in each zone, with subtotals per zone and overall? (WITH ROLLUP)
SELECT
    COALESCE(z.zone_name, 'ALL ZONES')                                AS zone,
    CASE WHEN GROUPING(k.weather_condition) = 1 THEN 'ALL WEATHER' ELSE k.weather_condition END AS weather,
    COUNT(*)                                                          AS orders,
    SUM(f.is_delivered)                                               AS delivered,
    ROUND(100 * SUM(f.is_late) / NULLIF(SUM(f.is_delivered), 0), 1)  AS late_rate_pct,
    ROUND(AVG(f.delivery_min), 1)                                     AS avg_delivery_min
FROM fact_order f
JOIN dim_zone z       ON z.zone_key = f.zone_key
JOIN dim_conditions k ON k.conditions_key = f.conditions_key
GROUP BY z.zone_name, k.weather_condition WITH ROLLUP
ORDER BY GROUPING(z.zone_name), z.zone_name, GROUPING(k.weather_condition), late_rate_pct DESC;

-- @@QUERY A2: Did the SCD2 kitchen expansion change performance? Late rate and queue pressure per restaurant version
SELECT r.restaurant_name, r.kitchen_capacity, r.valid_from, r.valid_to,
       COUNT(*)                                                         AS orders,
       ROUND(AVG(f.kitchen_queue / r.kitchen_capacity), 2)              AS avg_queue_to_capacity,
       ROUND(100 * SUM(f.is_late) / NULLIF(SUM(f.is_delivered), 0), 1) AS late_rate_pct
FROM fact_order f
JOIN dim_restaurant r ON r.restaurant_key = f.restaurant_key
WHERE r.restaurant_id IN (SELECT restaurant_id FROM dim_restaurant GROUP BY restaurant_id HAVING COUNT(*) > 1)
GROUP BY r.restaurant_key, r.restaurant_name, r.kitchen_capacity, r.valid_from, r.valid_to
ORDER BY r.restaurant_name, r.valid_from;

-- @@QUERY A3: Which dayparts and congestion levels produce the most late deliveries on weekdays vs weekends?
SELECT CASE d.is_weekend WHEN 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type, h.daypart, k.congestion_level,
       COUNT(*) AS orders,
       ROUND(100 * SUM(f.is_late) / NULLIF(SUM(f.is_delivered), 0), 1) AS late_rate_pct,
       ROUND(AVG(f.typical_speed_mph), 1) AS avg_typical_mph
FROM fact_order f
JOIN dim_date d       ON d.date_key = f.date_key
JOIN dim_hour h       ON h.hour_key = f.hour_key
JOIN dim_conditions k ON k.conditions_key = f.conditions_key
GROUP BY day_type, h.daypart, k.congestion_level
HAVING COUNT(*) >= 20
ORDER BY late_rate_pct DESC
LIMIT 12;

-- @@QUERY A4: Weekly revenue and late-rate trend per cuisine family, with week-over-week revenue change
WITH weekly AS (
    SELECT d.week_start, r.cuisine_family,
           SUM(f.food_revenue) AS revenue,
           100 * SUM(f.is_late) / NULLIF(SUM(f.is_delivered), 0) AS late_rate
    FROM fact_order f
    JOIN dim_date d       ON d.date_key = f.date_key
    JOIN dim_restaurant r ON r.restaurant_key = f.restaurant_key
    GROUP BY d.week_start, r.cuisine_family
)
SELECT week_start, cuisine_family, ROUND(revenue, 2) AS revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER w) / LAG(revenue) OVER w, 1) AS wow_revenue_pct,
       ROUND(late_rate, 1) AS late_rate_pct
FROM weekly
WINDOW w AS (PARTITION BY cuisine_family ORDER BY week_start)
ORDER BY cuisine_family, week_start;

-- @@QUERY A5: Which vehicle types stay on time in heavy congestion and rain?
SELECT dr.vehicle_type, k.congestion_level,
       CASE WHEN k.weather_severity >= 2 THEN 'Rain / heavy rain' WHEN k.weather_severity = 1 THEN 'Drizzle / light rain' ELSE 'Dry' END AS weather_group,
       COUNT(*) AS deliveries,
       ROUND(100 * (1 - AVG(f.is_late)), 1) AS on_time_pct,
       ROUND(AVG(f.delivery_min), 1) AS avg_delivery_min
FROM fact_order f
JOIN dim_driver dr    ON dr.driver_key = f.driver_key
JOIN dim_conditions k ON k.conditions_key = f.conditions_key
WHERE f.is_delivered = 1
GROUP BY dr.vehicle_type, k.congestion_level, weather_group
HAVING COUNT(*) >= 10
ORDER BY dr.vehicle_type, k.congestion_level, weather_group;
-- @@ANALYTICS_END


-- =====================================================================
-- PART 2b: OPTIMIZATION of the most expensive query (A1, WITH ROLLUP)
-- Baseline plan: 2,499 fact rows fetched through fk_fact_zone, 2,499 single-row lookups into
-- dim_conditions, then a sort of all 2,499 rows before the rollup.
-- MySQL has no materialized views, so a summary table is used as one: it pre-aggregates the fact
-- to zone x conditions grain (and denormalizes the two grouping attributes) and is refreshed by ETL.
-- run_sql.py OPTIMIZE measures A1 before, runs this block, measures A1_MV after, and checks that
-- both return identical results.
-- =====================================================================
-- @@OPTIMIZE_START
DROP TABLE IF EXISTS mv_zone_conditions_perf;

CREATE TABLE mv_zone_conditions_perf (
    zone_key          SMALLINT     NOT NULL,
    conditions_key    SMALLINT     NOT NULL,
    zone_name         VARCHAR(40)  NOT NULL,
    weather_condition VARCHAR(20)  NOT NULL,
    orders            INT          NOT NULL,
    delivered         INT          NOT NULL,
    late              INT          NOT NULL,
    delivery_min_sum  INT          NOT NULL,   -- SUM(delivery_min) over delivered orders
    delivery_min_cnt  INT          NOT NULL,   -- COUNT(delivery_min), so AVG can be rebuilt exactly
    refreshed_at      DATETIME     NOT NULL,
    PRIMARY KEY (zone_key, conditions_key),
    INDEX ix_mv_zone_weather (zone_name, weather_condition)
) ENGINE = InnoDB;

-- refresh (run at the end of every ETL load)
INSERT INTO mv_zone_conditions_perf
SELECT z.zone_key, k.conditions_key, z.zone_name, k.weather_condition,
       COUNT(*), SUM(f.is_delivered), COALESCE(SUM(f.is_late), 0), COALESCE(SUM(f.delivery_min), 0), COUNT(f.delivery_min), NOW()
FROM fact_order f
JOIN dim_zone z       ON z.zone_key = f.zone_key
JOIN dim_conditions k ON k.conditions_key = f.conditions_key
GROUP BY z.zone_key, k.conditions_key, z.zone_name, k.weather_condition;

-- @@QUERY A1_MV: A1 rewritten to read the summary table (same output)
SELECT
    COALESCE(zone_name, 'ALL ZONES')                                         AS zone,
    CASE WHEN GROUPING(weather_condition) = 1 THEN 'ALL WEATHER' ELSE weather_condition END AS weather,
    SUM(orders)                                                              AS orders,
    SUM(delivered)                                                           AS delivered,
    ROUND(100 * SUM(late) / NULLIF(SUM(delivered), 0), 1)                    AS late_rate_pct,
    ROUND(SUM(delivery_min_sum) / NULLIF(SUM(delivery_min_cnt), 0), 1)       AS avg_delivery_min
FROM mv_zone_conditions_perf
GROUP BY zone_name, weather_condition WITH ROLLUP
ORDER BY GROUPING(zone_name), zone_name, GROUPING(weather_condition), late_rate_pct DESC;
-- @@OPTIMIZE_END


-- =====================================================================
-- PART 4a: SHOWCASE — queries that play to the relational warehouse's strengths
-- =====================================================================
-- @@SHOWCASE_START
-- @@QUERY W1: Is delivery reliability improving over time? Daily late rate with a 7-day moving average, flagging rainy days (time-series window over the date dimension)
WITH daily AS (
    SELECT d.full_date, d.day_name,
           COUNT(*) AS orders,
           SUM(f.is_late) AS late, SUM(f.is_delivered) AS delivered,
           MAX(k.weather_severity) AS worst_weather_severity
    FROM fact_order f
    JOIN dim_date d       ON d.date_key = f.date_key
    JOIN dim_conditions k ON k.conditions_key = f.conditions_key
    GROUP BY d.full_date, d.day_name
)
SELECT full_date, day_name, orders,
       ROUND(100 * late / NULLIF(delivered, 0), 1) AS late_rate_pct,
       ROUND(100 * SUM(late) OVER w7 / NULLIF(SUM(delivered) OVER w7, 0), 1) AS late_rate_7day_avg,
       CASE WHEN worst_weather_severity >= 2 THEN 'rain' ELSE '' END AS rain_day
FROM daily
WINDOW w7 AS (ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
ORDER BY full_date;

-- @@QUERY W2: How do revenue and lateness roll up from month to daypart to congestion level? (three-level WITH ROLLUP hierarchy across two dimensions)
SELECT CASE WHEN GROUPING(d.month_name) = 1 THEN 'ALL MONTHS' ELSE d.month_name END        AS month,
       CASE WHEN GROUPING(h.daypart) = 1 THEN 'ALL DAYPARTS' ELSE h.daypart END             AS daypart,
       CASE WHEN GROUPING(k.congestion_level) = 1 THEN 'ALL' ELSE k.congestion_level END    AS congestion,
       COUNT(*) AS orders,
       ROUND(SUM(f.food_revenue), 2) AS food_revenue,
       ROUND(100 * SUM(f.is_late) / NULLIF(SUM(f.is_delivered), 0), 1) AS late_rate_pct
FROM fact_order f
JOIN dim_date d       ON d.date_key = f.date_key
JOIN dim_hour h       ON h.hour_key = f.hour_key
JOIN dim_conditions k ON k.conditions_key = f.conditions_key
WHERE h.daypart IN ('Lunch', 'Dinner')
GROUP BY d.month_num, d.month_name, h.daypart, k.congestion_level WITH ROLLUP
HAVING GROUPING(d.month_name) = GROUPING(d.month_num)
ORDER BY GROUPING(d.month_num), d.month_num, GROUPING(h.daypart), h.daypart, GROUPING(k.congestion_level), k.congestion_level;

-- @@QUERY W3: Why SCD2 matters: kitchen load measured against the capacity that was valid at order time vs. today's capacity
SELECT r_hist.restaurant_name,
       CASE WHEN r_hist.is_current = 1 THEN 'after expansion' ELSE 'before expansion' END AS period,
       COUNT(*) AS orders,
       r_hist.kitchen_capacity AS capacity_at_order_time,
       r_now.kitchen_capacity  AS capacity_today,
       ROUND(AVG(f.kitchen_queue / r_hist.kitchen_capacity), 2) AS load_ratio_correct_scd2,
       ROUND(AVG(f.kitchen_queue / r_now.kitchen_capacity), 2)  AS load_ratio_naive_current,
       SUM(CASE WHEN f.kitchen_queue >= r_hist.kitchen_capacity THEN 1 ELSE 0 END) AS overloaded_orders_scd2,
       SUM(CASE WHEN f.kitchen_queue >= r_now.kitchen_capacity THEN 1 ELSE 0 END)  AS overloaded_orders_naive
FROM fact_order f
JOIN dim_restaurant r_hist ON r_hist.restaurant_key = f.restaurant_key
JOIN dim_restaurant r_now  ON r_now.restaurant_id = r_hist.restaurant_id AND r_now.is_current = 1
WHERE r_hist.restaurant_id IN (SELECT restaurant_id FROM dim_restaurant WHERE is_current = 0)
GROUP BY r_hist.restaurant_key, r_hist.restaurant_name, r_hist.is_current, r_hist.kitchen_capacity, r_now.kitchen_capacity
ORDER BY r_hist.restaurant_name, period DESC;
-- @@SHOWCASE_END
