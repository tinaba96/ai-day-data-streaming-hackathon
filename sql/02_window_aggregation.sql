-- =====================================================================
-- 02_window_aggregation.sql
-- 1-minute tumbling-window aggregations driven by a Dynamic Table.
-- Produces the rolling stats that the anomaly detector compares against.
-- =====================================================================

USE SCHEMA PAYMENTS_RT.STREAMING;

-- ---------------------------------------------------------------------
-- Per-merchant, per-minute aggregation.
-- TIME_SLICE() gives us tumbling 60s windows aligned to wall clock.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE PAYMENT_STATS_1MIN
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_STREAMING
AS
SELECT
    TIME_SLICE(event_ts, 60, 'SECOND', 'START')           AS window_start,
    merchant_id,
    ANY_VALUE(merchant_category)                          AS merchant_category,
    COUNT(*)                                              AS txn_count,
    SUM(amount)                                           AS total_amount,
    AVG(amount)                                           AS avg_amount,
    MAX(amount)                                           AS max_amount,
    PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY amount)  AS p50_amount,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY amount)  AS p99_amount,
    SUM(IFF(status = 'DECLINED', 1, 0))                   AS decline_count,
    SUM(IFF(status = 'DECLINED', 1, 0)) / COUNT(*)        AS decline_rate,
    COUNT(DISTINCT user_id)                               AS unique_users,
    COUNT(DISTINCT country)                               AS unique_countries
FROM RAW_PAYMENTS
WHERE event_ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY 1, 2;

-- ---------------------------------------------------------------------
-- Trailing 60-minute baseline per merchant (for z-score comparison).
-- We don't read the current window into its own baseline — we lag by 1.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE PAYMENT_BASELINE_60MIN
    TARGET_LAG = '2 minutes'
    WAREHOUSE  = WH_STREAMING
AS
SELECT
    window_start,
    merchant_id,
    AVG(txn_count) OVER (
        PARTITION BY merchant_id
        ORDER BY window_start
        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
    ) AS baseline_count_mean,
    STDDEV(txn_count) OVER (
        PARTITION BY merchant_id
        ORDER BY window_start
        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
    ) AS baseline_count_std,
    AVG(total_amount) OVER (
        PARTITION BY merchant_id
        ORDER BY window_start
        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
    ) AS baseline_amount_mean,
    STDDEV(total_amount) OVER (
        PARTITION BY merchant_id
        ORDER BY window_start
        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
    ) AS baseline_amount_std,
    AVG(decline_rate) OVER (
        PARTITION BY merchant_id
        ORDER BY window_start
        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
    ) AS baseline_decline_rate
FROM PAYMENT_STATS_1MIN;

-- ---------------------------------------------------------------------
-- Per-user geo trace (used for the geo-velocity rule).
-- Keeps the last two locations and the elapsed time between them.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE USER_GEO_TRACE
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_STREAMING
AS
SELECT
    event_id,
    event_ts,
    user_id,
    city,
    country,
    lat,
    lon,
    LAG(lat)      OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_lat,
    LAG(lon)      OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_lon,
    LAG(city)     OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_city,
    LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_event_ts
FROM RAW_PAYMENTS;
