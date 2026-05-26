-- =====================================================================
-- 03_anomaly_detection.sql
-- Z-score, percentile, and rule-based anomaly views.
-- Each row carries a signal vector that the Cortex Agent will explain.
-- =====================================================================

USE SCHEMA PAYMENTS_RT.STREAMING;

-- ---------------------------------------------------------------------
-- (1) Volume / value / decline anomalies at the (merchant × window) grain.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW ANOMALY_MERCHANT_WINDOW AS
WITH joined AS (
    SELECT
        s.window_start,
        s.merchant_id,
        s.merchant_category,
        s.txn_count,
        s.total_amount,
        s.avg_amount,
        s.max_amount,
        s.decline_rate,
        s.unique_users,
        b.baseline_count_mean,
        b.baseline_count_std,
        b.baseline_amount_mean,
        b.baseline_amount_std,
        b.baseline_decline_rate
    FROM PAYMENT_STATS_1MIN     s
    LEFT JOIN PAYMENT_BASELINE_60MIN b
      ON  s.merchant_id  = b.merchant_id
      AND s.window_start = b.window_start
)
SELECT
    window_start,
    merchant_id,
    merchant_category,
    txn_count,
    total_amount,
    decline_rate,
    -- z-scores (guard against null/zero stddev)
    DIV0(txn_count    - baseline_count_mean,  NULLIF(baseline_count_std,  0)) AS z_count,
    DIV0(total_amount - baseline_amount_mean, NULLIF(baseline_amount_std, 0)) AS z_amount,
    -- rule flags
    (DIV0(txn_count    - baseline_count_mean,  NULLIF(baseline_count_std,  0)) > 3) AS is_volume_spike,
    (DIV0(total_amount - baseline_amount_mean, NULLIF(baseline_amount_std, 0)) > 3) AS is_value_spike,
    (decline_rate > 0.30 AND txn_count >= 20)                                       AS is_decline_storm,
    -- single overall flag
    ( (DIV0(txn_count    - baseline_count_mean,  NULLIF(baseline_count_std,  0)) > 3)
      OR (DIV0(total_amount - baseline_amount_mean, NULLIF(baseline_amount_std, 0)) > 3)
      OR (decline_rate > 0.30 AND txn_count >= 20)
    ) AS is_anomaly
FROM joined;

-- ---------------------------------------------------------------------
-- (2) Per-transaction value outliers vs. category p99.5.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW ANOMALY_VALUE_OUTLIER AS
WITH category_p995 AS (
    SELECT
        merchant_category,
        PERCENTILE_CONT(0.995) WITHIN GROUP (ORDER BY amount) AS p995_amount
    FROM RAW_PAYMENTS
    WHERE event_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY merchant_category
)
SELECT
    p.event_id,
    p.event_ts,
    p.user_id,
    p.merchant_id,
    p.merchant_category,
    p.amount,
    c.p995_amount,
    (p.amount / NULLIF(c.p995_amount, 0)) AS amount_ratio_to_p995,
    TRUE                                  AS is_value_outlier
FROM RAW_PAYMENTS p
JOIN category_p995 c USING (merchant_category)
WHERE p.amount > c.p995_amount;

-- ---------------------------------------------------------------------
-- (3) Geo-velocity violations.
-- Haversine distance / elapsed minutes; flag > 500 km in < 5 min.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION HAVERSINE_KM(lat1 FLOAT, lon1 FLOAT, lat2 FLOAT, lon2 FLOAT)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    2 * 6371 * ASIN(
        SQRT(
            POWER(SIN(RADIANS(lat2 - lat1) / 2), 2) +
            COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
            POWER(SIN(RADIANS(lon2 - lon1) / 2), 2)
        )
    )
$$;

CREATE OR REPLACE VIEW ANOMALY_GEO_VELOCITY AS
SELECT
    event_id,
    event_ts,
    user_id,
    city,
    prev_city,
    HAVERSINE_KM(lat, lon, prev_lat, prev_lon)             AS distance_km,
    DATEDIFF('second', prev_event_ts, event_ts) / 60.0     AS elapsed_minutes,
    HAVERSINE_KM(lat, lon, prev_lat, prev_lon)
        / NULLIF(DATEDIFF('second', prev_event_ts, event_ts) / 3600.0, 0) AS implied_kmh,
    TRUE AS is_geo_violation
FROM USER_GEO_TRACE
WHERE prev_lat IS NOT NULL
  AND HAVERSINE_KM(lat, lon, prev_lat, prev_lon) > 500
  AND DATEDIFF('second', prev_event_ts, event_ts) < 300;
