-- =====================================================================
-- 05_explanation_layer.sql
-- Final consumer view: anomalies + LLM explanation in one place.
-- This is what the dashboard / Slack alert task reads from.
-- =====================================================================

USE SCHEMA PAYMENTS_RT.STREAMING;

-- ---------------------------------------------------------------------
-- Materialized result of EXPLAIN_ANOMALY so we don't re-call the LLM
-- every time the dashboard refreshes. Refreshed by the task below.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE ANOMALY_EXPLANATIONS (
    window_start       TIMESTAMP_NTZ,
    merchant_id        STRING,
    signal_payload     VARIANT,
    explanation        VARIANT,
    summary            STRING,
    primary_signal     STRING,
    recommended_action STRING,
    created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start, merchant_id)
);

-- ---------------------------------------------------------------------
-- Task: for every newly-flagged anomaly, call EXPLAIN_ANOMALY once and
-- persist the result. Runs every minute, picks up only unseen rows.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TASK EXPLAIN_NEW_ANOMALIES
    WAREHOUSE = WH_STREAMING
    SCHEDULE  = '1 minute'
AS
MERGE INTO ANOMALY_EXPLANATIONS t USING (
    SELECT
        a.window_start,
        a.merchant_id,
        OBJECT_CONSTRUCT(
            'window_start',     a.window_start,
            'merchant_id',      a.merchant_id,
            'merchant_category',a.merchant_category,
            'txn_count',        a.txn_count,
            'total_amount',     a.total_amount,
            'decline_rate',     a.decline_rate,
            'z_count',          a.z_count,
            'z_amount',         a.z_amount,
            'is_volume_spike',  a.is_volume_spike,
            'is_value_spike',   a.is_value_spike,
            'is_decline_storm', a.is_decline_storm
        ) AS signal_payload
    FROM ANOMALY_MERCHANT_WINDOW a
    LEFT JOIN ANOMALY_EXPLANATIONS x
      ON  x.window_start = a.window_start
      AND x.merchant_id  = a.merchant_id
    WHERE a.is_anomaly = TRUE
      AND x.merchant_id IS NULL
) s
ON  t.window_start = s.window_start
AND t.merchant_id  = s.merchant_id
WHEN NOT MATCHED THEN INSERT (
    window_start, merchant_id, signal_payload, explanation,
    summary, primary_signal, recommended_action
)
VALUES (
    s.window_start,
    s.merchant_id,
    s.signal_payload,
    EXPLAIN_ANOMALY(s.signal_payload),
    EXPLAIN_ANOMALY(s.signal_payload):summary::STRING,
    EXPLAIN_ANOMALY(s.signal_payload):primary_signal::STRING,
    EXPLAIN_ANOMALY(s.signal_payload):recommended_action::STRING
);

ALTER TASK EXPLAIN_NEW_ANOMALIES RESUME;

-- ---------------------------------------------------------------------
-- Dashboard / alert view — one row per flagged window with explanation.
-- Joins back to per-transaction outliers and geo violations for context.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW ANOMALY_EXPLAINED AS
SELECT
    e.window_start,
    e.merchant_id,
    e.signal_payload:merchant_category::STRING AS merchant_category,
    e.signal_payload:txn_count::NUMBER          AS txn_count,
    e.signal_payload:total_amount::NUMBER       AS total_amount,
    e.signal_payload:decline_rate::FLOAT        AS decline_rate,
    e.signal_payload:z_count::FLOAT             AS z_count,
    e.summary,
    e.primary_signal,
    e.recommended_action,
    e.explanation,
    (SELECT COUNT(*) FROM ANOMALY_VALUE_OUTLIER v
        WHERE v.merchant_id = e.merchant_id
          AND v.event_ts >= e.window_start
          AND v.event_ts <  DATEADD('minute', 1, e.window_start)
    ) AS value_outlier_count,
    (SELECT COUNT(DISTINCT g.user_id) FROM ANOMALY_GEO_VELOCITY g
        WHERE g.event_ts >= e.window_start
          AND g.event_ts <  DATEADD('minute', 1, e.window_start)
    ) AS geo_violation_users
FROM ANOMALY_EXPLANATIONS e;
