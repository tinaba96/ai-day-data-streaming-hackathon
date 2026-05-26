-- =====================================================================
-- 04_agent_definition.sql
-- Cortex Agent that turns an anomaly signal vector into an explanation.
-- Uses CORTEX.COMPLETE wrapped in a UDF so it can be called from SQL.
-- =====================================================================

USE SCHEMA PAYMENTS_RT.STREAMING;

-- ---------------------------------------------------------------------
-- Prompt template stored as a constant so it's easy to iterate on.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE AGENT_PROMPTS (
    name       STRING PRIMARY KEY,
    body       STRING,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO AGENT_PROMPTS t USING (
    SELECT 'anomaly_explainer' AS name,
$$You are a fraud-operations analyst.

You receive one JSON object that describes a 1-minute payment window that the
detection layer has already flagged as anomalous. Your job is to produce a
*tight* operator-facing explanation.

Return JSON with exactly these keys:
  summary           – one sentence, plain English, no hedging
  primary_signal    – the single rule that contributed most (volume_spike |
                       value_spike | decline_storm | value_outlier |
                       geo_velocity)
  contributing      – array of every rule that fired
  recommended_action – short imperative; one of:
                       throttle_merchant, step_up_auth, block_user,
                       notify_oncall, monitor

Do not invent numbers. Only reference signals present in the input.$$ AS body
) s
ON t.name = s.name
WHEN MATCHED THEN UPDATE SET body = s.body, updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (name, body) VALUES (s.name, s.body);

-- ---------------------------------------------------------------------
-- UDF: signal-vector → JSON explanation.
-- Snowflake Cortex's COMPLETE() returns a string; we pass back as VARIANT.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION EXPLAIN_ANOMALY(signal_json VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    TO_VARIANT(
        PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE(
                'claude-4-sonnet',
                ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT(
                        'role',    'system',
                        'content', (SELECT body FROM AGENT_PROMPTS WHERE name = 'anomaly_explainer')
                    ),
                    OBJECT_CONSTRUCT(
                        'role',    'user',
                        'content', TO_JSON(signal_json)
                    )
                ),
                OBJECT_CONSTRUCT(
                    'temperature',    0,
                    'response_format', OBJECT_CONSTRUCT('type', 'json_object')
                )
            )
        )
    )
$$;

-- ---------------------------------------------------------------------
-- Smoke test
-- ---------------------------------------------------------------------
-- SELECT EXPLAIN_ANOMALY(OBJECT_CONSTRUCT(
--     'merchant_id',     'M-7741',
--     'window_start',    '2026-05-26 10:14:00',
--     'txn_count',       312,
--     'baseline_mean',   42,
--     'z_count',         11.4,
--     'decline_rate',    0.06,
--     'is_volume_spike', TRUE
-- ));
