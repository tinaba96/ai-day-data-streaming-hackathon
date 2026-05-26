-- =====================================================================
-- 01_create_source_table.sql
-- Raw payment event ingestion: landing table, stream, and pipe.
-- Target: Snowflake (Snowpipe Streaming compatible).
-- =====================================================================

CREATE DATABASE IF NOT EXISTS PAYMENTS_RT;
CREATE SCHEMA   IF NOT EXISTS PAYMENTS_RT.STREAMING;
USE SCHEMA PAYMENTS_RT.STREAMING;

-- ---------------------------------------------------------------------
-- Landing table for raw payment events.
-- Wide enough to support volume, value, geo, and decline-rate rules.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_PAYMENTS (
    event_id          STRING        NOT NULL,
    event_ts          TIMESTAMP_NTZ NOT NULL,
    user_id           STRING        NOT NULL,
    merchant_id       STRING        NOT NULL,
    merchant_category STRING,
    amount            NUMBER(12,2)  NOT NULL,
    currency          STRING(3)     NOT NULL,
    country           STRING(2),
    city              STRING,
    lat               FLOAT,
    lon               FLOAT,
    payment_method    STRING,
    status            STRING,           -- APPROVED / DECLINED / PENDING
    decline_reason    STRING,
    ingested_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (event_ts);

-- ---------------------------------------------------------------------
-- Stream that downstream tasks/dynamic-tables read from.
-- APPEND_ONLY = TRUE because RAW_PAYMENTS is insert-only.
-- ---------------------------------------------------------------------
CREATE OR REPLACE STREAM RAW_PAYMENTS_STREAM
    ON TABLE RAW_PAYMENTS
    APPEND_ONLY = TRUE;

-- ---------------------------------------------------------------------
-- File format + stage + pipe for the JSON seed data in /data.
-- Used when bootstrapping the demo without a live Kafka source.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FF_JSON
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE STAGE PAYMENT_EVENT_STAGE
    FILE_FORMAT = FF_JSON;

CREATE OR REPLACE PIPE PAYMENT_EVENT_PIPE
    AUTO_INGEST = FALSE
AS
COPY INTO RAW_PAYMENTS (
    event_id, event_ts, user_id, merchant_id, merchant_category,
    amount, currency, country, city, lat, lon,
    payment_method, status, decline_reason
)
FROM (
    SELECT
        $1:event_id::STRING,
        $1:event_ts::TIMESTAMP_NTZ,
        $1:user_id::STRING,
        $1:merchant_id::STRING,
        $1:merchant_category::STRING,
        $1:amount::NUMBER(12,2),
        $1:currency::STRING,
        $1:country::STRING,
        $1:city::STRING,
        $1:lat::FLOAT,
        $1:lon::FLOAT,
        $1:payment_method::STRING,
        $1:status::STRING,
        $1:decline_reason::STRING
    FROM @PAYMENT_EVENT_STAGE
);

-- ---------------------------------------------------------------------
-- Smoke test
-- ---------------------------------------------------------------------
-- PUT file://data/sample_events.json @PAYMENT_EVENT_STAGE AUTO_COMPRESS=FALSE;
-- ALTER PIPE PAYMENT_EVENT_PIPE REFRESH;
-- SELECT COUNT(*) FROM RAW_PAYMENTS;
