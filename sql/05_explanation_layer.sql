CREATE VIEW anomaly_explanations AS
SELECT
  region,
  window_start,
  failures,
  total_events,
  CONCAT(
    'Anomaly detected in ',
    region,
    ' with ',
    CAST(failures AS STRING),
    ' failures out of ',
    CAST(total_events AS STRING)
  ) AS explanation
FROM payment_anomalies
WHERE anomaly_result.is_anomaly = TRUE;
