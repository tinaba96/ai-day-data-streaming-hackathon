CREATE VIEW payment_anomalies AS
SELECT
  region,
  window_start,
  total_events,
  failures,
  total_revenue,
  ML_DETECT_ANOMALIES(
    CAST(total_events AS DOUBLE),
    window_start,
    JSON_OBJECT(
      'minTrainingSize' VALUE 5,
      'confidencePercentage' VALUE 95.0,
      'enableStl' VALUE FALSE
    )
  ) OVER (
    PARTITION BY region
    ORDER BY window_start
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS anomaly_result
FROM payment_window;
