CREATE VIEW payment_window AS
SELECT
  region,
  TUMBLE_START(TO_TIMESTAMP(event_time), INTERVAL '5' MINUTE) AS window_start,
  COUNT(*) AS total_events,
  SUM(CASE WHEN event_type = 'payment_failed' THEN 1 ELSE 0 END) AS failures,
  SUM(amount) AS total_revenue
FROM payment_events_v2
GROUP BY
  region,
  TUMBLE(TO_TIMESTAMP(event_time), INTERVAL '5' MINUTE);
