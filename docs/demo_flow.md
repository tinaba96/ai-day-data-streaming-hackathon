# Demo Flow — 5 minutes

A scripted walkthrough so the judges see signal, not setup.

## 0:00 — 0:30 — Context (slide)

> Payments at scale produce noisy, bursty traffic. Threshold alerts either
> miss real attacks or page on every Black Friday. We built a streaming
> pipeline that flags anomalies **and explains them in plain English**, so
> the on-call operator can act in seconds, not minutes.

## 0:30 — 1:30 — Show the pipeline

1. Open `sql/01_create_source_table.sql` — point out `RAW_PAYMENTS` + stream.
2. Open `sql/02_window_aggregation.sql` — dynamic table, 1-minute tumbling
   windows, 60-minute trailing baseline. *"This is the whole feature store."*
3. Open `sql/03_anomaly_detection.sql` — three signal families:
   z-score volume/value, percentile outlier, geo velocity.

## 1:30 — 2:30 — Live: baseline

```bash
python generator/payment_generator.py --mode normal --rate 50 --duration 0
```

Run the dashboard query:

```sql
SELECT COUNT(*) FROM ANOMALY_EXPLAINED
WHERE window_start > DATEADD('minute', -5, CURRENT_TIMESTAMP());
```

Expected: 0 rows. *"Quiet system, nothing fires."*

## 2:30 — 3:30 — Live: inject a spike

In a second terminal:

```bash
python generator/payment_generator.py --mode spike --rate 800 --duration 60
```

Refresh the dashboard query within a minute:

```sql
SELECT window_start, merchant_id, txn_count, z_count,
       primary_signal, summary, recommended_action
FROM   ANOMALY_EXPLAINED
ORDER BY window_start DESC
LIMIT 5;
```

Talk through:
- `z_count` is ~10σ above baseline.
- `primary_signal = volume_spike`.
- `summary` is a one-liner the on-call can paste into Slack.
- `recommended_action = throttle_merchant`.

## 3:30 — 4:15 — Live: geo-velocity

```bash
python generator/payment_generator.py --mode geo --user user-1042
```

```sql
SELECT * FROM ANOMALY_GEO_VELOCITY ORDER BY event_ts DESC LIMIT 5;
```

Show Seattle → Paris → Tokyo in 30 seconds. The Cortex Agent flags
`primary_signal = geo_velocity` and `recommended_action = step_up_auth`.

## 4:15 — 5:00 — Wrap

- Detection layer is plain SQL — auditable, testable, no opaque models.
- Explanation layer is a single UDF that any analyst can re-prompt.
- Latency end-to-end: < 90 seconds from event → explained alert.
- Cost: one warehouse, one Cortex model, one task. No external services.

If time: open `docs/design_decisions.md` and walk the "why" page.
