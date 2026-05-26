# Stream Lineage

Source document: **Hackathon_Stream_Lineage_TakahiroInaba**

<https://docs.google.com/document/d/1u8zhv8eAMrhmsi43OFlgyDr2X-A8BzC7tPfNqi7UZ9o/edit?tab=t.0>

> The Google Doc above is the authoritative copy. Access requires sign-in.
> Inline mirror below — paste the doc content here when ready, or export
> the doc as Markdown and replace this section.

## Pipeline at a glance

```
payment_events_v2  (Kafka topic, Flink source)
        │
        ▼
payment_window         (sql/02_window_aggregation.sql)
        │  5-min tumbling window, per region
        ▼
payment_anomalies      (sql/03_anomaly_detection.sql)
        │  ML_DETECT_ANOMALIES on total_events
        ▼
payment_recovery_agent (sql/04_agent_definition.sql)
        │  remote MCP agent — decides response action
        ▼
anomaly_explanations   (sql/05_explanation_layer.sql)
        │  human-readable summary per anomalous window
        ▼
        consumer / dashboard / alert sink
```

## Inputs

| Source | Type | Notes |
|---|---|---|
| `payment_events_v2` | Kafka topic | Produced by `generator/payment_generator.py`; one record per payment event |

## Outputs

| Object | Type | Consumers |
|---|---|---|
| `payment_window` | Flink view | feeds `payment_anomalies` |
| `payment_anomalies` | Flink view | feeds `anomaly_explanations`, agent input |
| `anomaly_explanations` | Flink view | dashboard, alert worker |
| `payment_recovery_agent` | Flink Agent | emits action JSON (`throttle_region` / `fraud_suspected` / `log_and_monitor`) |
