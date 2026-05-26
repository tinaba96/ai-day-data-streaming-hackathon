# Design Decisions

Short notes on the calls that shaped this project — and the ones we
deliberately said no to.

## 1. Detection: rules + z-scores, not an ML model

We considered an isolation forest or a streaming autoencoder. We picked
plain z-scores and percentile rules because:

- **Auditable.** A risk analyst can read the SQL and explain to a regulator
  exactly why an event was flagged. ML scores can't.
- **Cold-start.** A new merchant has no training data. Z-scores against a
  trailing window work from minute 60.
- **Cheap to iterate.** New rule = new column in a view. New ML feature =
  retrain + redeploy.

The Cortex Agent is doing the *narrative*, not the *decision*. That
separation matters: if the model hallucinates, no false alert fires —
only the explanation text would be off, and the operator still sees the
raw signal vector.

## 2. Snowflake Dynamic Tables, not external Flink/Kafka Streams

The hackathon constraint was "one platform if possible." Dynamic Tables
gave us:

- declarative tumbling windows,
- automatic incremental refresh,
- the same SQL surface for batch backfill and live serving.

We lose sub-second latency. We're fine with that — fraud ops act in
minutes, not milliseconds, and 60-second windows align cleanly with the
"trailing baseline" idea anyway.

## 3. Geo-velocity uses haversine, not PostGIS

We needed great-circle distance, not full geospatial joins. A 6-line UDF
is enough and runs in-warehouse with no extension.

## 4. The Cortex Agent gets a *signal vector*, not raw events

Two reasons:

- **Token budget.** Sending the 312 raw transactions in a spike would
  blow the context for a one-sentence summary.
- **Determinism.** With aggregated signals + `temperature=0` +
  `response_format=json_object`, two calls on the same window return the
  same explanation. The dashboard caches the first one.

## 5. We materialize explanations into a table

`EXPLAIN_ANOMALY()` is a UDF over `CORTEX.COMPLETE`. We don't want the
dashboard to re-pay the token cost on every refresh. The `EXPLAIN_NEW_ANOMALIES`
task runs once a minute and only inserts new (window, merchant) pairs.

## 6. What we cut for scope

- **No Slack integration.** `recommended_action` is in the row; wiring a
  webhook is one task away but not on the demo critical path.
- **No per-user behavioral baseline.** Only merchant-level baselines.
  Adding user-level would double storage and the demo doesn't need it.
- **No backtest harness.** We seed sample + spike JSON; a real eval set
  would need labeled historical fraud.

## 7. Things that would matter in production

- Late-arriving events: our tumbling windows are wall-clock, not
  watermarked. Replace with event-time watermarking before going live.
- Schema evolution: `decline_reason` is a free-text string. In real
  use, normalize to a closed enum at ingest.
- Prompt drift: pin the model version + snapshot the prompt in
  `AGENT_PROMPTS` and version-control it.
- Cost controls: cap `CORTEX.COMPLETE` calls per hour; degrade to a
  templated explanation if exceeded.
