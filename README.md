# 💸 Real-Time Payment Anomaly Detection (Flink + ML + Streaming Agent)

A real-time streaming system that detects payment anomalies across regions using Confluent Flink SQL, ML-based anomaly detection, and autonomous streaming agents.

---

# 🚀 Overview

This project demonstrates a full **event-driven AI system**:

1. Payment events stream into Kafka (Confluent Cloud)
2. Flink SQL performs real-time window aggregation
3. ML detects anomalies in transaction behavior
4. Streaming agent layer defines automated response strategies

---

# 🧠 Architecture

- Kafka (Confluent Cloud) → event ingestion
- Flink SQL → stream processing
- ML_DETECT_ANOMALIES → anomaly detection
- Streaming Agent → automated response logic

---

# ⚙️ Pipeline Steps

## 1. Source Table
Defines incoming payment events.

## 2. Window Aggregation
Groups events into 5-minute windows per region.

## 3. ML Anomaly Detection
Detects abnormal spikes in:
- transaction count
- failure rate
- revenue

## 4. Agent Layer
Defines automated responses:
- throttle_region
- fraud_suspected
- log_and_monitor

---

# 📊 Example Use Case

1. Normal traffic flows into system
2. Sudden spike in CA region occurs
3. ML detects anomaly in real time
4. Agent determines response strategy

---

# 🔥 Key Features

- Real-time streaming analytics
- Window-based aggregation
- ML-powered anomaly detection
- Autonomous decision layer (agent)
- Fully event-driven architecture

---

# 🧪 How to Reproduce

1. Deploy SQL scripts in Confluent Flink environment
2. Run `payment_generator.py`
3. Send sample events
4. Observe anomaly detection in `payment_anomalies`

---

# 📦 Sample Event

```json
{
  "event_type": "payment_failed",
  "user_id": "u1",
  "region": "CA",
  "amount": 25.0,
  "event_time": 1769258400000
}
```
