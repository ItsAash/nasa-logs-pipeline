# 🚀 Real-Time NASA Log Anomaly Detection Pipeline

A production-style **streaming data engineering + observability system** that ingests NASA HTTP logs, processes them in real time using Kafka and Spark Structured Streaming, stores them in Delta Lake, and visualizes anomalies through a Streamlit mission-control dashboard.

This project simulates real-world **security monitoring / observability platforms** like Datadog or Splunk.

---

## 📌 What This Project Does

The system processes NASA HTTP logs in both batch and streaming modes to detect:

- 🚨 Traffic spikes
- 🤖 Bot / abnormal request patterns
- 📉 Error surges (4xx / 5xx spikes)
- 🌐 Suspicious IP behavior
- 📡 Endpoint abuse / scanning
- 📊 System-wide anomalies in real time

---

## 🧠 Architecture

```
NASA Logs
   ↓
Kafka (Streaming Ingestion)
   ↓
Spark Structured Streaming
   ↓
Delta Lake (Bronze → Silver → Gold)
   ↓
Feature Aggregation Layer
   ↓
Streamlit Dashboard (Observability UI)
```

---

## 🏗️ Data Layers (Delta Lake)

### Bronze Layer (Raw Ingestion)

- Raw NASA HTTP logs streamed from Kafka

### Silver Layer (Cleaned Data)

- Parsed logs
- Extracted IP, endpoint, status codes, timestamps

### Gold Layer (Analytics Tables)

Stored under:

```
/Volumes/workspace/default/logs_volume/gold
```

Tables:

- `output/anomalies` → Detected anomalies with severity scoring
- `gold/hourly_stats` → Traffic aggregation per hour
- `gold/ip_behaviour` → IP-level behavioral profiling
- `gold/endpoint_stats` → Endpoint-level analytics

---

## 🚨 Anomaly Detection Logic

The system detects anomalies using:

- Z-score deviation from baseline traffic
- Rate multiplier spikes
- Error rate thresholds
- IP behavior deviation patterns

### Severity Levels

- 🔴 CRITICAL → System-threatening spikes
- 🟠 HIGH → Significant abnormal activity
- 🟡 MEDIUM → Suspicious deviations
- 🟢 LOW → Minor anomalies

---

## 📊 Dashboard (Streamlit)

The UI is a **mission-control style observability dashboard** featuring:

- Real-time KPI cards
- Live anomaly feed
- Traffic trend charts (last 48 hours)
- Error rate sparkline
- IP intelligence view
- Endpoint analytics

---

## 🧰 Tech Stack

### Data Engineering

- Apache Kafka
- Apache Spark Structured Streaming
- Databricks
- Delta Lake

### Backend

- Python
- Databricks SQL connector

### Frontend

- Streamlit
- Plotly
- Custom CSS system

---

## ⚙️ How to Run

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start pipeline (Kafka + Spark)

Ensure logs are flowing into Delta tables:

```
output/anomalies
gold/hourly_stats
gold/ip_behaviour
gold/endpoint_stats
```

### 3. Run dashboard

```bash
streamlit run app.py
```

---

## 🎯 Key Engineering Concepts

- Real-time stream processing
- Windowed aggregations (Spark)
- Event-driven architecture
- Stateful anomaly detection
- Data lakehouse design (Delta Lake)
- UI/UX observability design

---

## 🚀 Future Improvements

- ML-based anomaly detection (Isolation Forest / LSTM)
- Alerting system (Slack / Email)
- Role-based access control
- Multi-tenant dashboards
- Time-travel debugging using Delta history

---

## 🛰️ Summary

A full-stack **real-time observability system** that transforms raw NASA HTTP logs into actionable intelligence with streaming analytics and a production-grade dashboard UI.
