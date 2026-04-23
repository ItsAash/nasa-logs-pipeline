# 🚀 Real-Time NASA Log Anomaly Detection Pipeline

A production-style **streaming data engineering pipeline** that ingests NASA HTTP logs, streams them through Kafka, processes them using Spark Structured Streaming, and detects anomalies in real time.

This project simulates how large-scale observability / security systems work in real-world production environments.

---

## 📌 Project Overview

This system processes NASA server logs in both **batch and streaming modes** to detect:

- Bot traffic

- Endpoint scanning behavior

- Traffic spikes

- Error surges

- Suspicious IP activity

- Hotspot endpoints

It combines:

- Batch analytics (historical profiling)

- Real-time streaming detection

- Distributed messaging (Kafka)
