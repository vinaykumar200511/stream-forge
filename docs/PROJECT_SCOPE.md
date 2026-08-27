# StreamForge — Project Scope & Requirements Specification

## 1. Executive Summary & Business Context
- **Client:** FleetPulse Analytics (SaaS logistics platform for refrigerated cold-chain transportation).
- **Core Problem:** 50,000 refrigerated trucks transporting perishable cargo (frozen vaccines, dairy, seafood, produce) suffer catastrophic spoilage if cargo temperatures exceed safe bands (-25°C to -10°C) for over 30 minutes. Nightly batch ETL reports alert fleet operators 12 hours too late.
- **Solution:** **StreamForge** — A real-time, distributed, stateful Python stream processing engine capable of continuous sub-second ingestion, 5-minute tumbling/sliding window aggregations, instantaneous anomaly detection, and zero data loss resilience.

---

## 2. Core Functional Requirements

### Ingestion Tier (Producer & Ingestion)
- **High-Throughput Ingestion:** Ingest telemetry from 50,000 trucks across 5 multi-tenant fleets at >10,000 events/sec.
- **Strict Per-Truck Partition Ordering:** Kafka messages partitioned deterministically by `customer_id:truck_id`.
- **Out-of-Order Watermark Handling:** Accommodate up to a 30-second grace period for delayed cellular/edge IoT packets.

### Stream Processing Tier (Faust / aiokafka + RocksDB)
- **Windowed Aggregation:** Continuous computation of 5-minute tumbling/rolling temperature averages, min/max metrics, and event count per truck.
- **Local State Storage:** Fast RocksDB local state store per worker partition for sub-millisecond state access.
- **Fault-Tolerant Changelog:** Every local state mutation replicated to a compacted Kafka changelog topic (`changelog-topic`), ensuring instant 0.00% state recovery upon `kill -9` worker crashes.

### Alerting Tier (Autonomous Anomaly Engine)
- **Instant Anomaly Triggers:** Real-time alert dispatch when temperature exceeds safe thresholds (> -10.0°C) or compressor reports `FAULT`.
- **Webhook Dispatcher:** Asynchronous HTTP POST notification delivery to external fleet management dispatch centers with exponential backoff and retry policies.

### API & Observability Tier
- **FastAPI Engine:** Exposes RESTful query endpoints and real-time WebSocket streams for processed truck metrics.
- **Prometheus Metrics:** Standard `/metrics` endpoint tracking throughput (events/sec), processing latency (p50/p95/p99), and error rates.
- **Security:** OAuth2 / JWT token authentication with tenant-level RBAC data isolation.

### Frontend Dashboard
- **React Flow Pipeline Topology:** Visual, live-animated distributed stream architecture diagram showing active Kafka topics, worker health, and throughput.
- **Recharts Telemetry Graphs:** Real-time multi-truck temperature timelines with dynamic anomaly threshold lines.

---

## 3. Non-Functional Requirements & Performance SLAs

| Metric | Target SLA |
| :--- | :--- |
| **Ingestion Throughput** | ≥ 10,000 events/sec sustained per broker |
| **End-to-End Processing Latency** | p95 < 20 ms from ingestion to window aggregation |
| **State Loss on Crash (RPO)** | **0.00%** (Zero data loss via changelog replay) |
| **Worker Recovery Time (RTO)** | < 5.0 seconds |
| **Out-of-Order Tolerance** | 30-second watermark grace period |
| **Data Partitioning Layout** | 6 Kafka Partitions (Replication Factor = 1 in Dev / 3 in Prod) |
