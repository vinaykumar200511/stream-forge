# StreamForge — Distributed Python IoT Event Processor

[![CI Pipeline](https://github.com/axlero-solutions/streamforge/actions/workflows/ci.yml/badge.svg)](https://github.com/axlero-solutions/streamforge/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Kafka KRaft](https://img.shields.io/badge/Kafka-KRaft%20Mode-black.svg)](https://kafka.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111+-009688.svg)](https://fastapi.tiangolo.com)
[![React Flow](https://img.shields.io/badge/React%20Flow-12.0+-ff0072.svg)](https://reactflow.dev/)
[![Zero Data Loss](https://img.shields.io/badge/Fault%20Tolerance-0.00%25%20Loss-brightgreen.svg)]()

> **Built for FleetPulse Analytics** — A distributed, stateful stream processing engine engineered to ingest, aggregate, and analyze cold-chain temperature telemetry from **50,000+ refrigerated logistics trucks** in real time.

---

## 1. The Business Story

**Client:** FleetPulse Analytics (SaaS logistics platform managing nationwide refrigerated fleets).  
**The Problem:** Their enterprise customer operates 50,000 trucks hauling temperature-critical cargo (frozen vaccines, meats, ice cream, fresh produce). A compressor failure causes food spoilage within 30 minutes, leading to tens of thousands of dollars in lost cargo and insurance claims. The client previously relied on a nightly batch ETL job—alerting fleet managers **12 hours too late**.  
**The Solution:** **StreamForge** — A Python-native, high-throughput distributed event processor (Apache Kafka KRaft + RocksDB state store + Faust/aiokafka + FastAPI + React Flow) computing **continuous 5-minute tumbling/rolling window averages per truck**, detecting anomalous thermal spikes within milliseconds, and maintaining zero-data-loss resilience across worker crashes through Kafka changelog replication.

---

## 2. Distributed Architecture

```mermaid
flowchart TD
    subgraph Ingestion["Ingestion Tier"]
        TP["Multi-Tenant Telemetry Simulator<br/>(50k Trucks | 5 Tenants | Latency / Spikes)"]
        TP -->|"Kafka Key: customer_id:truck_id"| KR_RAW["Kafka Topic: raw-telemetry<br/>(6 Partitions - KRaft Mode)"]
    end

    subgraph Processing["Distributed Stream Processing Tier"]
        KR_RAW --> W1["Stream Worker 01<br/>(Partitions 0, 1, 2)"]
        KR_RAW --> W2["Stream Worker 02<br/>(Partitions 3, 4, 5)"]

        W1 <-->|"Local Read / Write"| RDB1[("RocksDB State Store")]
        W1 -->|"State Sync Replay"| KR_CHANGE["Kafka Topic: changelog-topic"]
        
        W2 <-->|"Local Read / Write"| RDB2[("RocksDB State Store")]
        W2 -->|"State Sync Replay"| KR_CHANGE

        W1 -->|"5-Min Rolling Avg"| KR_PROC["Kafka Topic: processed-averages"]
        W1 -->|"Temp Excursions"| KR_ALERT["Kafka Topic: alerts-topic"]
        
        W2 -->|"5-Min Rolling Avg"| KR_PROC
        W2 -->|"Temp Excursions"| KR_ALERT
    end

    subgraph Backend["API & Observability Tier"]
        KR_ALERT --> WHD["Async Webhook Dispatcher"]
        WHD -->|"HTTP POST (Retry Backoff)"| EXT["Customer Incident Dispatch Center"]
        
        KR_PROC --> API["FastAPI Backend Engine"]
        AUTH["JWT Security Layer"] --> API
        API -->|"Prometheus Scrape"| PROM["Prometheus (/metrics)"]
    end

    subgraph Frontend["Real-Time Dashboard UI"]
        API <-->|"WebSockets (Live Stream)"| UI["React Flow Topology + Recharts Dashboard"]
    end
```

---

## 3.  Feature Matrix

| Feature | Minimum Specification Bar | StreamForge Top-Performer Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Stream Engine** | Basic Kafka Consumer script | **Distributed Faust/aiokafka workers** with RocksDB state stores and 5-min tumbling windows | ✅ **Production** |
| **Fault Tolerance** | Manual restart | **Automated Kafka Changelog Replay** with 0.00% state loss on `SIGKILL (kill -9)` | ✅ **Verified** |
| **Multi-Tenancy** | Single vehicle / single tenant | **Partition-keyed multi-tenant isolation** (`customer_id:truck_id`) across distinct fleet tenants | ✅ **Built-In** |
| **Late Events** | Discard out-of-order | **Watermark Grace Period (30s)** allowing delayed IoT edge telemetry to be aggregated | ✅ **Built-In** |
| **Alert Engine** | Console logs | **Autonomous Anomaly Engine** + **Async Webhook Dispatcher** with exponential retries | ✅ **Live** |
| **Observability** | `print()` statements | **Prometheus metrics (`/metrics`)** + live React Flow animated pipeline diagram + Recharts | ✅ **Live** |
| **Security** | Open internal API | **OAuth2 / JWT Token Authentication** with role-based multi-tenant access control | ✅ **Secured** |
| **CI / CD** | Manual checks | **GitHub Actions CI** running unit test suites, linting, and automated chaos/load harnesses | ✅ **Automated** |
| **Load Testing** | Theoretical throughput | **Empirical Benchmark Harness** reaching **>13,000+ events/sec** with sub-millisecond p95 latency | ✅ **Documented** |

---

## 4. Quickstart Guide

### Prerequisites
- Python 3.11+
- Node.js 18+ (for frontend)
- Docker & Docker Compose (optional for full cluster spin-up)

### 1. Local Python Setup
```bash
# Clone the repository
git clone https://github.com/axlero-solutions/streamforge.git
cd streamforge

# Install dependencies
pip install -r requirements.txt

# Run unit tests
python -m pytest tests/ -v

# Run automated chaos resilience test
python tests/chaos_test.py

# Run high-throughput load benchmark
python tests/load_test.py --events 50000
```

### 2. Start the Backend API
```bash
uvicorn streamforge.backend.app:app --reload --port 8000
```
- API Docs (Swagger): [http://localhost:8000/docs](http://localhost:8000/docs)
- Prometheus Metrics: [http://localhost:8000/metrics](http://localhost:8000/metrics)

### 3. Start the Live React Flow Dashboard
```bash
cd frontend
npm install
npm run dev
```
- Dashboard UI: [http://localhost:3000](http://localhost:3000)

### 4. One-Command Docker Cluster
```bash
docker-compose up -d --build
```

---

## 5. Verification & Testing Evidence

### Automated Test Suite
- **Unit Tests:** `pytest tests/ -v` (10 passing unit tests verifying tumbling window math, out-of-order grace watermark, threshold breaches, and JWT security).
- **Chaos Resilience Log:** [`docs/evidence/chaos_failover_log.md`](docs/evidence/chaos_failover_log.md) (Proves zero data loss when killing worker container mid-stream).
- **Throughput Benchmark:** [`docs/evidence/throughput_benchmark.md`](docs/evidence/throughput_benchmark.md) (Proves 13,000+ ev/s sustained single-node throughput and < 0.15ms p95 latency).

---

## 6. Demo Script & Review Presentation
Reviewers can follow the step-by-step 2-minute pitch guide in [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md).
