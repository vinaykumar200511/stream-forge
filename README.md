# StreamForge — Distributed Python IoT Event Processor

[![CI Pipeline](https://github.com/axlero-solutions/streamforge/actions/workflows/ci.yml/badge.svg)](https://github.com/axlero-solutions/streamforge/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Kafka KRaft](https://img.shields.io/badge/Kafka-KRaft%20Mode-black.svg)](https://kafka.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111+-009688.svg)](https://fastapi.tiangolo.com)
[![React Flow](https://img.shields.io/badge/React%20Flow-12.0+-ff0072.svg)](https://reactflow.dev/)
[![Zero Data Loss](https://img.shields.io/badge/Fault%20Tolerance-0.00%25%20Loss-brightgreen.svg)]()

> **Built for FleetPulse Analytics** — A distributed, stateful stream processing engine engineered to ingest, aggregate, and analyze cold-chain temperature telemetry from **50,000+ refrigerated logistics trucks** in real time.

---

## 1. The Problem — Cold-Chain at Scale

| | Before StreamForge | After StreamForge |
|:---|:---|:---|
| **Detection latency** | 12 hours (nightly batch ETL) | **< 1 second** (continuous stream) |
| **Cargo at risk** | Entire trailer load spoils before alert | Operator notified within milliseconds |
| **Alerting model** | Manual review of next-day reports | Autonomous webhook dispatch to dispatch center |
| **Scale** | Single-tenant, single batch | 50,000 trucks · 5 tenants · > 13,000 ev/s |

**Client:** FleetPulse Analytics — a SaaS logistics platform managing nationwide refrigerated fleets.  

**The Pain:** Their enterprise customer runs **50,000 refrigerated trucks** hauling temperature-critical cargo — frozen vaccines, dairy, seafood, fresh produce. Cargo must stay within a **−25 °C to −10 °C** band. A compressor failure causes irreversible spoilage within **30 minutes**. The previous system ran a nightly batch ETL job, meaning fleet managers received alerts **12 hours too late** — after the damage was already done. Each incident cost tens of thousands of dollars in destroyed inventory, insurance claims, and regulatory exposure.

**The Solution:** StreamForge — a Python-native, high-throughput distributed stream processor that replaces the batch loop with a **continuous, stateful event pipeline**:
- **Apache Kafka (KRaft mode)** for partitioned, durable telemetry ingestion
- **Distributed aiokafka workers** with **RocksDB local state** for sub-millisecond per-truck rolling windows
- **Kafka changelog replication** for 0.00% state loss across worker crashes (RTO < 2.2 s, verified)
- **FastAPI + WebSockets** for live operational visibility
- **Autonomous anomaly engine** with async webhook dispatch to incident centers

> See full requirements: [`docs/problem_statement.md`](docs/problem_statement.md) · [`docs/PROJECT_SCOPE.md`](docs/PROJECT_SCOPE.md)

---

## 2. Distributed Architecture

![StreamForge Architecture — Four-tier distributed stream processing pipeline](docs/architecture_diagram.jpg)

> _Four-tier pipeline: Ingestion → Stream Processing → API & Observability → React Dashboard._  
> Full diagram source and tier-by-tier breakdown: [`docs/architecture_overview.md`](docs/architecture_overview.md)

### Mermaid (source-controlled)

```mermaid
flowchart TD
    subgraph Ingestion["⬡ Tier 1 — Ingestion"]
        TP["Multi-Tenant Telemetry Simulator\n50k Trucks · 5 Tenants · Fault Injection"]
        TP -->|"Key: customer_id:truck_id"| KR_RAW["Kafka: raw-telemetry\n6 Partitions · KRaft Mode"]
    end

    subgraph Processing["⚙ Tier 2 — Stream Processing"]
        KR_RAW --> W1["Worker 01\nPartitions 0–2"]
        KR_RAW --> W2["Worker 02\nPartitions 3–5"]

        W1 <-->|"Hot-path R/W"| RDB1[("RocksDB\nState Store")]
        W2 <-->|"Hot-path R/W"| RDB2[("RocksDB\nState Store")]

        W1 & W2 -->|"Changelog sync"| KR_CHANGE["Kafka: changelog-topic\n(Compacted · Durable Recovery)"]
        W1 & W2 -->|"5-min rolling avg"| KR_PROC["Kafka: processed-averages"]
        W1 & W2 -->|"Threshold breach"| KR_ALERT["Kafka: alerts-topic"]
    end

    subgraph Backend["🔭 Tier 3 — API & Observability"]
        KR_ALERT --> WHD["Async Webhook Dispatcher\n(Exp. Backoff · Retry)"]
        WHD -->|"HTTP POST"| EXT["Fleet Dispatch Center"]

        KR_PROC --> API["FastAPI Engine"]
        AUTH["JWT Auth · RBAC"] --> API
        API -->|"Prometheus scrape"| PROM["/metrics"]
    end

    subgraph Frontend["📊 Tier 4 — Live Dashboard"]
        API <-->|"WebSocket stream"| UI["React Flow Topology\n+ Recharts Telemetry Graphs"]
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
