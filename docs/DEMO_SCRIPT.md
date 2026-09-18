# StreamForge — 2-Minute Reviewer Pitch & Demo Guide

## Pitch Hook (30 Seconds)
"In cold-chain logistics, a single refrigeration failure across 50,000 trucks costs millions in spoiled perishable cargo if discovered 12 hours too late. **StreamForge** is a distributed Python event processing engine engineered for FleetPulse Analytics that computes continuous 5-minute tumbling window averages, detects thermal anomalies in real time, and guarantees zero data loss across worker crashes through Kafka changelog replication."

---

## 2-Minute Live Demo Flow

### Before the Demo
Start Kafka and the API in separate terminals, then start the dashboard from `frontend/`:
```powershell
docker compose up -d kafka
python -m uvicorn streamforge.backend.app:app --reload --port 8000
cd frontend
npm run dev
```
Open `http://localhost:5173` for the dashboard and `http://localhost:8000/docs` for the API. The dashboard has fallback data for a UI-only walkthrough, but Kafka must be running for the producer and smoke-test steps.

### 1. Ingestion & Multi-Tenant Telemetry Simulation
Run the IoT simulator streaming telemetry across 500 trucks and 5 customer tenants:
```powershell
python -m streamforge.producer.producer --dry-run --count 10
```
- **Showcase:** Multi-tenant keys (`customer_id:truck_id`), temperature drift, compressor status, and dynamic thermal spike injection.

### 2. Stream Processing & Tumbling Window Computation
- **Showcase:** Distributed stream processing with RocksDB state stores computing rolling temperature averages and evaluating safe threshold limits (-25°C to -10°C).

### 3. Automated Anomaly Webhook Dispatch
- **Showcase:** When a compressor fails, an `AnomalyAlert` is produced to `alerts-topic` and dispatched to client dispatch webhooks with exponential retry backoff.

### 4. Zero Data Loss Crash Failover
- **Showcase:** Kill a worker with `SIGKILL` (`kill -9`) mid-stream and observe instant state restoration from Kafka changelog topic with 0.00% data loss.

### Optional Verification
With Kafka and the API running, execute:
```powershell
python -m pytest tests -q
python scripts/smoke_test_e2e.py
```
