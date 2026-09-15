# StreamForge — Telemetry Message Format & Schema Plan

This document establishes the official event contracts, schemas, serialization rules, and Kafka partition strategies for the StreamForge streaming pipeline.

---

## 1. Event Taxonomy & Kafka Topics

| Event Type | Kafka Topic | Partition Key | Throughput | Retention |
| :--- | :--- | :--- | :--- | :--- |
| **Raw Telemetry** | `raw-telemetry` | `customer_id:truck_id` | High (~10k ev/s) | 24 Hours |
| **Window Aggregate** | `processed-averages` | `customer_id:truck_id` | Medium (~200 ev/s) | 7 Days |
| **Anomaly Alert** | `alerts-topic` | `customer_id:truck_id` | Low / Spike-driven | 30 Days |
| **State Changelog** | `changelog-topic` | `customer_id:truck_id` | Internal sync | Compacted |

---

## 2. Topic 1: `raw-telemetry` (Raw IoT Sensor Event)

### Purpose
Emitted by vehicle edge gateway devices every 1 to 5 seconds per truck to stream real-time cold-chain vitals and telematics.

### Partition Key
- **Format:** `"{customer_id}:{truck_id}"` (e.g., `"cust_01:trk_01_0042"`)
- **Rationale:** Ensures all successive events for a single physical vehicle are routed to the exact same Kafka partition, preserving strict chronological ordering.

### Field Specification

| Field Name | Type | Unit / Enum | Constraints | Description |
| :--- | :--- | :--- | :--- | :--- |
| `event_id` | `string (UUIDv4)` | — | Unique | Unique identifier for message deduplication |
| `timestamp` | `float (epoch sec)` | Seconds | > 0 | Event creation timestamp (UTC) |
| `customer_id` | `string` | — | Non-empty | Fleet tenant ID (e.g., `cust_01`) |
| `truck_id` | `string` | — | Non-empty | Physical vehicle asset ID (e.g., `trk_01_0042`) |
| `route_id` | `string` | — | Optional | Active delivery route identifier |
| `temperature` | `float` | °C | -50.0 to +50.0 | Current internal cargo bay temperature |
| `target_temp` | `float` | °C | -30.0 to +10.0 | Thermostat setpoint (e.g., -18.0°C for frozen) |
| `ambient_temp` | `float` | °C | -40.0 to +60.0 | Outside environmental temperature |
| `compressor_status`| `string (Enum)` | `RUNNING`, `DEFROST`, `OFF`, `FAULT` | — | Operational status of refrigeration compressor |
| `door_open` | `boolean` | `true` / `false` | — | Cargo door sensor state |
| `battery_level` | `float` | Percentage (%) | 0.0 to 100.0 | Gateway auxiliary battery percentage |
| `latitude` | `float` | Decimal degrees | -90.0 to +90.0 | GPS latitude coordinate |
| `longitude` | `float` | Decimal degrees | -180.0 to +180.0 | GPS longitude coordinate |
| `speed_kmh` | `float` | km/h | ≥ 0.0 | Vehicle current road speed |

### Sample JSON Payload
```json
{
  "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "timestamp": 1787725800.125,
  "customer_id": "cust_01",
  "truck_id": "trk_01_0042",
  "route_id": "route_01_04",
  "temperature": -18.35,
  "target_temp": -18.00,
  "ambient_temp": 26.40,
  "compressor_status": "RUNNING",
  "door_open": false,
  "battery_level": 96.8,
  "latitude": 37.774929,
  "longitude": -122.419416,
  "speed_kmh": 68.4
}
```

---

## 3. Topic 2: `processed-averages` (5-Minute Window Aggregate)

### Purpose
Computed continuously by the stream processing engine across 5-minute tumbling/sliding windows.

### Field Specification

| Field Name | Type | Unit | Description |
| :--- | :--- | :--- | :--- |
| `aggregate_id` | `string (UUID)` | — | Unique window aggregate ID |
| `customer_id` | `string` | — | Fleet tenant ID |
| `truck_id` | `string` | — | Vehicle asset ID |
| `window_start` | `float (epoch)` | Seconds | Window lower boundary timestamp |
| `window_end` | `float (epoch)` | Seconds | Window upper boundary timestamp |
| `sample_count` | `integer` | Count | Number of telemetry packets aggregated |
| `avg_temperature` | `float` | °C | Mean temperature across window |
| `min_temperature` | `float` | °C | Lowest recorded temperature in window |
| `max_temperature` | `float` | °C | Highest recorded temperature in window |
| `target_temperature`| `float` | °C | Setpoint temperature |
| `is_breached` | `boolean` | `true`/`false` | Flagged if `avg_temperature > SAFE_TEMP_MAX` (-10°C) |

### Sample JSON Payload
```json
{
  "aggregate_id": "e4d9b2a1-5c8f-412e-9d21-f09b3a721c08",
  "customer_id": "cust_01",
  "truck_id": "trk_01_0042",
  "window_start": 1787725500.0,
  "window_end": 1787725800.0,
  "sample_count": 30,
  "avg_temperature": -18.21,
  "min_temperature": -18.60,
  "max_temperature": -17.80,
  "target_temperature": -18.00,
  "is_breached": false,
  "created_at": 1787725800.450
}
```

---

## 4. Topic 3: `alerts-topic` (Real-Time Anomaly Alert)

### Purpose
Dispatched instantaneously upon detecting critical temperature excursions or mechanical failures.

### Field Specification

| Field Name | Type | Enum / Unit | Description |
| :--- | :--- | :--- | :--- |
| `alert_id` | `string (UUID)` | — | Unique incident identifier |
| `customer_id` | `string` | — | Multi-tenant customer identifier |
| `truck_id` | `string` | — | Affected vehicle |
| `severity` | `string (Enum)` | `INFO`, `WARNING`, `CRITICAL` | Incident escalation tier |
| `alert_type` | `string (Enum)` | `HIGH_TEMPERATURE`, `COMPRESSOR_FAILURE`, etc. | Classification code |
| `trigger_temperature` | `float` | °C | Actual temperature that caused the breach |
| `target_temperature` | `float` | °C | Normal expected temperature setpoint |
| `threshold_limit` | `float` | °C | Maximum allowable threshold limit |
| `compressor_status` | `string` | `FAULT` / `OFF` / `RUNNING` | Compressor state at alert time |
| `timestamp` | `float (epoch)` | Seconds | Alert creation timestamp |
| `message` | `string` | — | Human-readable alert summary |

### Sample JSON Payload
```json
{
  "alert_id": "a9103c39-29fa-475a-a309-847294bbca31",
  "customer_id": "cust_01",
  "truck_id": "trk_01_0042",
  "severity": "CRITICAL",
  "alert_type": "HIGH_TEMPERATURE",
  "trigger_temperature": -4.20,
  "target_temperature": -18.00,
  "threshold_limit": -10.00,
  "compressor_status": "FAULT",
  "timestamp": 1787725812.500,
  "message": "Critical thermal excursion: Cargo temp -4.20°C breached safe limit of -10.00°C due to COMPRESSOR FAULT",
  "acknowledged": false
}
```
