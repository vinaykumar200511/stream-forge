"""
StreamForge — Faust Stream Processing Application
==================================================
This module defines the single authoritative Faust ``app`` instance and all
four Kafka topic bindings used by the distributed stream workers.

Topics (sourced from ``streamforge.common.config.settings``):
    raw_telemetry_topic     — inbound IoT truck telemetry (source)
    processed_topic         — 5-minute rolling window aggregates (sink)
    alerts_topic            — threshold-breach anomaly alerts (sink)
    changelog_topic         — RocksDB state changelog for crash recovery (sink)

Usage
-----
Start a worker::

    faust -A streamforge.worker.app worker -l info

Scale to a second worker (different data directory)::

    faust -A streamforge.worker.app worker -l info --datadir ./data/worker2

The Faust app is importable by any agent module::

    from streamforge.worker.app import app, raw_telemetry_topic
"""

from __future__ import annotations

import logging
from typing import Optional

import faust

from streamforge.common.config import settings
from streamforge.common.models import AnomalyAlert, ProcessedAggregate, RawTelemetryEvent

logger = logging.getLogger("streamforge.worker.app")

# ---------------------------------------------------------------------------
# Faust App
# ---------------------------------------------------------------------------

app = faust.App(
    # Unique Kafka consumer-group prefix for this application.
    id="streamforge-worker",

    # Kafka broker(s) — pulled from .env / environment variables.
    broker=f"kafka://{settings.KAFKA_BOOTSTRAP_SERVERS}",

    # RocksDB state directory; each worker uses an isolated subdirectory
    # automatically namespaced by Faust (e.g. ./data/rocksdb_state/worker-1/).
    datadir=settings.ROCKSDB_STATE_DIR,

    # Store backend: RocksDB for persistent, fast local state.
    # Falls back gracefully to memory if the rocksdb extra is missing.
    store="rocksdb://",

    # Processing guarantee: "at_least_once" is the right choice here — the
    # changelog provides idempotent recovery, making duplicate processing safe.
    processing_guarantee="at_least_once",

    # Consumer auto-offset reset — start from the beginning on first run so
    # no telemetry events are silently skipped during initial deployment.
    consumer_auto_offset_reset="earliest",

    # Commit every 1 000 ms to keep consumer lag low without hammering Kafka.
    broker_commit_interval=1.0,

    # Log format aligned with the rest of the StreamForge service.
    loghandlers=[],  # Inherit root logger configured by the process entry-point.
)

# ---------------------------------------------------------------------------
# Faust Topic Bindings
# ---------------------------------------------------------------------------
# Each ``app.topic()`` call declares a Faust-managed Kafka topic.
# Faust will automatically create the topic if it does not yet exist (subject
# to broker ACLs) and handle serialisation / deserialisation transparently.

# ------------------------------------------------------------------
# SOURCE — raw inbound IoT telemetry from the truck simulator / gateway
# Partitioned by "customer_id:truck_id" (set at produce time).
# All events for a given truck are guaranteed to land on the same partition.
# ------------------------------------------------------------------
raw_telemetry_topic: faust.TopicT = app.topic(
    settings.KAFKA_RAW_TOPIC,
    key_type=str,
    value_type=RawTelemetryEvent,
    # 6 partitions match the Kafka cluster layout defined in kafka_admin.py.
    partitions=settings.KAFKA_NUM_PARTITIONS,
    # Internal=False — this is a shared external topic produced to by the
    # simulator / gateway; Faust must not delete it when the app shuts down.
    internal=False,
)

# ------------------------------------------------------------------
# SINK — 5-minute tumbling/rolling window aggregates per truck.
# Consumed by the FastAPI backend to serve the live dashboard.
# ------------------------------------------------------------------
processed_topic: faust.TopicT = app.topic(
    settings.KAFKA_PROCESSED_TOPIC,
    key_type=str,
    value_type=ProcessedAggregate,
    partitions=settings.KAFKA_NUM_PARTITIONS,
    internal=False,
)

# ------------------------------------------------------------------
# SINK — threshold-breach anomaly alerts dispatched to the webhook engine.
# Consumed by the async webhook dispatcher in streamforge.alerts.
# ------------------------------------------------------------------
alerts_topic: faust.TopicT = app.topic(
    settings.KAFKA_ALERTS_TOPIC,
    key_type=str,
    value_type=AnomalyAlert,
    partitions=settings.KAFKA_NUM_PARTITIONS,
    internal=False,
)

# ------------------------------------------------------------------
# SINK — Kafka changelog for RocksDB state recovery after SIGKILL.
# Compacted topic: only the latest state snapshot per key is retained.
# When a worker restarts it replays this topic to restore in-memory state.
# ------------------------------------------------------------------
changelog_topic: faust.TopicT = app.topic(
    settings.KAFKA_CHANGELOG_TOPIC,
    key_type=str,
    # Raw bytes for the serialised RocksDB state snapshot payload.
    value_type=bytes,
    partitions=settings.KAFKA_NUM_PARTITIONS,
    # Log-compacted — keeps only the latest value per key.
    # Broker-side setting; Faust passes this as a topic config override.
    config={"cleanup.policy": "compact"},
    internal=False,
)

# ---------------------------------------------------------------------------
# Faust Tables (Windowed In-Process State — backed by RocksDB)
# ---------------------------------------------------------------------------
# ``app.Table`` creates a distributed, persistent hash-map that Faust
# replicates to the changelog_topic automatically.
# The tumbling window matches the WINDOW_SIZE_SECONDS setting (default 300 s).

# Per-truck rolling aggregate state.  Key: "customer_id:truck_id"
truck_state_table: faust.TableT = app.Table(
    "truck-window-state",
    default=dict,
    partitions=settings.KAFKA_NUM_PARTITIONS,
    help="Per-truck rolling window aggregation state (RocksDB backed).",
).tumbling(
    size=settings.WINDOW_SIZE_SECONDS,
    expires=settings.WINDOW_SIZE_SECONDS * 4,  # Clean up windows older than 4× the window size.
)

logger.info(
    "Faust app '%s' initialised | broker=%s | store=%s | datadir=%s",
    app.conf.id,
    app.conf.broker,
    app.conf.store,
    app.conf.datadir,
)

__all__ = [
    "app",
    "raw_telemetry_topic",
    "processed_topic",
    "alerts_topic",
    "changelog_topic",
    "truck_state_table",
]
