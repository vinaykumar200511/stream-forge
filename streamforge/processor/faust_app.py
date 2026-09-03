"""Faust application and Kafka topic bindings for StreamForge."""

from __future__ import annotations

import logging

import faust

from streamforge.common.config import settings
from streamforge.common.models import AnomalyAlert, ProcessedAggregate, RawTelemetryEvent

logger = logging.getLogger(__name__)

app = faust.App(
    id="streamforge-worker",
    broker=f"kafka://{settings.KAFKA_BOOTSTRAP_SERVERS}",
    datadir=settings.ROCKSDB_STATE_DIR,
    store="rocksdb://",
    processing_guarantee="at_least_once",
    consumer_auto_offset_reset="earliest",
    broker_commit_interval=1.0,
    loghandlers=[],
)

raw_telemetry_topic = app.topic(
    settings.KAFKA_RAW_TOPIC,
    key_type=str,
    value_type=RawTelemetryEvent,
    partitions=settings.KAFKA_NUM_PARTITIONS,
)

processed_topic = app.topic(
    settings.KAFKA_PROCESSED_TOPIC,
    key_type=str,
    value_type=ProcessedAggregate,
    partitions=settings.KAFKA_NUM_PARTITIONS,
)

alerts_topic = app.topic(
    settings.KAFKA_ALERTS_TOPIC,
    key_type=str,
    value_type=AnomalyAlert,
    partitions=settings.KAFKA_NUM_PARTITIONS,
)

changelog_topic = app.topic(
    settings.KAFKA_CHANGELOG_TOPIC,
    key_type=str,
    value_type=bytes,
    partitions=settings.KAFKA_NUM_PARTITIONS,
    config={"cleanup.policy": "compact"},
)

truck_state_table = app.Table(
    "truck-window-state",
    default=dict,
    partitions=settings.KAFKA_NUM_PARTITIONS,
    help="Per-truck rolling window aggregation state.",
).tumbling(
    size=settings.WINDOW_SIZE_SECONDS,
    expires=settings.WINDOW_SIZE_SECONDS * 4,
)

logger.info(
    "Faust app '%s' initialized | broker=%s",
    app.conf.id,
    settings.KAFKA_BOOTSTRAP_SERVERS,
)

__all__ = [
    "app",
    "raw_telemetry_topic",
    "processed_topic",
    "alerts_topic",
    "changelog_topic",
    "truck_state_table",
]