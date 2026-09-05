"""Faust application and Kafka topic bindings for StreamForge."""

from __future__ import annotations

import logging
import json
from typing import Any, Mapping

import faust
from pydantic import ValidationError

from streamforge.common.config import settings
from streamforge.common.models import AnomalyAlert, NormalizedTelemetryEvent, ProcessedAggregate, RawTelemetryEvent

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
    # Keep the wire payload opaque so malformed records can be dropped by the topology.
    value_type=bytes,
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


def _parse_raw_event(value: Any) -> RawTelemetryEvent:
    """Parse a Kafka payload into the validated raw telemetry contract."""
    if isinstance(value, RawTelemetryEvent):
        return value
    if isinstance(value, bytes):
        return RawTelemetryEvent.model_validate_json(value)
    if isinstance(value, str):
        return RawTelemetryEvent.model_validate_json(value)
    if isinstance(value, Mapping):
        return RawTelemetryEvent.model_validate(value)
    raise TypeError(f"Unsupported telemetry payload type: {type(value).__name__}")


def is_valid_telemetry(key: str, value: Any) -> bool:
    """Accept only valid telemetry packets whose temperature is strictly above zero."""
    try:
        event = _parse_raw_event(value)
    except (TypeError, ValueError, ValidationError, json.JSONDecodeError, AttributeError):
        logger.warning("Dropping malformed telemetry record for key=%s", key)
        return False
    if event.temperature is None or event.temperature <= 0:
        logger.debug("Dropping telemetry with non-positive temperature for key=%s: temp=%s", key, event.temperature)
        return False
    return True


def normalize_telemetry(key: str, value: Any) -> dict[str, Any]:
    """Map a validated raw packet to the normalized schema consumed by later stages."""
    event = _parse_raw_event(value)
    normalized = NormalizedTelemetryEvent(
        event_id=event.event_id,
        timestamp=event.timestamp,
        customer_id=event.customer_id,
        truck_id=event.truck_id,
        route_id=event.route_id,
        temperature=event.temperature,
        target_temperature=event.target_temp,
        ambient_temperature=event.ambient_temp,
        compressor_status=event.compressor_status,
        door_open=event.door_open,
        battery_level=event.battery_level,
        latitude=event.latitude,
        longitude=event.longitude,
        speed_kmh=event.speed_kmh,
    )
    return normalized.model_dump(mode="json")


telemetry_stream = (
    raw_telemetry_topic.stream()
    .filter(is_valid_telemetry)
)


@app.agent(telemetry_stream)
async def consume_normalized_telemetry(stream):
    """Consume the filter/map output until a downstream sink is configured."""
    async for key, event in stream.items():
        normalized_event = normalize_telemetry(key, event)
        logger.debug("Normalized telemetry received for key=%s: %s", key, normalized_event)


__all__ = [
    "app",
    "raw_telemetry_topic",
    "processed_topic",
    "alerts_topic",
    "changelog_topic",
    "truck_state_table",
    "telemetry_stream",
    "is_valid_telemetry",
    "normalize_telemetry",
    "NormalizedTelemetryEvent",
]