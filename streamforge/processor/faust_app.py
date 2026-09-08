"""Faust application and Kafka topic bindings for StreamForge."""

from __future__ import annotations

import time
import logging
import json
from typing import Any, Mapping

import faust
from pydantic import ValidationError

from streamforge.common.config import settings
from streamforge.common.models import (
    AlertSeverity,
    AlertType,
    AnomalyAlert,
    CompressorStatus,
    NormalizedTelemetryEvent,
    ProcessedAggregate,
    RawTelemetryEvent,
)
from streamforge.processor.state_store import LocalStateStore

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

local_state_store = LocalStateStore(settings.ROCKSDB_STATE_DIR)

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


def calculate_window_bounds(
    timestamp: float,
    window_size: int = settings.WINDOW_SIZE_SECONDS,
) -> tuple[float, float]:
    """Calculate tumbling window start and end timestamps for a given event timestamp."""
    window_start = float((int(timestamp) // window_size) * window_size)
    window_end = window_start + float(window_size)
    return window_start, window_end


def create_initial_window_state(
    event: dict[str, Any] | NormalizedTelemetryEvent | RawTelemetryEvent,
    window_start: float,
    window_end: float,
) -> dict[str, Any]:
    """Initialize a new aggregation state dictionary for a tumbling window bucket."""
    if hasattr(event, "model_dump"):
        event_dict = event.model_dump(mode="json")
    elif isinstance(event, dict):
        event_dict = event
    else:
        event_dict = dict(event)

    customer_id = str(event_dict["customer_id"])
    truck_id = str(event_dict["truck_id"])
    temp = float(event_dict["temperature"])
    target_temp = float(event_dict.get("target_temperature", event_dict.get("target_temp", -18.0)))

    return {
        "customer_id": customer_id,
        "truck_id": truck_id,
        "window_start": window_start,
        "window_end": window_end,
        "sample_count": 1,
        "sum_temperature": temp,
        "min_temperature": temp,
        "max_temperature": temp,
        "target_temperature": target_temp,
    }


def update_window_state(
    state: dict[str, Any],
    event: dict[str, Any] | NormalizedTelemetryEvent | RawTelemetryEvent,
) -> dict[str, Any]:
    """Update an existing aggregation state dictionary with a new telemetry reading."""
    if hasattr(event, "model_dump"):
        event_dict = event.model_dump(mode="json")
    elif isinstance(event, dict):
        event_dict = event
    else:
        event_dict = dict(event)

    temp = float(event_dict["temperature"])
    target_temp = float(
        event_dict.get("target_temperature", event_dict.get("target_temp", state.get("target_temperature", -18.0)))
    )

    new_state = dict(state)
    new_state["sample_count"] = int(new_state["sample_count"]) + 1
    new_state["sum_temperature"] = float(new_state["sum_temperature"]) + temp
    new_state["min_temperature"] = min(float(new_state["min_temperature"]), temp)
    new_state["max_temperature"] = max(float(new_state["max_temperature"]), temp)
    new_state["target_temperature"] = target_temp
    return new_state


def _event_dict(event: dict[str, Any] | NormalizedTelemetryEvent | RawTelemetryEvent) -> dict[str, Any]:
    if hasattr(event, "model_dump"):
        return event.model_dump(mode="json")
    if isinstance(event, dict):
        return event
    return dict(event)


def _window_states(state: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not state:
        return {}
    stored_windows = state.get("windows")
    if isinstance(stored_windows, dict):
        return {str(key): dict(value) for key, value in stored_windows.items()}
    if "window_start" in state:
        return {str(float(state["window_start"])): dict(state)}
    return {}


def _state_for_window(
    windows: dict[str, dict[str, Any]],
    event: dict[str, Any],
    window_start: float,
    window_end: float,
) -> dict[str, Any]:
    window_key = str(window_start)
    current_window = windows.get(window_key)
    if current_window is None:
        current_window = create_initial_window_state(event, window_start, window_end)
    else:
        current_window = update_window_state(current_window, event)
    windows[window_key] = current_window
    return current_window


def build_processed_aggregate(state: dict[str, Any]) -> ProcessedAggregate:
    """Build a validated ProcessedAggregate Pydantic model from an aggregation state dictionary."""
    sample_count = int(state["sample_count"])
    sum_temp = float(state["sum_temperature"])
    avg_temp = round(sum_temp / sample_count, 2)
    is_breached = (avg_temp > settings.SAFE_TEMP_MAX) or (avg_temp < settings.SAFE_TEMP_MIN)

    return ProcessedAggregate(
        customer_id=str(state["customer_id"]),
        truck_id=str(state["truck_id"]),
        window_start=float(state["window_start"]),
        window_end=float(state["window_end"]),
        sample_count=sample_count,
        avg_temperature=avg_temp,
        min_temperature=round(float(state["min_temperature"]), 2),
        max_temperature=round(float(state["max_temperature"]), 2),
        target_temperature=round(float(state["target_temperature"]), 2),
        is_breached=is_breached,
    )


def process_telemetry_event(
    event: dict[str, Any] | NormalizedTelemetryEvent | RawTelemetryEvent,
    current_state: dict[str, Any] | None = None,
    window_size: int = settings.WINDOW_SIZE_SECONDS,
    grace_period: int = settings.GRACE_PERIOD_SECONDS,
) -> tuple[dict[str, Any], ProcessedAggregate]:
    """Aggregate an event-time window while accepting out-of-order events within the grace period."""
    event_dict = _event_dict(event)

    ts = float(event_dict["timestamp"])
    w_start, w_end = calculate_window_bounds(ts, window_size=window_size)
    windows = _window_states(current_state)
    previous_max_timestamp = float((current_state or {}).get("max_event_timestamp", ts))
    max_event_timestamp = max(previous_max_timestamp, ts)
    watermark = max_event_timestamp - float(grace_period)

    if ts < watermark:
        if not windows:
            fallback_window = dict(current_state or {})
            fallback_window.pop("windows", None)
            fallback_window.pop("max_event_timestamp", None)
            if not fallback_window:
                fallback_window = create_initial_window_state(event_dict, w_start, w_end)
            return {
                **fallback_window,
                "windows": windows,
                "max_event_timestamp": max_event_timestamp,
            }, build_processed_aggregate(fallback_window)

        latest_window = max(windows.values(), key=lambda value: float(value["window_start"]))
        return {
            **latest_window,
            "windows": windows,
            "max_event_timestamp": max_event_timestamp,
        }, build_processed_aggregate(latest_window)

    updated_window = _state_for_window(windows, event_dict, w_start, w_end)
    for window_key in list(windows):
        candidate = windows[window_key]
        if float(candidate["window_end"]) + float(grace_period) < watermark:
            del windows[window_key]

    latest_window = max(windows.values(), key=lambda value: float(value["window_start"]))
    updated_state = {
        **latest_window,
        "windows": windows,
        "max_event_timestamp": max_event_timestamp,
    }

    aggregate = build_processed_aggregate(updated_window)
    return updated_state, aggregate


telemetry_stream = (
    raw_telemetry_topic.stream()
    .filter(is_valid_telemetry)
)


@app.agent(telemetry_stream)
async def consume_normalized_telemetry(stream):
    """Consume filter/map telemetry events, aggregate into 5-minute tumbling windows grouped by (customer_id, truck_id),
    and emit ProcessedAggregate records."""
    async for key, event in stream.items():
        normalized_event = normalize_telemetry(key, event)
        logger.debug("Normalized telemetry received for key=%s: %s", key, normalized_event)

        cust_id = normalized_event["customer_id"]
        trk_id = normalized_event["truck_id"]
        group_key = f"{cust_id}:{trk_id}"

        current_state = local_state_store.current(group_key)
        try:
            if current_state is None and group_key in truck_state_table:
                win_val = truck_state_table[group_key]
                if hasattr(win_val, "current"):
                    current_state = win_val.current()
                elif isinstance(win_val, dict):
                    current_state = win_val
        except Exception:
            current_state = None

        updated_state, aggregate = process_telemetry_event(normalized_event, current_state)

        try:
            local_state_store[group_key] = updated_state
            truck_state_table[group_key] = updated_state
        except Exception as err:
            logger.warning("Could not update truck_state_table for key=%s: %s", group_key, err)

        await processed_topic.send(key=group_key, value=aggregate)

        temp = float(normalized_event["temperature"])
        comp_status = normalized_event.get("compressor_status")
        if comp_status == CompressorStatus.FAULT or temp > settings.SAFE_TEMP_MAX:
            severity = AlertSeverity.CRITICAL if comp_status == CompressorStatus.FAULT else AlertSeverity.WARNING
            alert_type = AlertType.COMPRESSOR_FAILURE if comp_status == CompressorStatus.FAULT else AlertType.HIGH_TEMPERATURE
            msg = (
                f"Compressor FAULT detected on vehicle {trk_id}"
                if comp_status == CompressorStatus.FAULT
                else f"High thermal excursion on vehicle {trk_id}: {temp}°C exceeds limit {settings.SAFE_TEMP_MAX}°C"
            )
            alert = AnomalyAlert(
                customer_id=cust_id,
                truck_id=trk_id,
                severity=severity,
                alert_type=alert_type,
                trigger_temperature=temp,
                target_temperature=float(normalized_event.get("target_temperature", -18.0)),
                threshold_limit=settings.SAFE_TEMP_MAX,
                compressor_status=comp_status or CompressorStatus.FAULT,
                timestamp=float(normalized_event.get("timestamp", time.time())),
                message=msg,
            )
            await alerts_topic.send(key=group_key, value=alert)


__all__ = [
    "app",
    "raw_telemetry_topic",
    "processed_topic",
    "alerts_topic",
    "changelog_topic",
    "truck_state_table",
    "local_state_store",
    "telemetry_stream",
    "is_valid_telemetry",
    "normalize_telemetry",
    "NormalizedTelemetryEvent",
    "calculate_window_bounds",
    "create_initial_window_state",
    "update_window_state",
    "build_processed_aggregate",
    "process_telemetry_event",
]
