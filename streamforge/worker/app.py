"""Backward-compatible Faust worker entry point."""

from streamforge.processor.faust_app import (
    alerts_topic,
    app,
    changelog_topic,
    processed_topic,
    raw_telemetry_topic,
    truck_state_table,
)

__all__ = [
    "app",
    "raw_telemetry_topic",
    "processed_topic",
    "alerts_topic",
    "changelog_topic",
    "truck_state_table",
]
