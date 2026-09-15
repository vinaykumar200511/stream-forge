"""
Prometheus Metrics Exporter for StreamForge FleetPulse Analytics.
Exposes operational & stream processing metrics for Prometheus scraping.
"""

from typing import Tuple
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    generate_latest,
    REGISTRY,
)

# Shared metrics registry
registry = REGISTRY

# Core Observability Metrics
TELEMETRY_EVENTS_TOTAL = Counter(
    "streamforge_telemetry_events_total",
    "Total number of raw IoT telemetry events ingested",
    ["customer_id"],
    registry=registry,
)

ANOMALY_ALERTS_TOTAL = Counter(
    "streamforge_anomaly_alerts_total",
    "Total number of cold-chain thermal anomaly alerts triggered",
    ["severity", "alert_type"],
    registry=registry,
)

ACTIVE_TRUCKS_GAUGE = Gauge(
    "streamforge_active_trucks",
    "Number of active cold-chain vehicles in fleet",
    registry=registry,
)

PROCESSING_LATENCY_SECONDS = Histogram(
    "streamforge_processing_latency_seconds",
    "Time spent aggregating events in tumbling windows (in seconds)",
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
    registry=registry,
)


def get_prometheus_metrics() -> Tuple[bytes, str]:
    """Generate latest Prometheus metric export in plain text format."""
    return generate_latest(registry), CONTENT_TYPE_LATEST
