import pytest
from pydantic import ValidationError
from streamforge.common.models import (
    AlertSeverity,
    AlertType,
    AnomalyAlert,
    CompressorStatus,
    ProcessedAggregate,
    RawTelemetryEvent,
)


def test_raw_telemetry_event_creation():
    event = RawTelemetryEvent(
        customer_id="cust_01",
        truck_id="trk_01_0001",
        route_id="route_101",
        temperature=-18.4,
        target_temp=-18.0,
        ambient_temp=25.0,
        compressor_status=CompressorStatus.RUNNING,
        door_open=False,
        battery_level=97.5,
        latitude=37.7749,
        longitude=-122.4194,
        speed_kmh=72.0,
    )

    assert event.customer_id == "cust_01"
    assert event.truck_id == "trk_01_0001"
    assert event.kafka_key == "cust_01:trk_01_0001"
    assert event.temperature == -18.4
    assert event.compressor_status == CompressorStatus.RUNNING


def test_raw_telemetry_json_serialization():
    event = RawTelemetryEvent(
        customer_id="cust_02",
        truck_id="trk_02_0055",
        temperature=-19.2,
    )
    json_data = event.model_dump_json()
    assert "cust_02" in json_data
    assert "trk_02_0055" in json_data
    assert "temperature" in json_data

    # Deserialization test
    parsed = RawTelemetryEvent.model_validate_json(json_data)
    assert parsed.customer_id == "cust_02"
    assert parsed.truck_id == "trk_02_0055"
    assert parsed.temperature == -19.2


def test_model_validations():
    with pytest.raises(ValidationError):
        # Empty customer_id should fail
        RawTelemetryEvent(customer_id="", truck_id="trk_1", temperature=-10.0)

    with pytest.raises(ValidationError):
        # Out of bounds battery level
        RawTelemetryEvent(customer_id="c1", truck_id="t1", temperature=-10.0, battery_level=120.0)


def test_processed_aggregate_model():
    agg = ProcessedAggregate(
        customer_id="cust_01",
        truck_id="trk_01_0001",
        window_start=1700000000.0,
        window_end=1700000300.0,
        sample_count=30,
        avg_temperature=-18.2,
        min_temperature=-19.0,
        max_temperature=-17.5,
        target_temperature=-18.0,
        is_breached=False,
    )
    assert agg.kafka_key == "cust_01:trk_01_0001"
    assert agg.sample_count == 30
    assert not agg.is_breached


def test_anomaly_alert_model():
    alert = AnomalyAlert(
        customer_id="cust_01",
        truck_id="trk_01_0001",
        severity=AlertSeverity.CRITICAL,
        alert_type=AlertType.HIGH_TEMPERATURE,
        trigger_temperature=-5.0,
        target_temperature=-18.0,
        threshold_limit=-10.0,
        compressor_status=CompressorStatus.FAULT,
        message="Critical high temperature excursion detected: -5.0°C exceeds threshold -10.0°C",
    )
    assert alert.kafka_key == "cust_01:trk_01_0001"
    assert alert.severity == AlertSeverity.CRITICAL
    assert alert.alert_type == AlertType.HIGH_TEMPERATURE
