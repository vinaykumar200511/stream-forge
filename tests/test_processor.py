import json

from streamforge.common.models import RawTelemetryEvent
from streamforge.processor.faust_app import is_valid_telemetry, normalize_telemetry


def test_filter_accepts_positive_temperature_event():
    event = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=2.5)

    assert is_valid_telemetry(event.kafka_key, event.model_dump_json().encode())


def test_filter_drops_zero_and_malformed_events():
    zero_event = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=0.0)

    assert not is_valid_telemetry(zero_event.kafka_key, zero_event.model_dump_json().encode())
    assert not is_valid_telemetry("cust_01:truck_01", b"not-json")


def test_map_normalizes_telemetry_schema():
    event = RawTelemetryEvent(
        customer_id="cust_01",
        truck_id="truck_01",
        temperature=2.5,
        target_temp=-18.0,
        ambient_temp=21.0,
    )

    normalized = normalize_telemetry(event.kafka_key, json.dumps(event.model_dump(mode="json")))

    assert normalized["customer_id"] == "cust_01"
    assert normalized["truck_id"] == "truck_01"
    assert normalized["target_temperature"] == -18.0
    assert normalized["ambient_temperature"] == 21.0
    assert "target_temp" not in normalized
