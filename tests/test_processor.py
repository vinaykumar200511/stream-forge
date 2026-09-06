import json

from streamforge.common.models import RawTelemetryEvent
from streamforge.processor.faust_app import (
    build_processed_aggregate,
    calculate_window_bounds,
    is_valid_telemetry,
    normalize_telemetry,
    process_telemetry_event,
)


def test_filter_accepts_positive_temperature_event():
    event = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=2.5)

    assert is_valid_telemetry(event.kafka_key, event.model_dump_json().encode())


def test_filter_drops_zero_and_malformed_events():
    zero_event = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=0.0)

    assert not is_valid_telemetry(zero_event.kafka_key, zero_event.model_dump_json().encode())
    assert not is_valid_telemetry("cust_01:truck_01", b"not-json")


def test_filter_drops_negative_temperature_events():
    negative_event = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=-5.0)

    assert not is_valid_telemetry(negative_event.kafka_key, negative_event.model_dump_json().encode())


def test_filter_drops_various_malformed_payloads():
    assert not is_valid_telemetry("key", None)
    assert not is_valid_telemetry("key", 12345)
    assert not is_valid_telemetry("key", b"{invalid json}")
    assert not is_valid_telemetry("key", json.dumps({"missing": "fields"}).encode())
    assert not is_valid_telemetry("key", json.dumps({"customer_id": "", "truck_id": "t1", "temperature": 5.0}))


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
    assert normalized["temperature"] == 2.5
    assert normalized["target_temperature"] == -18.0
    assert normalized["ambient_temperature"] == 21.0
    assert "target_temp" not in normalized
    assert "ambient_temp" not in normalized
    assert normalized["compressor_status"] == "RUNNING"
    assert "event_id" in normalized
    assert "timestamp" in normalized


def test_map_accepts_various_input_formats():
    event = RawTelemetryEvent(customer_id="cust_02", truck_id="truck_02", temperature=5.0)

    # RawTelemetryEvent instance
    res1 = normalize_telemetry("key", event)
    assert res1["truck_id"] == "truck_02"

    # Bytes
    res2 = normalize_telemetry("key", event.model_dump_json().encode())
    assert res2["truck_id"] == "truck_02"

    # Mapping / Dict
    res3 = normalize_telemetry("key", event.model_dump(mode="json"))
    assert res3["truck_id"] == "truck_02"


def test_calculate_5min_tumbling_window_bounds():
    ts = 1787725812.5
    w_start, w_end = calculate_window_bounds(ts, window_size=300)
    assert w_start == 1787725800.0
    assert w_end == 1787726100.0
    assert w_end - w_start == 300.0


def test_tumbling_window_aggregation_single_truck():
    base_ts = 1787725800.0
    e1 = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=-18.0, timestamp=base_ts + 10)
    e2 = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=-12.0, timestamp=base_ts + 60)
    e3 = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=-15.0, timestamp=base_ts + 120)

    n1 = normalize_telemetry(e1.kafka_key, e1)
    n2 = normalize_telemetry(e2.kafka_key, e2)
    n3 = normalize_telemetry(e3.kafka_key, e3)

    state1, agg1 = process_telemetry_event(n1, None)
    assert agg1.sample_count == 1
    assert agg1.avg_temperature == -18.0
    assert agg1.min_temperature == -18.0
    assert agg1.max_temperature == -18.0

    state2, agg2 = process_telemetry_event(n2, state1)
    assert agg2.sample_count == 2
    assert agg2.avg_temperature == -15.0  # (-18 + -12) / 2
    assert agg2.min_temperature == -18.0
    assert agg2.max_temperature == -12.0

    state3, agg3 = process_telemetry_event(n3, state2)
    assert agg3.sample_count == 3
    assert agg3.avg_temperature == -15.0  # (-18 + -12 + -15) / 3
    assert agg3.min_temperature == -18.0
    assert agg3.max_temperature == -12.0
    assert not agg3.is_breached


def test_tumbling_window_grouping_by_customer_and_truck():
    base_ts = 1787725800.0
    e_c1_t1 = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=-20.0, timestamp=base_ts + 5)
    e_c1_t2 = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_02", temperature=-14.0, timestamp=base_ts + 10)
    e_c2_t1 = RawTelemetryEvent(customer_id="cust_02", truck_id="truck_01", temperature=-16.0, timestamp=base_ts + 15)

    n_c1_t1 = normalize_telemetry(e_c1_t1.kafka_key, e_c1_t1)
    n_c1_t2 = normalize_telemetry(e_c1_t2.kafka_key, e_c1_t2)
    n_c2_t1 = normalize_telemetry(e_c2_t1.kafka_key, e_c2_t1)

    state_c1_t1, agg_c1_t1 = process_telemetry_event(n_c1_t1, None)
    state_c1_t2, agg_c1_t2 = process_telemetry_event(n_c1_t2, None)
    state_c2_t1, agg_c2_t1 = process_telemetry_event(n_c2_t1, None)

    assert (agg_c1_t1.customer_id, agg_c1_t1.truck_id) == ("cust_01", "truck_01")
    assert (agg_c1_t2.customer_id, agg_c1_t2.truck_id) == ("cust_01", "truck_02")
    assert (agg_c2_t1.customer_id, agg_c2_t1.truck_id) == ("cust_02", "truck_01")

    assert agg_c1_t1.avg_temperature == -20.0
    assert agg_c1_t2.avg_temperature == -14.0
    assert agg_c2_t1.avg_temperature == -16.0


def test_tumbling_window_transition_after_5_minutes():
    base_ts = 1787725800.0
    e_win1 = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=-18.0, timestamp=base_ts + 100)
    e_win2 = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=-22.0, timestamp=base_ts + 350)

    n_win1 = normalize_telemetry(e_win1.kafka_key, e_win1)
    n_win2 = normalize_telemetry(e_win2.kafka_key, e_win2)

    state1, agg1 = process_telemetry_event(n_win1, None)
    assert agg1.window_start == 1787725800.0
    assert agg1.sample_count == 1

    # Transition to next window (past 300s)
    state2, agg2 = process_telemetry_event(n_win2, state1)
    assert agg2.window_start == 1787726100.0
    assert agg2.sample_count == 1
    assert agg2.avg_temperature == -22.0


def test_tumbling_window_breach_flag():
    base_ts = 1787725800.0
    e_breached = RawTelemetryEvent(customer_id="cust_01", truck_id="truck_01", temperature=5.0, timestamp=base_ts + 10)
    n_breached = normalize_telemetry(e_breached.kafka_key, e_breached)

    _, agg = process_telemetry_event(n_breached, None)
    assert agg.is_breached


