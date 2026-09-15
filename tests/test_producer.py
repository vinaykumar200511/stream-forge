import json
import time
from unittest.mock import MagicMock, patch
import pytest

from streamforge.common.models import RawTelemetryEvent, CompressorStatus
from streamforge.producer.producer import StreamForgeProducer, check_broker_reachable, run_dry_run
from streamforge.producer.simulator import TelemetrySimulator

# Shared patch target for broker probe used across all live-producer tests
_PROBE_PATCH = "streamforge.producer.producer.check_broker_reachable"


def test_producer_initialization():
    producer = StreamForgeProducer(
        bootstrap_servers="localhost:9092",
        topic="test-raw-telemetry",
        rate_per_sec=250,
    )
    assert producer.bootstrap_servers == "localhost:9092"
    assert producer.topic == "test-raw-telemetry"
    assert producer.rate_per_sec == 250
    assert producer.total_published == 0
    assert producer.total_delivered == 0
    assert producer.total_errors == 0


@patch("streamforge.producer.producer.Producer")
@patch(_PROBE_PATCH, return_value=True)
def test_producer_start_and_send_event(mock_probe, mock_kafka_producer_cls):
    mock_inner_producer = MagicMock()
    mock_kafka_producer_cls.return_value = mock_inner_producer

    producer = StreamForgeProducer(
        bootstrap_servers="localhost:9092",
        topic="raw-telemetry",
    )
    producer.start()
    assert producer.producer is not None
    mock_probe.assert_called_once()

    event = RawTelemetryEvent(
        customer_id="cust_01",
        truck_id="trk_01_0042",
        temperature=-18.5,
        target_temp=-18.0,
        compressor_status=CompressorStatus.RUNNING,
    )

    producer.send_event(event)
    assert producer.total_published == 1

    # Verify produce was called with correct key, value bytes, and topic
    mock_inner_producer.produce.assert_called_once()
    call_kwargs = mock_inner_producer.produce.call_args[1]
    assert call_kwargs["topic"] == "raw-telemetry"
    assert call_kwargs["key"] == b"cust_01:trk_01_0042"

    payload = json.loads(call_kwargs["value"].decode("utf-8"))
    assert payload["customer_id"] == "cust_01"
    assert payload["truck_id"] == "trk_01_0042"
    assert payload["temperature"] == -18.5

    mock_inner_producer.poll.assert_called_with(0)


@patch("streamforge.producer.producer.Producer")
@patch(_PROBE_PATCH, return_value=True)
def test_producer_delivery_callback(mock_probe, mock_kafka_producer_cls):
    mock_inner_producer = MagicMock()
    mock_kafka_producer_cls.return_value = mock_inner_producer

    producer = StreamForgeProducer()
    producer.start()

    # Test success delivery
    mock_msg = MagicMock()
    mock_msg.key.return_value = b"cust_01:trk_01_0001"
    producer._delivery_callback(None, mock_msg)
    assert producer.total_delivered == 1
    assert producer.total_errors == 0

    # Test error delivery
    mock_err = MagicMock()
    producer._delivery_callback(mock_err, mock_msg)
    assert producer.total_delivered == 1
    assert producer.total_errors == 1


@patch("streamforge.producer.producer.Producer")
@patch(_PROBE_PATCH, return_value=True)
def test_producer_rate_control_and_streaming(mock_probe, mock_kafka_producer_cls):
    mock_inner_producer = MagicMock()
    mock_kafka_producer_cls.return_value = mock_inner_producer

    simulator = TelemetrySimulator(num_customers=2, num_trucks=10)
    producer = StreamForgeProducer(
        simulator=simulator,
        rate_per_sec=500,
    )
    producer.start()

    # Stream 25 events
    producer.run_stream(max_events=25)
    assert producer.total_published == 25
    assert mock_inner_producer.produce.call_count == 25

    # Test dynamic rate change
    producer.set_rate(1000)
    assert producer.rate_per_sec == 1000


@patch("streamforge.producer.producer.Producer")
@patch(_PROBE_PATCH, return_value=True)
def test_producer_duration_bounded_streaming(mock_probe, mock_kafka_producer_cls):
    mock_inner_producer = MagicMock()
    mock_kafka_producer_cls.return_value = mock_inner_producer

    simulator = TelemetrySimulator(num_customers=2, num_trucks=10)
    producer = StreamForgeProducer(
        simulator=simulator,
        rate_per_sec=100,
    )
    producer.start()

    start = time.time()
    producer.run_stream(duration_seconds=0.15)
    elapsed = time.time() - start

    assert producer.total_published > 0
    assert elapsed >= 0.12


@patch("streamforge.producer.producer.Producer")
@patch(_PROBE_PATCH, return_value=True)
def test_producer_flush_and_stop(mock_probe, mock_kafka_producer_cls):
    mock_inner_producer = MagicMock()
    mock_inner_producer.__len__.return_value = 0
    mock_inner_producer.flush.return_value = 0
    mock_kafka_producer_cls.return_value = mock_inner_producer

    producer = StreamForgeProducer()
    producer.start()
    producer.stop(timeout=5.0)

    mock_inner_producer.flush.assert_called_once_with(timeout=5.0)
    assert producer.producer is None
    assert not producer.is_running


def test_dry_run_execution():
    simulator = TelemetrySimulator(num_customers=2, num_trucks=4)
    # Should execute cleanly without raising errors
    run_dry_run(simulator=simulator, count=5, rate_per_sec=100)


@patch(_PROBE_PATCH, return_value=False)
def test_start_raises_when_broker_unreachable(mock_probe):
    """producer.start() must raise RuntimeError immediately if broker probe fails."""
    producer = StreamForgeProducer(bootstrap_servers="localhost:9092")
    with pytest.raises(RuntimeError, match="UNREACHABLE"):
        producer.start()
    assert producer.producer is None  # never created


@patch("streamforge.producer.producer.Producer")
@patch(_PROBE_PATCH, return_value=True)
def test_circuit_breaker_trips_on_high_error_rate(mock_probe, mock_kafka_producer_cls):
    """Circuit-breaker must abort run_stream when >50% deliveries fail."""
    mock_inner = MagicMock()
    mock_kafka_producer_cls.return_value = mock_inner

    simulator = TelemetrySimulator(num_customers=1, num_trucks=4)
    producer = StreamForgeProducer(simulator=simulator, rate_per_sec=1000)
    producer.start()

    # Simulate 100% failure rate by injecting errors directly
    producer.total_published = 100
    producer.total_errors = 60  # 60% error rate — above 50% threshold

    assert producer._check_circuit_breaker() is True


@patch("streamforge.producer.producer.Producer")
@patch(_PROBE_PATCH, return_value=True)
def test_delivery_error_throttling(mock_probe, mock_kafka_producer_cls):
    """Delivery errors beyond MAX_ERROR_LOGS_PER_WINDOW must be suppressed."""
    mock_inner = MagicMock()
    mock_kafka_producer_cls.return_value = mock_inner

    producer = StreamForgeProducer()
    producer.start()

    mock_err = MagicMock()
    mock_msg = MagicMock()
    mock_msg.key.return_value = b"cust_01:trk_01_0001"

    # Fire 10 delivery errors
    for _ in range(10):
        producer._delivery_callback(mock_err, mock_msg)

    assert producer.total_errors == 10
    # Log counter should be capped at MAX+1 (one extra for the suppression notice)
    assert producer._error_logs_this_window == StreamForgeProducer.MAX_ERROR_LOGS_PER_WINDOW + 1


