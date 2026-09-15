import json
from unittest.mock import MagicMock, patch

import pytest

from streamforge.common.models import RawTelemetryEvent, CompressorStatus


@patch("streamforge.backend.consumer.consumer.Consumer")
def test_raw_consumer_smoke_reads_message(mock_consumer_cls):
    """Raw Kafka consumer should subscribe to the configured topic and log a parsed payload."""
    mock_consumer = MagicMock()
    mock_consumer_cls.return_value = mock_consumer

    mock_message = MagicMock()
    mock_message.error.return_value = None
    mock_message.key.return_value = b"cust_01:trk_01_0001"
    mock_message.value.return_value = json.dumps({
        "customer_id": "cust_01",
        "truck_id": "trk_01_0001",
        "temperature": -18.5,
        "target_temp": -18.0,
        "timestamp": "2026-09-01T00:00:00Z",
    }).encode("utf-8")
    mock_consumer.poll.return_value = mock_message

    from streamforge.backend.consumer.consumer import RawKafkaConsumer

    consumer = RawKafkaConsumer(bootstrap_servers="localhost:9092", topic="raw-telemetry")
    payload = consumer.consume_once(timeout=0.1)

    assert payload["customer_id"] == "cust_01"
    assert payload["truck_id"] == "trk_01_0001"
    assert payload["temperature"] == -18.5
    mock_consumer.subscribe.assert_called_once_with(["raw-telemetry"])
    assert mock_consumer.poll.call_count == 1


@patch("streamforge.backend.consumer.consumer.Consumer")
def test_run_smoke_test_returns_message_count(mock_consumer_cls):
    """The smoke helper should consume a bounded number of messages and report success."""
    mock_consumer = MagicMock()
    mock_consumer_cls.return_value = mock_consumer

    payload = {
        "customer_id": "cust_02",
        "truck_id": "trk_02_0002",
        "temperature": -16.0,
        "target_temp": -18.0,
        "timestamp": "2026-09-01T00:00:05Z",
    }
    mock_consumer.poll.side_effect = [
        MagicMock(error=lambda: None, key=lambda: b"cust_02:trk_02_0002", value=lambda: json.dumps(payload).encode("utf-8")),
        None,
    ]

    from streamforge.backend.consumer.consumer import run_smoke_test

    count = run_smoke_test(bootstrap_servers="localhost:9092", topic="raw-telemetry", max_messages=1)

    assert count == 1
