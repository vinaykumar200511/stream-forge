from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from streamforge.backend.app import app
from streamforge.backend.operations import TemperatureAlertEvaluator


def test_temperature_alert_waits_for_duration_and_deduplicates():
    evaluator = TemperatureAlertEvaluator(threshold=-10.0, duration_seconds=30)
    started = datetime.now(timezone.utc) - timedelta(seconds=31)
    timestamp = started.isoformat().replace("+00:00", "Z")

    alert = evaluator.evaluate("truck-1", -8.0, timestamp=timestamp)
    duplicate = evaluator.evaluate("truck-1", -7.5, timestamp=timestamp)

    assert alert is not None
    assert duplicate is not None
    assert len(evaluator.active_alerts()) == 1
    assert duplicate["temperature"] == -7.5


def test_temperature_alert_resolves_when_reading_returns_to_threshold():
    evaluator = TemperatureAlertEvaluator(threshold=-10.0, duration_seconds=0)
    assert evaluator.evaluate("truck-1", -8.0) is not None
    assert evaluator.evaluate("truck-1", -10.0) is None
    assert evaluator.active_alerts() == []


def test_operations_endpoints_expose_truthful_empty_states():
    client = TestClient(app)
    response = client.get("/api/operations/overview")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["failures"]["active"] == 0
    assert data["temperature"]["status"] == "unavailable"
    assert data["loads"]["status"] == "unavailable"
    assert data["deliveries"]["status"] == "unavailable"


def test_temperature_alerts_endpoint_exposes_configured_threshold():
    client = TestClient(app)
    response = client.get("/api/alerts/temperature")
    assert response.status_code == 200
    assert response.json()["threshold"] == -10.0