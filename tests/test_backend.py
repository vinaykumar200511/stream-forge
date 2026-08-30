"""Unit tests for FastAPI backend endpoints (/health, /metrics, root)."""

from fastapi.testclient import TestClient
from streamforge.backend.app import app

client = TestClient(app)


def test_root_endpoint():
    """Verify GET / returns 200 OK with correct service metadata."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "streamforge-backend"
    assert data["version"] == "1.0.0"
    assert "endpoints" in data


def test_health_check_endpoint():
    """Verify GET /health returns 200 OK with status ok and uptime."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == "streamforge-backend"
    assert "uptime_seconds" in data
    assert "timestamp" in data


def test_prometheus_metrics_endpoint():
    """Verify GET /metrics returns 200 OK with Prometheus formatted plain text."""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    content = response.text
    # Verify core metric names exist in the Prometheus export
    assert "streamforge_telemetry_events_total" in content
    assert "streamforge_anomaly_alerts_total" in content
    assert "streamforge_active_trucks" in content
