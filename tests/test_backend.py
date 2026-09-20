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
    assert "streamforge_events_per_second" in content


def test_topology_endpoint():
    """Verify GET /topology returns live DAG metadata for the React Flow view."""
    response = client.get("/topology")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "nodes" in data and "edges" in data
    assert len(data["nodes"]) >= 4
    assert any(node["id"] == "producer" for node in data["nodes"])
    assert any(edge["source"] == "producer" and edge["target"] == "kafka" for edge in data["edges"])


def test_routes_endpoint():
    """Verify GET /routes returns movement data for the GPS route panel."""
    response = client.get("/routes")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "vehicles" in data
    assert len(data["vehicles"]) >= 9
    assert data["vehicles"][-6]["name"] == "Truck 1"
    assert all("route" in vehicle for vehicle in data["vehicles"])
    assert all({"telemetry_date", "route_name", "truck_type"} <= vehicle.keys() for vehicle in data["vehicles"])


def test_dashboard_metrics_endpoint():
    """Verify dashboard KPIs come from the backend metrics contract."""
    response = client.get("/dashboard/metrics")
    assert response.status_code == 200
    assert response.json()["metrics"] == {
        "activeTrucks": 9,
        "eventsPerSecond": 13400,
        "processingLagMs": 42,
        "activeWorkers": 2,
        "healthyWorkers": 2,
        "kafkaStatus": "Healthy",
        "temperatureAlerts": 12,
        "fleetUptime": 99.2,
    }


def test_vehicle_history_endpoint():
    """Verify plate lookup returns owner, driver, and recent trip history."""
    response = client.get("/vehicles/history", params={"plate": "nyc-204"})
    assert response.status_code == 200
    record = response.json()["record"]
    assert record["owner"] == "Northstar Cold Logistics"
    assert record["driver"] == "Maya Patel"
    assert len(record["history"]) == 3


def test_vehicle_history_endpoint_has_swagger_default():
    """Verify Swagger can execute the lookup without manually entering a plate."""
    response = client.get("/vehicles/history")
    assert response.status_code == 200
    assert response.json()["record"]["plate"] == "NYC-204"


def test_dashboard_vehicle_history_alias():
    """Verify the dashboard-prefixed route remains available as an alias."""
    response = client.get("/dashboard/vehicles/history", params={"plate": "NYC-118"})
    assert response.status_code == 200
    assert response.json()["record"]["driver"] == "Jordan Brooks"


def test_routes_endpoint_filters_by_all_dimensions():
    """Verify route filtering accepts truck ID, date, route, and truck type."""
    response = client.get(
        "/routes",
        params={
            "truck_id": "truck-204",
            "date": "2026-09-19",
            "route": "Hudson Cold Chain",
            "truck_type": "Refrigerated",
        },
    )
    assert response.status_code == 200
    vehicles = response.json()["vehicles"]
    assert len(vehicles) == 1
    assert vehicles[0]["id"] == "truck-204"
