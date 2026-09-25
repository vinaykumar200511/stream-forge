from pathlib import Path

from fastapi.testclient import TestClient

from streamforge.backend.app import app
from streamforge.backend.performance import PerformanceService, percentile


def test_percentile_interpolates_sorted_latency_samples():
    assert percentile([1, 2, 3, 4], 50) == 2.5
    assert percentile([], 99) == 0.0


def test_throughput_test_measures_and_persists_result(tmp_path: Path):
    service = PerformanceService()
    service.store = service.store.__class__(str(tmp_path / "observability.db"))
    result = service.run_test(0.01, 1000, 32, 1)
    assert result["status"] == "COMPLETED"
    assert result["actualRate"] > 0
    assert result["p95LatencyMs"] >= 0
    assert service.store.tests(1)[0]["id"] == result["id"]


def test_throughput_api_validates_and_returns_results():
    client = TestClient(app)
    response = client.post("/api/throughput-tests", json={"duration_seconds": 0.01, "target_rate": 1000})
    assert response.status_code == 200
    result = response.json()
    assert result["status"] == "COMPLETED"
    history = client.get("/api/throughput-tests")
    assert history.status_code == 200
    assert any(item["id"] == result["id"] for item in history.json()["items"])


def test_anomaly_history_api_is_available():
    client = TestClient(app)
    response = client.get("/api/anomalies", params={"active": "true"})
    assert response.status_code == 200
    assert isinstance(response.json()["items"], list)


def test_bottleneck_evaluation_persists_one_deduplicated_alert(tmp_path: Path):
    service = PerformanceService()
    service.store = service.store.__class__(str(tmp_path / "alerts.db"))
    result = {
        "id": "test-run",
        "targetRate": 1000,
        "actualRate": 400,
        "failureRate": 0.0,
        "p95LatencyMs": 10.0,
    }
    assert service.evaluate(result) is None
    first = service.evaluate(result)
    second = service.evaluate(result)
    assert first is not None
    assert first["alertType"] == "THROUGHPUT_DROP"
    assert second is None
    assert len(service.store.alerts(active_only=True)) == 1


def test_resolving_bottleneck_alert_clears_active_state(tmp_path: Path):
    service = PerformanceService()
    service.store = service.store.__class__(str(tmp_path / "alerts.db"))
    alert = {
        "id": "alert-1",
        "alertType": "THROUGHPUT_DROP",
        "severity": "HIGH",
        "nodeId": "processor",
        "metric": "throughput_drop",
        "currentValue": 400.0,
        "threshold": 700.0,
        "status": "TRIGGERED",
        "description": "Throughput drop detected.",
        "triggeredAt": "2026-09-25T00:00:00Z",
        "acknowledgedAt": None,
        "resolvedAt": None,
    }
    service.store.save_alert(alert)
    service._active_keys.add(("processor", "THROUGHPUT_DROP"))

    assert service.transition_alert("alert-1", "RESOLVED") is True
    assert ("processor", "THROUGHPUT_DROP") not in service._active_keys