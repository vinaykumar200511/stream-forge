"""Throughput testing, bottleneck evaluation, and persisted anomaly history."""

import math
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from streamforge.common.config import settings


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile_value / 100
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


class PerformanceStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS throughput_tests (
                    id TEXT PRIMARY KEY, target_rate REAL NOT NULL, duration_seconds REAL NOT NULL,
                    actual_rate REAL NOT NULL, peak_rate REAL NOT NULL, average_latency_ms REAL NOT NULL,
                    p95_latency_ms REAL NOT NULL, p99_latency_ms REAL NOT NULL, error_count INTEGER NOT NULL,
                    failure_rate REAL NOT NULL, started_at TEXT NOT NULL, completed_at TEXT NOT NULL,
                    status TEXT NOT NULL, bottleneck_node TEXT
                );
                CREATE TABLE IF NOT EXISTS anomaly_alerts (
                    id TEXT PRIMARY KEY, alert_type TEXT NOT NULL, severity TEXT NOT NULL,
                    node_id TEXT NOT NULL, metric TEXT NOT NULL, current_value REAL NOT NULL,
                    threshold REAL NOT NULL, status TEXT NOT NULL, description TEXT NOT NULL,
                    triggered_at TEXT NOT NULL, acknowledged_at TEXT, resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_alerts_status ON anomaly_alerts(status);
                CREATE INDEX IF NOT EXISTS idx_alerts_node ON anomaly_alerts(node_id);
                CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON anomaly_alerts(triggered_at);
            """)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    def save_test(self, result: dict[str, Any]) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                "INSERT INTO throughput_tests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(result[key] for key in ("id", "targetRate", "durationSeconds", "actualRate", "peakRate", "averageLatencyMs", "p95LatencyMs", "p99LatencyMs", "errorCount", "failureRate", "startedAt", "completedAt", "status", "bottleneckNode")),
            )

    def tests(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM throughput_tests ORDER BY started_at DESC LIMIT ?", (limit,))]

    def save_alert(self, alert: dict[str, Any]) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                "INSERT INTO anomaly_alerts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(alert.get(key) for key in ("id", "alertType", "severity", "nodeId", "metric", "currentValue", "threshold", "status", "description", "triggeredAt", "acknowledgedAt", "resolvedAt")),
            )

    def alerts(self, active_only: bool = False, limit: int = 50) -> list[dict[str, Any]]:
        query = "SELECT * FROM anomaly_alerts"
        params: tuple[Any, ...] = ()
        if active_only:
            query += " WHERE status IN ('TRIGGERED', 'ACKNOWLEDGED')"
        query += " ORDER BY triggered_at DESC LIMIT ?"
        params += (limit,)
        with self._connect() as connection:
            return [dict(row) for row in connection.execute(query, params)]

    def update_alert(self, alert_id: str, status: str) -> bool:
        column = "acknowledged_at" if status == "ACKNOWLEDGED" else "resolved_at"
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                f"UPDATE anomaly_alerts SET status = ?, {column} = ? WHERE id = ? AND status != 'RESOLVED'",
                (status, utc_now(), alert_id),
            )
            return cursor.rowcount == 1


class PerformanceService:
    def __init__(self) -> None:
        self.store = PerformanceStore(settings.OBSERVABILITY_DB_PATH)
        self._latest: dict[str, Any] | None = None
        self._active_keys: set[tuple[str, str]] = set()
        self._violation_counts: dict[tuple[str, str], int] = {}

    def run_test(self, duration_seconds: float, target_rate: int, payload_size: int = 256, workers: int = 1) -> dict[str, Any]:
        duration_seconds = min(max(duration_seconds, 0.01), 30.0)
        target_rate = min(max(target_rate, 1), 100_000)
        payload = b"x" * min(max(payload_size, 1), 1_048_576)
        started_at = utc_now()
        latencies: list[float] = []
        errors = 0
        processed = 0
        deadline = time.perf_counter() + duration_seconds
        interval = 1.0 / target_rate
        next_due = time.perf_counter()
        while time.perf_counter() < deadline:
            now = time.perf_counter()
            if now < next_due:
                time.sleep(min(next_due - now, 0.001))
                continue
            operation_start = time.perf_counter()
            try:
                _ = len(payload)
                processed += 1
            except Exception:
                errors += 1
            latencies.append((time.perf_counter() - operation_start) * 1000)
            next_due += interval
        elapsed = max(time.perf_counter() - (deadline - duration_seconds), 0.001)
        actual_rate = processed / elapsed
        result = {
            "id": str(uuid.uuid4()), "targetRate": target_rate, "durationSeconds": round(elapsed, 3),
            "actualRate": round(actual_rate, 2), "peakRate": round(actual_rate, 2),
            "averageLatencyMs": round(sum(latencies) / len(latencies), 3) if latencies else 0,
            "p95LatencyMs": round(percentile(latencies, 95), 3), "p99LatencyMs": round(percentile(latencies, 99), 3),
            "errorCount": errors, "failureRate": round(errors / max(processed + errors, 1), 6),
            "startedAt": started_at, "completedAt": utc_now(), "status": "COMPLETED", "bottleneckNode": None,
            "workers": workers,
        }
        bottleneck = self.evaluate(result)
        result["bottleneckNode"] = bottleneck["nodeId"] if bottleneck else None
        self.store.save_test(result)
        self._latest = result
        return result

    def evaluate(self, result: dict[str, Any]) -> dict[str, Any] | None:
        violations = []
        if result["p95LatencyMs"] > settings.BOTTLENECK_P95_LATENCY_MS:
            violations.append(("LATENCY_SPIKE", result["p95LatencyMs"], settings.BOTTLENECK_P95_LATENCY_MS))
        if result["failureRate"] > settings.BOTTLENECK_FAILURE_RATE:
            violations.append(("ERROR_RATE_SPIKE", result["failureRate"], settings.BOTTLENECK_FAILURE_RATE))
        if result["actualRate"] < result["targetRate"] * settings.BOTTLENECK_MIN_THROUGHPUT_RATIO:
            violations.append(("THROUGHPUT_DROP", result["actualRate"], result["targetRate"] * settings.BOTTLENECK_MIN_THROUGHPUT_RATIO))
        if not violations:
            for alert in self.store.alerts(active_only=True):
                self.store.update_alert(alert["id"], "RESOLVED")
            self._active_keys.clear()
            self._violation_counts.clear()
            return None
        alert_type, current, threshold = violations[0]
        key = ("processor", alert_type)
        self._violation_counts[key] = self._violation_counts.get(key, 0) + 1
        if self._violation_counts[key] < settings.BOTTLENECK_CONSECUTIVE_VIOLATIONS:
            return None
        if key in self._active_keys:
            return None
        self._active_keys.add(key)
        severity = "CRITICAL" if len(violations) > 1 else "HIGH"
        alert = {
            "id": str(uuid.uuid4()), "alertType": alert_type, "severity": severity, "nodeId": "processor",
            "metric": alert_type.lower(), "currentValue": round(current, 4), "threshold": round(threshold, 4),
            "status": "TRIGGERED", "description": f"{alert_type.replace('_', ' ').title()} detected during throughput test.",
            "triggeredAt": utc_now(), "acknowledgedAt": None, "resolvedAt": None,
        }
        self.store.save_alert(alert)
        return alert

    def node_status(self) -> list[dict[str, Any]]:
        latest = self._latest
        if not latest:
            return []
        alert = self.store.alerts(active_only=True, limit=1)
        status = "BOTTLENECK" if alert else "HEALTHY"
        return [{"nodeId": "processor", "status": status, "severity": alert[0]["severity"] if alert else "INFO", "throughput": latest["actualRate"], "latency": latest["averageLatencyMs"], "p95Latency": latest["p95LatencyMs"], "errorRate": latest["failureRate"], "timestamp": latest["completedAt"]}]

    def transition_alert(self, alert_id: str, status: str) -> bool:
        alert = next((item for item in self.store.alerts() if item["id"] == alert_id), None)
        updated = self.store.update_alert(alert_id, status)
        if updated and status == "RESOLVED" and alert:
            self._active_keys.discard((alert["node_id"], alert["alert_type"]))
        return updated

    def overview(self) -> dict[str, Any]:
        latest = self._latest
        return {"throughputTests": self.store.tests(), "alerts": self.store.alerts(), "activeAlerts": self.store.alerts(active_only=True), "nodeStatuses": self.node_status(), "latestTest": latest}


performance_service = PerformanceService()