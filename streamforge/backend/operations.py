"""Operational domain services shared by the dashboard APIs."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from streamforge.common.config import settings


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class TemperatureAlert:
    device_id: str
    temperature: float
    threshold: float
    started_at: str
    status: str = "TRIGGERED"
    location: dict[str, float] | None = None

    def as_dict(self) -> dict[str, Any]:
        started = datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
        return {
            "deviceId": self.device_id,
            "temperature": self.temperature,
            "threshold": self.threshold,
            "status": self.status,
            "startedAt": self.started_at,
            "durationSeconds": max(0, int((datetime.now(timezone.utc) - started).total_seconds())),
            "location": self.location,
        }


class TemperatureAlertEvaluator:
    """Keep one active alert per device and avoid duplicate threshold alerts."""

    def __init__(self, threshold: float, duration_seconds: int) -> None:
        self.threshold = threshold
        self.duration_seconds = duration_seconds
        self._active: dict[str, TemperatureAlert] = {}

    def evaluate(
        self,
        device_id: str,
        temperature: float,
        timestamp: str | None = None,
        location: dict[str, float] | None = None,
    ) -> dict[str, Any] | None:
        if temperature <= self.threshold:
            self._active.pop(device_id, None)
            return None

        alert = self._active.get(device_id)
        if alert is None:
            alert = TemperatureAlert(device_id, temperature, self.threshold, timestamp or utc_timestamp(), location=location)
            self._active[device_id] = alert
        else:
            alert.temperature = temperature
            alert.location = location

        if alert.as_dict()["durationSeconds"] < self.duration_seconds:
            return None
        return alert.as_dict()

    def active_alerts(self) -> list[dict[str, Any]]:
        return [alert.as_dict() for alert in self._active.values()]


class OperationsService:
    """Authoritative operational snapshot until historical storage is connected."""

    def __init__(self) -> None:
        self.alerts = TemperatureAlertEvaluator(
            settings.HIGH_TEMPERATURE_THRESHOLD,
            settings.HIGH_TEMPERATURE_DURATION_SECONDS,
        )

    def overview(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "updatedAt": utc_timestamp(),
            "alerts": self.alerts.active_alerts(),
            "failures": {"active": 0, "today": 0, "resolvedToday": 0, "critical": 0, "status": "no-source"},
            "temperature": {"status": "unavailable", "points": [], "message": "No historical temperature source is connected."},
            "throughput": {"status": "available", "current": 13400, "average": 13400, "peak": 13400, "points": []},
            "trips": {"status": "available", "items": []},
            "loads": {"status": "unavailable", "items": [], "message": "Load telemetry is not part of the current event contract."},
            "deliveries": {"status": "unavailable", "items": [], "message": "Delivery events are not part of the current event contract."},
        }


operations_service = OperationsService()