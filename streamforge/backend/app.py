"""
StreamForge API & Observability Tier — FastAPI Application Entrypoint.
Provides operational health checks, Prometheus /metrics exporter, and telemetry APIs.
"""

import asyncio
import time
from typing import Dict, Any

from fastapi import FastAPI, Query, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from streamforge.common.config import settings
from streamforge.backend.metrics import get_dashboard_metrics, get_kafka_observability, get_prometheus_metrics
from streamforge.backend.kafka_metrics import kafka_metrics_service
from streamforge.backend.operations import operations_service

# Application initialization
app = FastAPI(
    title="StreamForge FleetPulse API",
    description="Real-Time Cold-Chain Telemetry Event Processing & Analytics API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Configure CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Server startup timestamp for uptime tracking
START_TIME = time.time()


@app.on_event("startup")
async def start_metrics_collection() -> None:
    await kafka_metrics_service.start()


@app.on_event("shutdown")
async def stop_metrics_collection() -> None:
    await kafka_metrics_service.stop()


@app.get("/", tags=["General"])
async def root() -> Dict[str, Any]:
    """Root metadata endpoint."""
    return {
        "title": "StreamForge FleetPulse API",
        "service": "streamforge-backend",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "health": "/health",
            "metrics": "/metrics",
        },
    }


@app.get("/health", tags=["Observability"])
async def health_check() -> Dict[str, Any]:
    """Operational health check endpoint for liveness probes."""
    uptime = round(time.time() - START_TIME, 2)
    return {
        "status": "ok",
        "service": "streamforge-backend",
        "version": "1.0.0",
        "uptime_seconds": uptime,
        "timestamp": round(time.time(), 3),
        "kafka_bootstrap": settings.KAFKA_BOOTSTRAP_SERVERS,
    }


@app.get("/api/metrics/consumer-groups", tags=["Observability"])
async def consumer_group_metrics() -> Dict[str, Any]:
    """Return the latest cached authoritative Kafka consumer-group metrics."""
    return kafka_metrics_service.snapshot()


@app.get("/api/metrics/consumer-groups/{group_id}", tags=["Observability"])
async def consumer_group_metric(group_id: str) -> Dict[str, Any]:
    """Return one cached consumer-group metric record."""
    snapshot = kafka_metrics_service.snapshot()
    group = next((item for item in snapshot["groups"] if item["consumerGroup"] == group_id), None)
    if group is None:
        return {"status": snapshot["status"], "consumerGroup": group_id, "topics": [], "timestamp": snapshot["timestamp"]}
    return {"status": snapshot["status"], **group}


@app.websocket("/api/metrics/stream")
async def metrics_stream(websocket: WebSocket) -> None:
    """Push cached metrics to the dashboard without creating Kafka clients per browser."""
    await websocket.accept()
    try:
        while True:
            await websocket.send_json({
                "type": "metrics_update",
                **kafka_metrics_service.snapshot(),
                "operations": operations_service.overview(),
            })
            await asyncio.sleep(settings.KAFKA_METRICS_POLL_INTERVAL_SECONDS)
    except (WebSocketDisconnect, asyncio.CancelledError):
        return


@app.get("/api/operations/overview", tags=["Operations"])
async def operations_overview() -> Dict[str, Any]:
    """Return authoritative alert, failure, analytics, trip, load, and delivery states."""
    return operations_service.overview()


@app.get("/api/alerts/temperature", tags=["Operations"])
async def temperature_alerts() -> Dict[str, Any]:
    """Return currently active high-temperature alerts."""
    return {
        "status": "ok",
        "threshold": operations_service.alerts.threshold,
        "alerts": operations_service.alerts.active_alerts(),
        "updatedAt": operations_service.overview()["updatedAt"],
    }


@app.websocket("/api/operations/stream")
async def operations_stream(websocket: WebSocket) -> None:
    """Stream one consistent operations envelope over the existing realtime channel."""
    await websocket.accept()
    try:
        while True:
            await websocket.send_json({"type": "operations_update", **operations_service.overview()})
            await asyncio.sleep(settings.KAFKA_METRICS_POLL_INTERVAL_SECONDS)
    except (WebSocketDisconnect, asyncio.CancelledError):
        return


@app.get("/metrics", tags=["Observability"])
async def prometheus_metrics() -> Response:
    """Prometheus metrics scraping endpoint."""
    content, media_type = get_prometheus_metrics()
    return Response(content=content, media_type=media_type)


@app.get("/topology", tags=["Observability"])
async def topology_view() -> Dict[str, Any]:
    """Return the live processing topology for the React Flow dashboard."""
    kafka_metrics = get_kafka_observability()
    kafka_status = "live" if kafka_metrics["status"] == "healthy" else "unavailable"
    lag_status = "healthy" if kafka_metrics["maxPartitionLag"] < 1000 else "degraded"
    return {
        "status": "ok",
        "service": "streamforge-backend",
        "updated_at": round(time.time(), 3),
        "nodes": [
            {
                "id": "producer",
                "position": {"x": 60, "y": 180},
                "data": {"label": "Truck Telemetry Producer", "status": "healthy"},
                "style": {
                    "background": "#2563eb",
                    "color": "#ffffff",
                    "border": "none",
                    "borderRadius": "8px",
                    "padding": "12px",
                    "width": "180px",
                },
            },
            {
                "id": "kafka",
                "position": {"x": 330, "y": 180},
                "data": {"label": "Kafka: raw-telemetry", "status": kafka_status, "lag": kafka_metrics["totalLag"], "consumerGroup": kafka_metrics["consumerGroup"]},
                "style": {
                    "background": "#f97316",
                    "color": "#ffffff",
                    "border": "none",
                    "borderRadius": "8px",
                    "padding": "12px",
                    "width": "180px",
                },
            },
            {
                "id": "worker1",
                "position": {"x": 620, "y": 90},
                "data": {"label": "Worker 01\nPartitions 0-2", "status": lag_status, "lag": sum(item["lag"] for item in kafka_metrics["partitions"] if item["partition"] in (0, 1, 2))},
                "style": {
                    "background": "#16a34a",
                    "color": "#ffffff",
                    "border": "none",
                    "borderRadius": "8px",
                    "padding": "12px",
                    "width": "150px",
                },
            },
            {
                "id": "worker2",
                "position": {"x": 620, "y": 270},
                "data": {"label": "Worker 02\nPartitions 3-5", "status": lag_status, "lag": sum(item["lag"] for item in kafka_metrics["partitions"] if item["partition"] in (3, 4, 5))},
                "style": {
                    "background": "#2dd4bf",
                    "color": "#ffffff",
                    "border": "none",
                    "borderRadius": "8px",
                    "padding": "12px",
                    "width": "150px",
                },
            },
            {
                "id": "aggregator",
                "position": {"x": 900, "y": 180},
                "data": {"label": "processed-averages", "status": kafka_status},
                "style": {
                    "background": "#7c3aed",
                    "color": "#ffffff",
                    "border": "none",
                    "borderRadius": "8px",
                    "padding": "12px",
                    "width": "180px",
                },
            },
            {
                "id": "dashboard",
                "position": {"x": 1180, "y": 180},
                "data": {"label": "Fleet Dashboard", "status": "streaming", "lag": kafka_metrics["averagePartitionLag"]},
                "style": {
                    "background": "#0ea5e9",
                    "color": "#ffffff",
                    "border": "none",
                    "borderRadius": "8px",
                    "padding": "12px",
                    "width": "170px",
                },
            },
        ],
        "edges": [
            {"id": "producer-kafka", "source": "producer", "target": "kafka", "animated": True, "label": "13k ev/s"},
            {"id": "kafka-worker1", "source": "kafka", "target": "worker1", "animated": True, "label": "partitions 0-2"},
            {"id": "kafka-worker2", "source": "kafka", "target": "worker2", "animated": True, "label": "partitions 3-5"},
            {"id": "worker1-aggregator", "source": "worker1", "target": "aggregator", "animated": True, "label": "5m avg"},
            {"id": "worker2-aggregator", "source": "worker2", "target": "aggregator", "animated": True, "label": "5m avg"},
            {"id": "aggregator-dashboard", "source": "aggregator", "target": "dashboard", "animated": True, "label": "WebSocket"},
        ],
        "streamMetrics": kafka_metrics,
    }


@app.get("/dashboard/metrics", tags=["Dashboard"])
async def dashboard_metrics() -> Dict[str, Any]:
    """Return the KPI values consumed by the React dashboard."""
    return {
        "status": "ok",
        "service": "streamforge-backend",
        "updated_at": round(time.time(), 3),
        "metrics": get_dashboard_metrics(9),
        "streamMetrics": get_kafka_observability(),
    }


@app.get("/routes", tags=["Dashboard"])
async def route_view(
    truck_id: str | None = Query(default=None, min_length=1),
    date: str | None = Query(default=None, min_length=1),
    route: str | None = Query(default=None, min_length=1),
    truck_type: str | None = Query(default=None, min_length=1),
) -> Dict[str, Any]:
    """Return GPS route data, optionally filtered by fleet dimensions."""
    vehicles = [
            {
                "id": "truck-204",
                "telemetry_date": "2026-09-19",
                "route_name": "Hudson Cold Chain",
                "truck_type": "Refrigerated",
                "name": "Truck 204",
                "speed": 58,
                "status": "moving",
                "lat": 40.7128,
                "lng": -74.0060,
                "route": [
                    {"lat": 40.7000, "lng": -74.0100},
                    {"lat": 40.7060, "lng": -74.0200},
                    {"lat": 40.7128, "lng": -74.0060},
                    {"lat": 40.7200, "lng": -73.9900},
                ],
            },
            {
                "id": "truck-118",
                "telemetry_date": "2026-09-19",
                "route_name": "Midtown Express",
                "truck_type": "Refrigerated",
                "name": "Truck 118",
                "speed": 45,
                "status": "delayed",
                "lat": 40.7484,
                "lng": -73.9857,
                "route": [
                    {"lat": 40.7600, "lng": -73.9800},
                    {"lat": 40.7560, "lng": -73.9880},
                    {"lat": 40.7484, "lng": -73.9857},
                    {"lat": 40.7420, "lng": -73.9770},
                ],
            },
            {
                "id": "truck-87",
                "telemetry_date": "2026-09-18",
                "route_name": "Queens Transfer",
                "truck_type": "Frozen Goods",
                "name": "Truck 87",
                "speed": 62,
                "status": "moving",
                "lat": 40.7306,
                "lng": -73.9352,
                "route": [
                    {"lat": 40.7400, "lng": -73.9500},
                    {"lat": 40.7340, "lng": -73.9430},
                    {"lat": 40.7306, "lng": -73.9352},
                    {"lat": 40.7240, "lng": -73.9280},
                ],
            },
            {
                "id": "truck-1",
                "telemetry_date": "2026-09-18",
                "route_name": "Hudson Cold Chain",
                "truck_type": "Produce",
                "name": "Truck 1",
                "speed": 51,
                "status": "moving",
                "lat": 40.7180,
                "lng": -73.9970,
                "route": [
                    {"lat": 40.7040, "lng": -74.0160},
                    {"lat": 40.7100, "lng": -74.0060},
                    {"lat": 40.7180, "lng": -73.9970},
                    {"lat": 40.7260, "lng": -73.9850},
                ],
            },
            {
                "id": "truck-2",
                "telemetry_date": "2026-09-19",
                "route_name": "Midtown Express",
                "truck_type": "Frozen Goods",
                "name": "Truck 2",
                "speed": 42,
                "status": "delayed",
                "lat": 40.7540,
                "lng": -73.9760,
                "route": [
                    {"lat": 40.7660, "lng": -73.9910},
                    {"lat": 40.7600, "lng": -73.9840},
                    {"lat": 40.7540, "lng": -73.9760},
                    {"lat": 40.7460, "lng": -73.9680},
                ],
            },
            {
                "id": "truck-3",
                "telemetry_date": "2026-09-19",
                "route_name": "Queens Transfer",
                "truck_type": "Produce",
                "name": "Truck 3",
                "speed": 67,
                "status": "moving",
                "lat": 40.7350,
                "lng": -73.9580,
                "route": [
                    {"lat": 40.7440, "lng": -73.9740},
                    {"lat": 40.7400, "lng": -73.9660},
                    {"lat": 40.7350, "lng": -73.9580},
                    {"lat": 40.7280, "lng": -73.9480},
                ],
            },
            {
                "id": "truck-4",
                "telemetry_date": "2026-09-18",
                "route_name": "Hudson Cold Chain",
                "truck_type": "Refrigerated",
                "name": "Truck 4",
                "speed": 38,
                "status": "delayed",
                "lat": 40.7420,
                "lng": -73.9420,
                "route": [
                    {"lat": 40.7500, "lng": -73.9540},
                    {"lat": 40.7460, "lng": -73.9480},
                    {"lat": 40.7420, "lng": -73.9420},
                    {"lat": 40.7360, "lng": -73.9340},
                ],
            },
            {
                "id": "truck-5",
                "telemetry_date": "2026-09-19",
                "route_name": "Midtown Express",
                "truck_type": "Produce",
                "name": "Truck 5",
                "speed": 55,
                "status": "moving",
                "lat": 40.7240,
                "lng": -73.9700,
                "route": [
                    {"lat": 40.7120, "lng": -73.9820},
                    {"lat": 40.7180, "lng": -73.9760},
                    {"lat": 40.7240, "lng": -73.9700},
                    {"lat": 40.7320, "lng": -73.9620},
                ],
            },
            {
                "id": "truck-6",
                "telemetry_date": "2026-09-18",
                "route_name": "Queens Transfer",
                "truck_type": "Refrigerated",
                "name": "Truck 6",
                "speed": 48,
                "status": "moving",
                "lat": 40.7590,
                "lng": -73.9520,
                "route": [
                    {"lat": 40.7480, "lng": -73.9660},
                    {"lat": 40.7540, "lng": -73.9600},
                    {"lat": 40.7590, "lng": -73.9520},
                    {"lat": 40.7650, "lng": -73.9440},
                ],
            },
        ]

    filtered_vehicles = [
        vehicle for vehicle in vehicles
        if (not truck_id or vehicle["id"].lower() == truck_id.lower())
        and (not date or vehicle["telemetry_date"] == date)
        and (not route or vehicle["route_name"].lower() == route.lower())
        and (not truck_type or vehicle["truck_type"].lower() == truck_type.lower())
    ]

    for vehicle in filtered_vehicles:
        point_count = len(vehicle["route"])
        vehicle["route"] = [
            {**point, "timestamp": round(time.time() - (point_count - index - 1) * 60, 3)}
            for index, point in enumerate(vehicle["route"])
        ]

    return {
        "status": "ok",
        "service": "streamforge-backend",
        "updated_at": round(time.time(), 3),
        "vehicles": filtered_vehicles,
    }


@app.get("/vehicles/history", tags=["Dashboard"])
@app.get("/dashboard/vehicles/history", tags=["Dashboard"], include_in_schema=False)
async def vehicle_history(
    plate: str = Query(
        default="NYC-204",
        min_length=2,
        description="Vehicle number plate, for example NYC-204 or NYC-118.",
    ),
) -> Dict[str, Any]:
    """Return owner, driver, and recent trip history for a number plate."""
    records = {
        "NYC-204": {
            "plate": "NYC-204",
            "vehicle": "Truck 204",
            "owner": "Northstar Cold Logistics",
            "driver": "Maya Patel",
            "driverStatus": "On duty",
            "lastSeen": "2026-09-19 14:32",
            "history": [
                {"date": "2026-09-19", "route": "Hudson Cold Chain", "status": "Completed", "distance": "48 km"},
                {"date": "2026-09-18", "route": "Hudson Cold Chain", "status": "Completed", "distance": "51 km"},
                {"date": "2026-09-17", "route": "Midtown Express", "status": "Delayed", "distance": "36 km"},
            ],
        },
        "NYC-118": {
            "plate": "NYC-118",
            "vehicle": "Truck 118",
            "owner": "Northstar Cold Logistics",
            "driver": "Jordan Brooks",
            "driverStatus": "On duty",
            "lastSeen": "2026-09-19 14:28",
            "history": [
                {"date": "2026-09-19", "route": "Midtown Express", "status": "Delayed", "distance": "29 km"},
                {"date": "2026-09-18", "route": "Queens Transfer", "status": "Completed", "distance": "62 km"},
            ],
        },
    }
    normalized_plate = plate.strip().upper()
    record = records.get(normalized_plate)
    if record is None:
        return {"status": "not_found", "plate": normalized_plate, "history": []}
    return {"status": "ok", "service": "streamforge-backend", "record": record}
