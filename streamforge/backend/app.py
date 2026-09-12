"""
StreamForge API & Observability Tier — FastAPI Application Entrypoint.
Provides operational health checks, Prometheus /metrics exporter, and telemetry APIs.
"""

import time
from typing import Dict, Any

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

from streamforge.common.config import settings
from streamforge.backend.metrics import get_prometheus_metrics

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


@app.get("/metrics", tags=["Observability"])
async def prometheus_metrics() -> Response:
    """Prometheus metrics scraping endpoint."""
    content, media_type = get_prometheus_metrics()
    return Response(content=content, media_type=media_type)


@app.get("/topology", tags=["Observability"])
async def topology_view() -> Dict[str, Any]:
    """Return the live processing topology for the React Flow dashboard."""
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
                "data": {"label": "Kafka: raw-telemetry", "status": "live"},
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
                "data": {"label": "Worker 01\nPartitions 0-2", "status": "healthy"},
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
                "data": {"label": "Worker 02\nPartitions 3-5", "status": "healthy"},
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
                "data": {"label": "processed-averages", "status": "live"},
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
                "data": {"label": "Fleet Dashboard", "status": "streaming"},
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
    }


@app.get("/routes", tags=["Observability"])
async def route_view() -> Dict[str, Any]:
    """Return GPS route data for the movement visualization panel."""
    return {
        "status": "ok",
        "service": "streamforge-backend",
        "updated_at": round(time.time(), 3),
        "vehicles": [
            {
                "id": "truck-204",
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
        ],
    }
