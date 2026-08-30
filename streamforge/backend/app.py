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
