import time
import uuid
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class CompressorStatus(str, Enum):
    RUNNING = "RUNNING"
    DEFROST = "DEFROST"
    OFF = "OFF"
    FAULT = "FAULT"


class AlertSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertType(str, Enum):
    HIGH_TEMPERATURE = "HIGH_TEMPERATURE"
    LOW_TEMPERATURE = "LOW_TEMPERATURE"
    COMPRESSOR_FAILURE = "COMPRESSOR_FAILURE"
    RAPID_THERMAL_RISE = "RAPID_THERMAL_RISE"
    BATTERY_LOW = "BATTERY_LOW"


class RawTelemetryEvent(BaseModel):
    """
    IoT telemetry packet emitted by refrigerated truck sensor gateways.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = Field(default_factory=time.time, description="Unix epoch timestamp in seconds")
    customer_id: str = Field(..., description="Multi-tenant customer / fleet identifier (e.g. cust_01)")
    truck_id: str = Field(..., description="Unique vehicle asset ID (e.g. truck_0012)")
    route_id: str = Field(default="route_default", description="Active logistics route ID")
    
    # Cold-chain thermal readings (°C)
    temperature: float = Field(..., description="Current cargo bay temperature in °C")
    target_temp: float = Field(default=-18.0, description="Set target temperature in °C")
    ambient_temp: float = Field(default=25.0, description="External outside ambient temperature in °C")
    
    # Mechanical & Electrical Status
    compressor_status: CompressorStatus = Field(default=CompressorStatus.RUNNING)
    door_open: bool = Field(default=False)
    battery_level: float = Field(default=98.5, ge=0.0, le=100.0, description="Sensor unit battery percentage")
    
    # Telematics & Geolocation
    latitude: float = Field(default=37.7749, ge=-90.0, le=90.0)
    longitude: float = Field(default=-122.4194, ge=-180.0, le=180.0)
    speed_kmh: float = Field(default=65.0, ge=0.0, description="Current speed in km/h")

    @property
    def kafka_key(self) -> str:
        """Composite partition routing key ensuring all events for a truck route to the same partition."""
        return f"{self.customer_id}:{self.truck_id}"

    @field_validator("customer_id", "truck_id")
    @classmethod
    def not_empty_str(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Identifier must not be empty.")
        return v.strip()


class ProcessedAggregate(BaseModel):
    """
    Continuous rolling / tumbling window aggregate for a single truck.
    """
    aggregate_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    truck_id: str
    window_start: float = Field(description="Window start unix timestamp")
    window_end: float = Field(description="Window end unix timestamp")
    sample_count: int = Field(ge=1, description="Number of events aggregated in this window")
    avg_temperature: float = Field(description="Calculated average temperature across window")
    min_temperature: float
    max_temperature: float
    target_temperature: float
    is_breached: bool = Field(default=False, description="True if average temperature exceeded safe threshold")
    created_at: float = Field(default_factory=time.time)

    @property
    def kafka_key(self) -> str:
        return f"{self.customer_id}:{self.truck_id}"


class AnomalyAlert(BaseModel):
    """
    Real-time critical excursion or failure alert dispatched to operators/webhooks.
    """
    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    truck_id: str
    severity: AlertSeverity = Field(default=AlertSeverity.CRITICAL)
    alert_type: AlertType = Field(default=AlertType.HIGH_TEMPERATURE)
    trigger_temperature: float
    target_temperature: float
    threshold_limit: float
    compressor_status: CompressorStatus = Field(default=CompressorStatus.FAULT)
    timestamp: float = Field(default_factory=time.time)
    message: str
    acknowledged: bool = Field(default=False)

    @property
    def kafka_key(self) -> str:
        return f"{self.customer_id}:{self.truck_id}"
