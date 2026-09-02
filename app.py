import faust

# Faust application, pointed at local Kafka
app = faust.App(
    'streamforge',
    broker='kafka://localhost:9092',
    store='memory://',  # we'll swap this for RocksDB in Week 3
)

# Matches streamforge/common/models.py -> RawTelemetryEvent
class RawTelemetryEvent(faust.Record, serializer='json'):
    event_id: str
    timestamp: float
    customer_id: str
    truck_id: str
    route_id: str
    temperature: float
    target_temp: float
    ambient_temp: float
    compressor_status: str
    door_open: bool
    battery_level: float
    latitude: float
    longitude: float
    speed_kmh: float

# Matches streamforge/common/config.py -> KAFKA_RAW_TOPIC (default: "raw-telemetry")
raw_topic = app.topic('raw-telemetry', value_type=RawTelemetryEvent)

# Sane physical bounds for a sensor reading — anything outside this is a glitch,
# not a real temperature (even a runaway FAULT wouldn't exceed these).
SENSOR_MIN_PLAUSIBLE_TEMP = -60.0
SENSOR_MAX_PLAUSIBLE_TEMP = 60.0


@app.agent(raw_topic)
async def process_telemetry(events):
    async for event in events:
        # --- Filter stage: drop physically impossible sensor glitches ---
        if not (SENSOR_MIN_PLAUSIBLE_TEMP <= event.temperature <= SENSOR_MAX_PLAUSIBLE_TEMP):
            continue  # drop this event, it's a bad reading

        # Placeholder for tomorrow's Map stage
        print(f"[{event.truck_id}] temp={event.temperature}°C at {event.timestamp}")


if __name__ == '__main__':
    app.main()