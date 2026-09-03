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

# Simplified, mapped shape we'll feed into windowing (Day 4)
class MappedReading(faust.Record, serializer='json'):
    customer_id: str
    truck_id: str
    temperature: float
    timestamp: float
    is_breach: bool  # True if outside the safe cold-chain range

# Matches streamforge/common/config.py -> KAFKA_RAW_TOPIC (default: "raw-telemetry")
raw_topic = app.topic('raw-telemetry', value_type=RawTelemetryEvent)

# Sane physical bounds for a sensor reading — anything outside this is a glitch,
# not a real temperature (even a runaway FAULT wouldn't exceed these).
SENSOR_MIN_PLAUSIBLE_TEMP = -60.0
SENSOR_MAX_PLAUSIBLE_TEMP = 60.0

# Matches streamforge/common/config.py -> SAFE_TEMP_MIN / SAFE_TEMP_MAX
SAFE_TEMP_MIN = -25.0
SAFE_TEMP_MAX = -10.0


def map_event(event: RawTelemetryEvent) -> MappedReading:
    """Reshape a raw telemetry event into the fields windowing actually needs,
    and flag whether it breaches the safe cargo temperature range."""
    is_breach = not (SAFE_TEMP_MIN <= event.temperature <= SAFE_TEMP_MAX)
    return MappedReading(
        customer_id=event.customer_id,
        truck_id=event.truck_id,
        temperature=event.temperature,
        timestamp=event.timestamp,
        is_breach=is_breach,
    )


@app.agent(raw_topic)
async def process_telemetry(events):
    async for event in events:
        # --- Filter stage: drop physically impossible sensor glitches ---
        if not (SENSOR_MIN_PLAUSIBLE_TEMP <= event.temperature <= SENSOR_MAX_PLAUSIBLE_TEMP):
            continue  # drop this event, it's a bad reading

        # --- Map stage: reshape + flag breach status ---
        mapped = map_event(event)

        # Placeholder for tomorrow's windowing stage
        status = "BREACH" if mapped.is_breach else "ok"
        print(f"[{mapped.truck_id}] temp={mapped.temperature}°C ({status}) at {mapped.timestamp}")


if __name__ == '__main__':
    app.main()