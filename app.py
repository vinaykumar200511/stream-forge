import faust
from datetime import timedelta

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

# Simplified, mapped shape fed into windowing
class MappedReading(faust.Record, serializer='json'):
    customer_id: str
    truck_id: str
    temperature: float
    timestamp: float
    is_breach: bool

# Running stats stored per key, per window
class WindowStats(faust.Record, serializer='json'):
    count: int = 0
    total_temp: float = 0.0
    min_temp: float = 999.0   # will be overwritten on first real reading
    max_temp: float = -999.0  # will be overwritten on first real reading

# Matches streamforge/common/config.py -> KAFKA_RAW_TOPIC (default: "raw-telemetry")
raw_topic = app.topic('raw-telemetry', value_type=RawTelemetryEvent)

# Sane physical bounds for a sensor reading — anything outside this is a glitch.
SENSOR_MIN_PLAUSIBLE_TEMP = -60.0
SENSOR_MAX_PLAUSIBLE_TEMP = 60.0

# Matches streamforge/common/config.py -> SAFE_TEMP_MIN / SAFE_TEMP_MAX
SAFE_TEMP_MIN = -25.0
SAFE_TEMP_MAX = -10.0

# Matches streamforge/common/config.py windowing settings
WINDOW_SIZE_SECONDS = 300   # 5-minute windows
WINDOW_SLIDE_SECONDS = 10   # hopping every 10 seconds
GRACE_PERIOD_SECONDS = 30   # late events still accepted (Day 5)


def map_event(event: RawTelemetryEvent) -> MappedReading:
    """Reshape a raw telemetry event and flag whether it breaches the safe range."""
    is_breach = not (SAFE_TEMP_MIN <= event.temperature <= SAFE_TEMP_MAX)
    return MappedReading(
        customer_id=event.customer_id,
        truck_id=event.truck_id,
        temperature=event.temperature,
        timestamp=event.timestamp,
        is_breach=is_breach,
    )


def update_stats(current: WindowStats, temp: float) -> WindowStats:
    """Fold a new temperature reading into the running window stats."""
    is_first = current.count == 0
    return WindowStats(
        count=current.count + 1,
        total_temp=current.total_temp + temp,
        min_temp=temp if is_first else min(current.min_temp, temp),
        max_temp=temp if is_first else max(current.max_temp, temp),
    )


# Windowed table: keyed by "customer_id:truck_id", tracks running stats
# across a 5-minute hopping window that advances every 10 seconds.
temp_windows = app.Table(
    'temp-windows',
    default=WindowStats,
).hopping(
    WINDOW_SIZE_SECONDS,
    WINDOW_SLIDE_SECONDS,
    expires=timedelta(seconds=WINDOW_SIZE_SECONDS + GRACE_PERIOD_SECONDS),
)


@app.agent(raw_topic)
async def process_telemetry(events):
    # Repartition the stream so all events for the same truck land on the
    # same worker/partition — required for correct per-truck windowing.
    async for event in events.group_by(lambda e: f"{e.customer_id}:{e.truck_id}"):
        # --- Filter stage: drop physically impossible sensor glitches ---
        if not (SENSOR_MIN_PLAUSIBLE_TEMP <= event.temperature <= SENSOR_MAX_PLAUSIBLE_TEMP):
            continue

        # --- Map stage: reshape + flag breach status ---
        mapped = map_event(event)
        key = f"{mapped.customer_id}:{mapped.truck_id}"

        # --- Windowing stage: fold this reading into the current window's stats ---
        current = temp_windows[key].value()
        temp_windows[key] = update_stats(current, mapped.temperature)

        updated = temp_windows[key].value()
        avg = updated.total_temp / updated.count
        status = "BREACH" if mapped.is_breach else "ok"
        print(
            f"[{key}] n={updated.count} avg={avg:.2f}°C "
            f"min={updated.min_temp}°C max={updated.max_temp}°C ({status})"
        )


if __name__ == '__main__':
    app.main()