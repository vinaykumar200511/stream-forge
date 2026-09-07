import faust
from datetime import timedelta
import time

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

    # Tell Faust to use THIS field as the event's timestamp for windowing,
    # instead of the time the message happened to be processed/received.
    def faust_timestamp(self) -> float:
        return self.timestamp


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
    min_temp: float = 999.0
    max_temp: float = -999.0

# Matches streamforge/common/config.py -> KAFKA_RAW_TOPIC (default: "raw-telemetry")
raw_topic = app.topic('raw-telemetry', value_type=RawTelemetryEvent)

SENSOR_MIN_PLAUSIBLE_TEMP = -60.0
SENSOR_MAX_PLAUSIBLE_TEMP = 60.0

SAFE_TEMP_MIN = -25.0
SAFE_TEMP_MAX = -10.0

WINDOW_SIZE_SECONDS = 300
WINDOW_SLIDE_SECONDS = 10
GRACE_PERIOD_SECONDS = 30  # late events up to 30s old are still accepted


def map_event(event: RawTelemetryEvent) -> MappedReading:
    is_breach = not (SAFE_TEMP_MIN <= event.temperature <= SAFE_TEMP_MAX)
    return MappedReading(
        customer_id=event.customer_id,
        truck_id=event.truck_id,
        temperature=event.temperature,
        timestamp=event.timestamp,
        is_breach=is_breach,
    )


def update_stats(current: WindowStats, temp: float) -> WindowStats:
    is_first = current.count == 0
    return WindowStats(
        count=current.count + 1,
        total_temp=current.total_temp + temp,
        min_temp=temp if is_first else min(current.min_temp, temp),
        max_temp=temp if is_first else max(current.max_temp, temp),
    )


def is_too_late(event_timestamp: float, current_time: float) -> bool:
    """
    An event is considered unrecoverably late (its window has already closed
    and won't reopen) if it arrives older than the grace period allows.
    """
    lateness = current_time - event_timestamp
    return lateness > (WINDOW_SIZE_SECONDS + GRACE_PERIOD_SECONDS)


# Windowed table: keyed by "customer_id:truck_id", tracks running stats
# across a 5-minute hopping window that advances every 10 seconds.
# expires= gives late events up to GRACE_PERIOD_SECONDS extra time to land
# in their correct window before that window is finalized and discarded.
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
    async for event in events.group_by(lambda e: f"{e.customer_id}:{e.truck_id}"):
        # --- Filter stage: drop physically impossible sensor glitches ---
        if not (SENSOR_MIN_PLAUSIBLE_TEMP <= event.temperature <= SENSOR_MAX_PLAUSIBLE_TEMP):
            continue

        # --- Late-arrival guard: drop events too old to matter anymore ---
        if is_too_late(event.timestamp, time.time()):
            print(f"[DROPPED - too late] truck={event.truck_id} timestamp={event.timestamp}")
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