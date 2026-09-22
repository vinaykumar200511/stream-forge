import faust
from datetime import timedelta
import time

# Faust application, pointed at local Kafka
app = faust.App(
    'streamforge',
    broker='kafka://localhost:9092',
    store='memory://',
    # TODO (Week 3 - RocksDB): store='rocksdb://' is blocked on Windows.
    # Full native toolchain was verified working (pkg-config, MSVC Build Tools,
    # and RocksDB 11.8.1 built successfully from source via vcpkg - ~1.5hr build).
    # The blocker is a bug inside the faust-streaming-rocksdb==0.9.3 wrapper's
    # own setup.py ("ValueError: list.remove(x): x not in list") when parsing
    # pkg-config output for this RocksDB version - not an environment issue.
    # Next step: run this worker in Docker/WSL (Linux), where prebuilt
    # faust-streaming-rocksdb wheels install cleanly, instead of fighting
    # the Windows native build further.
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
raw_topic = app.topic('raw-telemetry', key_type=str, value_type=RawTelemetryEvent)

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
    lateness = current_time - event_timestamp
    return lateness > (WINDOW_SIZE_SECONDS + GRACE_PERIOD_SECONDS)


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
    # --- TEMPORARY DEBUG: bypass group_by entirely to isolate whether raw
    # messages are being received/deserialized at all. ---
    async for raw_event in events:
        print(f"[RAW RECEIVED] truck={raw_event.truck_id} temp={raw_event.temperature}")
    return

    # --- Original pipeline (temporarily unreachable while debugging) ---
    async for event in events.group_by(lambda e: f"{e.customer_id}:{e.truck_id}", name="by_truck"):
        if not (SENSOR_MIN_PLAUSIBLE_TEMP <= event.temperature <= SENSOR_MAX_PLAUSIBLE_TEMP):
            continue

        if is_too_late(event.timestamp, time.time()):
            print(f"[DROPPED - too late] truck={event.truck_id} timestamp={event.timestamp}")
            continue

        mapped = map_event(event)
        key = f"{mapped.customer_id}:{mapped.truck_id}"

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