import faust
from datetime import timedelta
import time

app = faust.App(
    'streamforge',
    broker='kafka://localhost:9092',
    store='memory://',
    # TODO (Week 3 - RocksDB): store='rocksdb://' is blocked on Windows.
    # Full toolchain (pkg-config, MSVC Build Tools, RocksDB via vcpkg) built
    # successfully; blocked by a bug in faust-streaming-rocksdb==0.9.3's
    # own setup.py. Next step: run this worker in Docker/WSL (Linux).
)

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

    def faust_timestamp(self) -> float:
        return self.timestamp


class MappedReading(faust.Record, serializer='json'):
    customer_id: str
    truck_id: str
    temperature: float
    timestamp: float
    is_breach: bool


class WindowStats(faust.Record, serializer='json'):
    count: int = 0
    total_temp: float = 0.0
    min_temp: float = 999.0
    max_temp: float = -999.0


raw_topic = app.topic('raw-telemetry', key_type=str, value_type=RawTelemetryEvent)

SENSOR_MIN_PLAUSIBLE_TEMP = -60.0
SENSOR_MAX_PLAUSIBLE_TEMP = 60.0
SAFE_TEMP_MIN = -25.0
SAFE_TEMP_MAX = -10.0
WINDOW_SIZE_SECONDS = 300
WINDOW_SLIDE_SECONDS = 10
GRACE_PERIOD_SECONDS = 30


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