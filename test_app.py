"""
Unit tests for the pure logic functions in app.py.
These don't require a live Kafka connection - they test the
filter/map/windowing math in isolation, the same way it was
verified manually throughout Week 2 development.
"""
from app import (
    map_event,
    update_stats,
    is_too_late,
    WindowStats,
    RawTelemetryEvent,
    SENSOR_MIN_PLAUSIBLE_TEMP,
    SENSOR_MAX_PLAUSIBLE_TEMP,
    SAFE_TEMP_MIN,
    SAFE_TEMP_MAX,
    WINDOW_SIZE_SECONDS,
    GRACE_PERIOD_SECONDS,
)


def make_event(temperature: float, timestamp: float = 1000.0) -> RawTelemetryEvent:
    """Helper to build a minimal valid RawTelemetryEvent for testing."""
    return RawTelemetryEvent(
        event_id="test-1",
        timestamp=timestamp,
        customer_id="cust_01",
        truck_id="truck_0001",
        route_id="route_default",
        temperature=temperature,
        target_temp=-18.0,
        ambient_temp=25.0,
        compressor_status="RUNNING",
        door_open=False,
        battery_level=98.5,
        latitude=37.7749,
        longitude=-122.4194,
        speed_kmh=65.0,
    )


class TestMapEvent:
    def test_temperature_inside_safe_range_is_not_a_breach(self):
        event = make_event(temperature=-18.0)
        result = map_event(event)
        assert result.is_breach is False

    def test_temperature_above_safe_max_is_a_breach(self):
        event = make_event(temperature=-5.0)
        result = map_event(event)
        assert result.is_breach is True

    def test_temperature_below_safe_min_is_a_breach(self):
        event = make_event(temperature=-30.0)
        result = map_event(event)
        assert result.is_breach is True

    def test_boundary_values_are_inclusive_and_safe(self):
        assert map_event(make_event(temperature=SAFE_TEMP_MIN)).is_breach is False
        assert map_event(make_event(temperature=SAFE_TEMP_MAX)).is_breach is False


class TestUpdateStats:
    def test_first_reading_sets_all_fields(self):
        stats = update_stats(WindowStats(), temp=-18.0)
        assert stats.count == 1
        assert stats.total_temp == -18.0
        assert stats.min_temp == -18.0
        assert stats.max_temp == -18.0

    def test_running_average_across_multiple_readings(self):
        stats = WindowStats()
        for temp in [-18.0, -20.0, -15.0, -22.0]:
            stats = update_stats(stats, temp)
        assert stats.count == 4
        assert stats.total_temp == -75.0
        assert stats.min_temp == -22.0
        assert stats.max_temp == -15.0


class TestIsTooLate:
    def test_recent_event_is_accepted(self):
        now = 1000000.0
        assert is_too_late(event_timestamp=now - 10, current_time=now) is False

    def test_event_within_grace_period_is_accepted(self):
        now = 1000000.0
        limit = WINDOW_SIZE_SECONDS + GRACE_PERIOD_SECONDS
        assert is_too_late(event_timestamp=now - limit, current_time=now) is False

    def test_event_past_grace_period_is_rejected(self):
        now = 1000000.0
        limit = WINDOW_SIZE_SECONDS + GRACE_PERIOD_SECONDS
        assert is_too_late(event_timestamp=now - limit - 1, current_time=now) is True
        