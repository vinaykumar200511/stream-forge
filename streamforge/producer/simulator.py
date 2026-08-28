import random
import time
from dataclasses import dataclass, field
from typing import Dict, Generator, List, Optional

from streamforge.common.config import settings
from streamforge.common.models import CompressorStatus, RawTelemetryEvent


@dataclass
class TruckState:
    customer_id: str
    truck_id: str
    route_id: str
    target_temp: float = -18.0
    current_temp: float = -18.5
    ambient_temp: float = 24.0
    compressor_status: CompressorStatus = CompressorStatus.RUNNING
    door_open: bool = False
    battery_level: float = 99.0
    latitude: float = 37.7749
    longitude: float = -122.4194
    speed_kmh: float = 65.0
    is_faulty: bool = False
    fault_ticks_remaining: int = 0

    def step(
        self,
        inject_anomaly: bool = False,
        force_recover: bool = False,
        inject_spike: bool = False,
    ) -> None:
        """Simulate one time step of realistic thermodynamic and vehicle behavior."""
        if force_recover:
            self.is_faulty = False
            self.fault_ticks_remaining = 0
            self.compressor_status = CompressorStatus.RUNNING

        if inject_anomaly and not self.is_faulty:
            self.is_faulty = True
            self.fault_ticks_remaining = random.randint(15, 40)
            self.compressor_status = CompressorStatus.FAULT

        # Sudden high-temperature spike injection (transient sensor glitch or thermal burst)
        if inject_spike:
            self.current_temp += random.uniform(12.0, 25.0)

        if self.is_faulty:
            # Compressor failed: cargo bay temperature rises rapidly towards ambient
            heat_gain = random.uniform(0.3, 0.8) + random.gauss(0.0, 0.05)
            self.current_temp += max(0.1, heat_gain)
            self.fault_ticks_remaining -= 1
            if self.fault_ticks_remaining <= 0:
                # Maintenance intervention / recovery
                self.is_faulty = False
                self.compressor_status = CompressorStatus.RUNNING
        else:
            # Normal operation: cooling towards target with realistic small variations
            small_variation = random.gauss(0.0, 0.05)
            if self.current_temp > self.target_temp + 0.5:
                self.compressor_status = CompressorStatus.RUNNING
                cooling_delta = random.uniform(0.1, 0.4) + small_variation
                self.current_temp -= max(0.05, cooling_delta)
            elif self.current_temp < self.target_temp - 1.0:
                self.compressor_status = CompressorStatus.OFF
                warming_delta = random.uniform(0.05, 0.2) + small_variation
                self.current_temp += max(0.02, warming_delta)
            else:
                # Oscillating around target with fine-grained noise
                self.current_temp += random.uniform(-0.15, 0.15) + small_variation

            # Rare brief door opening
            if random.random() < 0.02:
                self.door_open = True
                self.current_temp += random.uniform(0.2, 0.6)
            else:
                self.door_open = False

        # Battery slow drain
        self.battery_level = max(5.0, self.battery_level - random.uniform(0.001, 0.005))

        # Vehicle kinematics simulation
        self.speed_kmh = max(0.0, min(110.0, self.speed_kmh + random.uniform(-2.0, 2.0)))
        if self.speed_kmh > 0:
            self.latitude += random.uniform(-0.0005, 0.0005)
            self.longitude += random.uniform(-0.0005, 0.0005)


class TelemetrySimulator:
    """
    Multi-tenant IoT fleet simulator producing high-fidelity cold-chain telemetry events.
    """

    def __init__(
        self,
        num_customers: int = None,
        num_trucks: int = None,
        anomaly_rate: float = 0.03,
        late_event_rate: float = 0.05,
        drop_rate: float = 0.0,
        max_late_delay_sec: float = 120.0,
    ):
        self.num_customers = num_customers or settings.SIMULATION_NUM_CUSTOMERS
        self.num_trucks = num_trucks or settings.SIMULATION_NUM_TRUCKS
        self.anomaly_rate = anomaly_rate
        self.late_event_rate = late_event_rate
        self.drop_rate = drop_rate
        self.max_late_delay_sec = max_late_delay_sec
        self.fleet: Dict[str, TruckState] = {}
        self._init_fleet()

    def _init_fleet(self) -> None:
        """Initialize fleet state distribution across tenants."""
        for c_idx in range(1, self.num_customers + 1):
            customer_id = f"cust_{c_idx:02d}"
            trucks_per_cust = max(1, self.num_trucks // self.num_customers)
            for t_idx in range(1, trucks_per_cust + 1):
                truck_id = f"trk_{c_idx:02d}_{t_idx:04d}"
                route_id = f"route_{c_idx:02d}_{(t_idx % 10) + 1:02d}"
                
                # Vary base target temperature per vehicle profile
                base_target = random.choice([-22.0, -20.0, -18.0, -15.0])
                initial_temp = base_target + random.uniform(-0.5, 0.5)

                state = TruckState(
                    customer_id=customer_id,
                    truck_id=truck_id,
                    route_id=route_id,
                    target_temp=base_target,
                    current_temp=initial_temp,
                    ambient_temp=random.uniform(20.0, 32.0),
                    battery_level=random.uniform(85.0, 100.0),
                    latitude=37.7749 + random.uniform(-2.0, 2.0),
                    longitude=-122.4194 + random.uniform(-2.0, 2.0),
                    speed_kmh=random.uniform(50.0, 85.0),
                )
                self.fleet[f"{customer_id}:{truck_id}"] = state

    @property
    def total_trucks(self) -> int:
        return len(self.fleet)

    def generate_event_for_truck(
        self,
        truck_key: str,
        force_spike: bool = False,
        force_late: bool = False,
        delay_seconds: Optional[float] = None,
    ) -> RawTelemetryEvent:
        """Advance one truck's state and emit its telemetry event with optional late-arrival timestamp delay."""
        state = self.fleet[truck_key]
        
        # Determine if an anomaly or high-temp spike should be injected
        trigger_anomaly = random.random() < self.anomaly_rate
        inject_spike = force_spike or (trigger_anomaly and random.random() < 0.5)
        inject_fault = trigger_anomaly and not inject_spike

        state.step(inject_anomaly=inject_fault, inject_spike=inject_spike)

        event_timestamp = time.time()
        # Simulate late-arriving / out-of-order telemetry for watermark grace testing
        should_be_late = force_late or (random.random() < self.late_event_rate)
        if should_be_late:
            actual_delay = delay_seconds if delay_seconds is not None else random.uniform(10.0, self.max_late_delay_sec)
            event_timestamp -= actual_delay

        # Add realistic minor sensor measurement noise
        sensor_noise = random.gauss(0.0, 0.05)
        reported_temp = round(state.current_temp + sensor_noise, 2)

        return RawTelemetryEvent(
            customer_id=state.customer_id,
            truck_id=state.truck_id,
            route_id=state.route_id,
            timestamp=event_timestamp,
            temperature=reported_temp,
            target_temp=round(state.target_temp, 2),
            ambient_temp=round(state.ambient_temp, 2),
            compressor_status=state.compressor_status,
            door_open=state.door_open,
            battery_level=round(state.battery_level, 2),
            latitude=round(state.latitude, 6),
            longitude=round(state.longitude, 6),
            speed_kmh=round(state.speed_kmh, 1),
        )

    def generate_batch(self, batch_size: int = 100, simulate_drops: bool = True) -> List[RawTelemetryEvent]:
        """Generate a random batch of events across the fleet, honoring optional dropped message logic."""
        keys = list(self.fleet.keys())
        selected_keys = random.choices(keys, k=batch_size)
        events = []
        for k in selected_keys:
            if simulate_drops and self.drop_rate > 0 and random.random() < self.drop_rate:
                continue  # Simulate dropped packet / network loss
            events.append(self.generate_event_for_truck(k))
        return events

    def stream_events(self, simulate_drops: bool = True) -> Generator[RawTelemetryEvent, None, None]:
        """Continuous generator streaming events infinitely across the active fleet, honoring dropped message logic."""
        keys = list(self.fleet.keys())
        while True:
            k = random.choice(keys)
            if simulate_drops and self.drop_rate > 0 and random.random() < self.drop_rate:
                continue  # Simulate dropped packet / network loss
            yield self.generate_event_for_truck(k)
