import time
from streamforge.common.models import CompressorStatus
from streamforge.producer.simulator import TelemetrySimulator, TruckState


def test_truck_state_normal_cooling():
    truck = TruckState(
        customer_id="cust_01",
        truck_id="trk_01_0001",
        route_id="route_01",
        target_temp=-20.0,
        current_temp=-10.0,
        compressor_status=CompressorStatus.RUNNING,
    )

    # Step forward multiple times
    for _ in range(10):
        truck.step(inject_anomaly=False)

    # Temperature should have decreased towards target
    assert truck.current_temp < -10.0


def test_truck_state_fault_heating():
    truck = TruckState(
        customer_id="cust_01",
        truck_id="trk_01_0001",
        route_id="route_01",
        target_temp=-20.0,
        current_temp=-20.0,
        compressor_status=CompressorStatus.RUNNING,
    )

    # Inject anomaly
    truck.step(inject_anomaly=True)
    assert truck.is_faulty is True
    assert truck.compressor_status == CompressorStatus.FAULT

    initial_fault_temp = truck.current_temp
    for _ in range(5):
        truck.step(inject_anomaly=False)

    # Temperature should rise rapidly towards ambient during compressor fault
    assert truck.current_temp > initial_fault_temp


def test_simulator_fleet_initialization():
    simulator = TelemetrySimulator(num_customers=3, num_trucks=30, anomaly_rate=0.0)
    assert simulator.total_trucks == 30
    assert len(simulator.fleet) == 30

    # Ensure keys format is cust_id:truck_id
    sample_key = list(simulator.fleet.keys())[0]
    assert ":" in sample_key
    assert sample_key.startswith("cust_")


def test_simulator_batch_generation():
    simulator = TelemetrySimulator(num_customers=2, num_trucks=20, anomaly_rate=0.1, late_event_rate=0.0)
    batch = simulator.generate_batch(batch_size=15)

    assert len(batch) == 15
    for event in batch:
        assert event.customer_id in ["cust_01", "cust_02"]
        assert event.kafka_key == f"{event.customer_id}:{event.truck_id}"
        assert -40.0 <= event.temperature <= 40.0


def test_simulator_late_event_generation():
    # 100% late event rate to test delay injection
    simulator = TelemetrySimulator(num_customers=1, num_trucks=5, late_event_rate=1.0)
    event = simulator.generate_batch(batch_size=1)[0]
    
    current_time = time.time()
    # Event timestamp should be delayed by at least 9 seconds
    assert current_time - event.timestamp >= 9.0
