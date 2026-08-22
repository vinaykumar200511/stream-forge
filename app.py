import faust

# Faust application, pointed at local Kafka
app = faust.App(
    'streamforge',
    broker='kafka://localhost:9092',
    store='memory://',  # we'll swap this for RocksDB in Week 3
)

# Schema for incoming truck telemetry events
class TruckEvent(faust.Record, serializer='json'):
    truck_id: str
    temperature: float
    timestamp: float

# Input topic — matches whatever topic the Week 1 producer wrote to
truck_topic = app.topic('truck-telemetry', value_type=TruckEvent)

if __name__ == '__main__':
    app.main()