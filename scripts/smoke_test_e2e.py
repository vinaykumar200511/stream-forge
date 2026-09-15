"""
End-to-end Kafka smoke test for StreamForge.

Produces a few raw telemetry events, then consumes them back to prove
message flow from producer -> broker -> consumer.  Logs every step to
console so you can watch the round-trip in real time.

Usage:
    python scripts/smoke_test_e2e.py
    python scripts/smoke_test_e2e.py --max-messages 5 --topic raw-telemetry
"""
import argparse
import json
import logging
import sys
import time

from confluent_kafka import Consumer, KafkaException, Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("streamforge.smoke")


def _build_producer(bootstrap_servers: str) -> Producer:
    config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "streamforge-smoke-producer",
        "acks": "1",
        "linger.ms": 10,
    }
    return Producer(config)


def _build_consumer(bootstrap_servers: str, topic: str, group_id: str) -> Consumer:
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(config)
    consumer.subscribe([topic])
    return consumer


def _delivery_callback(err, msg):
    if err is not None:
        logger.error("Delivery failed [key=%s]: %s", msg.key(), err)
    else:
        logger.info("Produced message | topic=%s partition=%s offset=%s key=%s",
                     msg.topic(), msg.partition(), msg.offset(), msg.key())


def run_smoke_test(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "raw-telemetry",
    max_messages: int = 3,
    timeout: float = 5.0,
) -> int:
    """Produce `max_messages` events and consume them back, returning the count received."""
    logger.info("=== StreamForge End-to-End Smoke Test ===")
    logger.info("Broker: %s | Topic: %s | Target messages: %d", bootstrap_servers, topic, max_messages)

    test_events = [
        {
            "event_id": "smoke-001",
            "customer_id": "smoke_cust",
            "truck_id": "smoke_truck",
            "temperature": -18.5,
            "target_temp": -18.0,
            "compressor_status": "RUNNING",
        },
        {
            "event_id": "smoke-002",
            "customer_id": "smoke_cust",
            "truck_id": "smoke_truck",
            "temperature": -19.2,
            "target_temp": -18.0,
            "compressor_status": "RUNNING",
        },
        {
            "event_id": "smoke-003",
            "customer_id": "smoke_cust",
            "truck_id": "smoke_truck",
            "temperature": -17.5,
            "target_temp": -18.0,
            "compressor_status": "DEFROST",
        },
    ]

    producer = _build_producer(bootstrap_servers)
    consumer = _build_consumer(bootstrap_servers, topic, group_id="streamforge-smoke-e2e")

    produced = 0
    received = []

    try:
        # --- Phase 1: Produce ---
        logger.info("--- Phase 1: Producing %d test events ---", max_messages)
        for i in range(max_messages):
            event = test_events[i % len(test_events)]
            event["event_id"] = f"smoke-{i+1:03d}"
            event["timestamp"] = time.time()

            key = f"{event['customer_id']}:{event['truck_id']}"
            value = json.dumps(event)

            producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=_delivery_callback,
            )
            produced += 1
            logger.info("[PRODUCE #%d] key=%s payload=%s", produced, key, value)

        producer.flush(timeout=10.0)
        logger.info("Produced %d messages successfully.", produced)

        # --- Phase 2: Consume ---
        logger.info("--- Phase 2: Consuming messages ---")
        deadline = time.time() + timeout
        while len(received) < produced and time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            payload = msg.value().decode("utf-8") if isinstance(msg.value(), (bytes, bytearray)) else msg.value()

            try:
                decoded = json.loads(payload)
            except json.JSONDecodeError:
                decoded = {"raw_message": payload}

            logger.info("[CONSUME #%d] key=%s payload=%s", len(received) + 1, key, decoded)
            received.append(decoded)

        # --- Phase 3: Summary ---
        logger.info("--- Smoke Test Summary ---")
        logger.info("Produced: %d | Consumed: %d", produced, len(received))
        if produced == len(received):
            logger.info("SUCCESS: All messages round-tripped through Kafka.")
        else:
            logger.warning("MISMATCH: Produced %d but only consumed %d.", produced, len(received))

        return len(received)

    finally:
        consumer.close()
        producer.flush(timeout=2.0)


def main():
    parser = argparse.ArgumentParser(description="StreamForge end-to-end Kafka smoke test")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    parser.add_argument("--topic", type=str, default="raw-telemetry")
    parser.add_argument("--max-messages", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()

    count = run_smoke_test(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        max_messages=args.max_messages,
        timeout=args.timeout,
    )
    sys.exit(0 if count == args.max_messages else 1)


if __name__ == "__main__":
    main()
