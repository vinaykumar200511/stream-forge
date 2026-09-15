import argparse
import json
import logging
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaException

from streamforge.common.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("streamforge.consumer")


class RawKafkaConsumer:
    """Minimal raw Kafka consumer used to smoke-test message flow end-to-end."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        consumer_config: Optional[Dict[str, Any]] = None,
        auto_offset_reset: str = "earliest",
    ):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or settings.KAFKA_RAW_TOPIC
        self.group_id = group_id or f"streamforge-{self.topic}-smoke"

        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": True,
        }
        if consumer_config:
            config.update(consumer_config)

        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])
        logger.info("Listening for Kafka messages on topic '%s' at %s", self.topic, self.bootstrap_servers)

    def consume_once(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Poll once and return the decoded payload if a message is available."""
        message = self.consumer.poll(timeout)
        if message is None:
            return None

        if message.error():
            raise KafkaException(message.error())

        value = message.value()
        if value is None:
            logger.warning("Received empty message on topic '%s'", self.topic)
            return None

        payload = value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else value
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            logger.info("Non-JSON payload received on topic '%s': %s", self.topic, payload)
            return {"raw_message": payload}

        key = message.key().decode("utf-8") if message.key() is not None else None
        print(
            f"[Kafka] topic={self.topic} key={key} payload={decoded}"
        )
        logger.info("Consumed message from topic '%s' with key '%s': %s", self.topic, key, decoded)
        return decoded

    def close(self) -> None:
        self.consumer.close()

    def consume_loop(self, max_messages: Optional[int] = 5, timeout: float = 1.0):
        """Consume up to max_messages and return the list of decoded payloads."""
        messages = []
        while max_messages is None or len(messages) < max_messages:
            payload = self.consume_once(timeout=timeout)
            if payload is None:
                continue
            messages.append(payload)
            if max_messages is not None and len(messages) >= max_messages:
                break
        return messages


def run_smoke_test(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    group_id: Optional[str] = None,
    max_messages: int = 1,
    timeout: float = 1.0,
) -> int:
    """Consume a bounded number of messages and return the count received."""
    consumer = RawKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
    )
    try:
        count = 0
        while count < max_messages:
            payload = consumer.consume_once(timeout=timeout)
            if payload is None:
                continue
            count += 1
            print(f"[Smoke Test] Received message #{count} from {topic or settings.KAFKA_RAW_TOPIC}: {payload}")
        return count
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Basic raw Kafka consumer smoke test")
    parser.add_argument("--bootstrap-servers", type=str, default=settings.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", type=str, default=settings.KAFKA_RAW_TOPIC)
    parser.add_argument("--group-id", type=str, default=None)
    parser.add_argument("--max-messages", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=1.0)
    args = parser.parse_args()

    print(f"Starting raw Kafka consumer smoke test on {args.topic}...")
    count = run_smoke_test(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        max_messages=args.max_messages,
        timeout=args.timeout,
    )
    print(f"Smoke test complete. Consumed {count} message(s).")