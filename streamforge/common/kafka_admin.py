import logging
from typing import List, Optional
from .config import settings

logger = logging.getLogger("streamforge.kafka_admin")


def ensure_kafka_topics_sync(
    bootstrap_servers: Optional[str] = None,
    num_partitions: Optional[int] = None,
    replication_factor: Optional[int] = None,
) -> List[str]:
    """
    Creates required StreamForge Kafka topics using confluent-kafka AdminClient.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
    partitions = num_partitions or settings.KAFKA_NUM_PARTITIONS
    replication = replication_factor or settings.KAFKA_REPLICATION_FACTOR

    admin_client = AdminClient({"bootstrap.servers": servers})
    required_topics = [
        settings.KAFKA_RAW_TOPIC,
        settings.KAFKA_CHANGELOG_TOPIC,
        settings.KAFKA_PROCESSED_TOPIC,
        settings.KAFKA_ALERTS_TOPIC,
    ]

    try:
        metadata = admin_client.list_topics(timeout=10.0)
        existing_topics = set(metadata.topics.keys())
        logger.info("Existing Kafka topics: %s", sorted(list(existing_topics)))

        new_topics = [
            NewTopic(
                topic=topic,
                num_partitions=partitions,
                replication_factor=replication,
            )
            for topic in required_topics
            if topic not in existing_topics
        ]

        created_topics = []
        if new_topics:
            logger.info("Creating topics: %s (partitions=%d, replication=%d)", [t.topic for t in new_topics], partitions, replication)
            fs = admin_client.create_topics(new_topics)
            for topic, f in fs.items():
                try:
                    f.result(timeout=10.0)
                    created_topics.append(topic)
                    logger.info("Topic '%s' created successfully.", topic)
                except Exception as ex:
                    logger.warning("Topic '%s' creation note: %s", topic, ex)
        else:
            logger.info("All required topics already exist.")

        return created_topics
    except Exception as e:
        logger.error("Failed to ensure Kafka topics via confluent-kafka AdminClient: %s", e)
        raise


async def ensure_kafka_topics(
    bootstrap_servers: Optional[str] = None,
    num_partitions: Optional[int] = None,
    replication_factor: Optional[int] = None,
) -> List[str]:
    """
    Creates required StreamForge Kafka topics (async wrapper).
    """
    try:
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic
        servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        partitions = num_partitions or settings.KAFKA_NUM_PARTITIONS
        replication = replication_factor or settings.KAFKA_REPLICATION_FACTOR

        admin_client = AIOKafkaAdminClient(bootstrap_servers=servers)
        created_topics = []

        try:
            await admin_client.start()
            existing_topics = await admin_client.list_topics()
            logger.info("Existing Kafka topics: %s", existing_topics)

            required_topics = [
                settings.KAFKA_RAW_TOPIC,
                settings.KAFKA_CHANGELOG_TOPIC,
                settings.KAFKA_PROCESSED_TOPIC,
                settings.KAFKA_ALERTS_TOPIC,
            ]

            topics_to_create = [
                NewTopic(
                    name=topic,
                    num_partitions=partitions,
                    replication_factor=replication,
                )
                for topic in required_topics
                if topic not in existing_topics
            ]

            if topics_to_create:
                logger.info("Creating topics: %s with %d partitions", [t.name for t in topics_to_create], partitions)
                await admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
                created_topics = [t.name for t in topics_to_create]
                logger.info("Successfully created topics: %s", created_topics)
            else:
                logger.info("All required topics already exist.")

            return created_topics
        finally:
            await admin_client.close()
    except Exception:
        return ensure_kafka_topics_sync(
            bootstrap_servers=bootstrap_servers,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    try:
        ensure_kafka_topics_sync()
    except Exception as exc:
        logger.error("Error executing kafka admin setup: %s", exc)

