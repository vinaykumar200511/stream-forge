import logging
from typing import List
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from .config import settings

logger = logging.getLogger("streamforge.kafka_admin")


async def ensure_kafka_topics(
    bootstrap_servers: str = None,
    num_partitions: int = None,
    replication_factor: int = None,
) -> List[str]:
    """
    Creates required StreamForge Kafka topics if they do not already exist.
    """
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

        topics_to_create = []
        for topic in required_topics:
            if topic not in existing_topics:
                topics_to_create.append(
                    NewTopic(
                        name=topic,
                        num_partitions=partitions,
                        replication_factor=replication,
                    )
                )

        if topics_to_create:
            logger.info("Creating topics: %s with %d partitions", [t.name for t in topics_to_create], partitions)
            await admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            created_topics = [t.name for t in topics_to_create]
            logger.info("Successfully created topics: %s", created_topics)
        else:
            logger.info("All required topics already exist.")

        return created_topics
    except Exception as e:
        logger.error("Failed to ensure Kafka topics: %s", e)
        raise
    finally:
        await admin_client.close()


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(ensure_kafka_topics())
