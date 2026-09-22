"""Cached Kafka consumer-group and partition-lag metrics collection."""

import asyncio
import logging
import threading
from datetime import datetime, timezone
from typing import Any

from streamforge.common.config import settings

logger = logging.getLogger("streamforge.kafka_metrics")


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class KafkaMetricsService:
    """Collect Kafka offsets in a worker thread and serve the last good snapshot."""

    def __init__(self) -> None:
        self._snapshot: dict[str, Any] = {
            "status": "unavailable",
            "groups": [],
            "timestamp": _timestamp(),
            "error": "Kafka metrics have not been collected yet",
        }
        self._lock = threading.Lock()
        self._admin = None
        self._consumer = None
        self._task: asyncio.Task[None] | None = None

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._snapshot)

    def collect_sync(self) -> dict[str, Any]:
        """Collect all discoverable groups using reusable Kafka clients."""
        try:
            from confluent_kafka import Consumer, TopicPartition
            from confluent_kafka.admin import AdminClient

            if self._admin is None:
                self._admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
            if self._consumer is None:
                self._consumer = Consumer({
                    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                    "group.id": "streamforge-metrics-reader",
                    "enable.auto.commit": False,
                })

            groups_result = self._admin.list_consumer_groups()
            groups = []
            for item in getattr(groups_result, "valid", []) or []:
                group_id = getattr(item, "group_id", None) or getattr(item, "id", None)
                if group_id:
                    groups.append(group_id)
            if settings.KAFKA_METRICS_GROUPS:
                groups = sorted(set(groups) | set(settings.KAFKA_METRICS_GROUPS.split(",")))

            metadata = self._consumer.list_topics(timeout=settings.KAFKA_METRICS_TIMEOUT_SECONDS)
            descriptions = self._admin.describe_consumer_groups(groups) if groups else {}
            discovered: list[dict[str, Any]] = []
            for group_id in groups:
                description = descriptions.get(group_id)
                group_info = description.result(timeout=settings.KAFKA_METRICS_TIMEOUT_SECONDS) if description else None
                discovered.append(self._collect_group(group_id, metadata, TopicPartition, group_info))

            snapshot = {"status": "healthy", "groups": discovered, "timestamp": _timestamp()}
        except Exception as exc:
            logger.warning("Kafka metrics collection failed: %s", exc)
            snapshot = {"status": "unavailable", "groups": [], "timestamp": _timestamp(), "error": str(exc)}

        with self._lock:
            self._snapshot = snapshot
        return snapshot

    def _collect_group(self, group_id: str, metadata: Any, topic_partition_type: Any, group_info: Any = None) -> dict[str, Any]:
        topic_partitions = []
        for topic, topic_metadata in getattr(metadata, "topics", {}).items():
            for partition in getattr(topic_metadata, "partitions", {}):
                topic_partitions.append(topic_partition_type(topic, partition))

        offsets_result = self._admin.list_consumer_group_offsets(group_id, topic_partitions)
        offsets = offsets_result.result(timeout=settings.KAFKA_METRICS_TIMEOUT_SECONDS)
        topics: dict[str, dict[str, Any]] = {}
        for topic_partition in topic_partitions:
            committed = offsets.get(topic_partition)
            current_offset = getattr(committed, "offset", -1) if committed else -1
            _, latest_offset = self._consumer.get_watermark_offsets(
                topic_partition, timeout=settings.KAFKA_METRICS_TIMEOUT_SECONDS
            )
            current_offset = max(0, current_offset)
            lag = max(0, latest_offset - current_offset)
            topics.setdefault(topic_partition.topic, {"topic": topic_partition.topic, "partitions": []})["partitions"].append({
                "partition": topic_partition.partition,
                "currentOffset": current_offset,
                "latestOffset": latest_offset,
                "lag": lag,
                "broker": self._broker_for_partition(metadata, topic_partition),
            })

        partitions = [partition for topic in topics.values() for partition in topic["partitions"]]
        members = getattr(group_info, "members", []) if group_info else []
        assigned_partitions = sum(
            len(getattr(getattr(member, "assignment", None), "topic_partitions", []) or [])
            for member in members
        )
        return {
            "consumerGroup": group_id,
            "state": getattr(group_info, "state", "unknown") if group_info else "unknown",
            "members": len(members),
            "assignedPartitions": assigned_partitions,
            "topics": list(topics.values()),
            "totalLag": sum(item["lag"] for item in partitions),
            "timestamp": _timestamp(),
        }

    @staticmethod
    def _broker_for_partition(metadata: Any, topic_partition: Any) -> int | None:
        topic = getattr(metadata, "topics", {}).get(topic_partition.topic)
        partition = getattr(topic, "partitions", {}).get(topic_partition.partition) if topic else None
        replicas = getattr(partition, "replicas", []) if partition else []
        return replicas[0] if replicas else None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._poll())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self._consumer:
            self._consumer.close()
            self._consumer = None

    async def _poll(self) -> None:
        while True:
            await asyncio.to_thread(self.collect_sync)
            await asyncio.sleep(settings.KAFKA_METRICS_POLL_INTERVAL_SECONDS)


kafka_metrics_service = KafkaMetricsService()