import argparse
import asyncio
import logging
import signal
import sys
import time
from typing import Optional

from aiokafka import AIOKafkaProducer

from streamforge.common.config import settings
from streamforge.common.models import RawTelemetryEvent
from streamforge.producer.simulator import TelemetrySimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("streamforge.producer")


class StreamForgeProducer:
    """
    High-throughput async Kafka event publisher for multi-tenant IoT telemetry.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        simulator: Optional[TelemetrySimulator] = None,
        rate_per_sec: int = 100,
    ):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or settings.KAFKA_RAW_TOPIC
        self.simulator = simulator or TelemetrySimulator()
        self.rate_per_sec = rate_per_sec
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_running = False
        self.total_published = 0
        self.start_time = 0.0

    async def start(self) -> None:
        """Initialize and connect the AIOKafkaProducer."""
        logger.info("Connecting Kafka producer to %s...", self.bootstrap_servers)
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            acks=1,  # Fast acknowledgement for high-throughput stream ingestion
            compression_type="gzip",
            linger_ms=10,  # Batch buffering
            batch_size=32 * 1024,
        )
        await self.producer.start()
        logger.info("Kafka Producer successfully started on topic '%s'.", self.topic)

    async def stop(self) -> None:
        """Gracefully stop producer and flush buffers."""
        self.is_running = False
        if self.producer:
            logger.info("Flushing and shutting down Kafka producer...")
            await self.producer.stop()
            logger.info("Kafka producer stopped.")

    async def send_event(self, event: RawTelemetryEvent) -> None:
        """Send an individual event to Kafka partitioned by customer_id:truck_id."""
        if not self.producer:
            raise RuntimeError("Producer has not been started.")
        await self.producer.send(
            topic=self.topic,
            key=event.kafka_key,
            value=event,
        )
        self.total_published += 1

    async def run_stream(self, max_events: Optional[int] = None) -> None:
        """
        Stream events continuously at the target rate per second.
        """
        self.is_running = True
        self.start_time = time.time()
        logger.info(
            "Starting telemetry stream: rate=%d ev/s across %d trucks (max_events=%s)...",
            self.rate_per_sec,
            self.simulator.total_trucks,
            max_events or "unlimited",
        )

        batch_interval = 0.1  # emit in 100ms mini-batches for smooth throughput
        batch_size = max(1, int(self.rate_per_sec * batch_interval))

        last_stat_time = time.time()
        last_stat_count = 0

        try:
            while self.is_running:
                step_start = time.time()
                events = self.simulator.generate_batch(batch_size=batch_size)

                # Send batch asynchronously
                send_tasks = [self.send_event(ev) for ev in events]
                await asyncio.gather(*send_tasks)

                if max_events and self.total_published >= max_events:
                    logger.info("Reached maximum target event count: %d. Exiting stream.", max_events)
                    break

                # Periodic throughput metrics log
                now = time.time()
                if now - last_stat_time >= 5.0:
                    elapsed = now - last_stat_time
                    delta = self.total_published - last_stat_count
                    current_rate = delta / elapsed
                    overall_rate = self.total_published / (now - self.start_time)
                    logger.info(
                        "Stats: Published %d events (Current: %.1f ev/s | Overall: %.1f ev/s)",
                        self.total_published,
                        current_rate,
                        overall_rate,
                    )
                    last_stat_time = now
                    last_stat_count = self.total_published

                # Rate limiting sleep
                spent = time.time() - step_start
                sleep_needed = max(0.0, batch_interval - spent)
                if sleep_needed > 0:
                    await asyncio.sleep(sleep_needed)

        except asyncio.CancelledError:
            logger.info("Stream cancelled.")
        finally:
            self.is_running = False


async def run_dry_run(simulator: TelemetrySimulator, count: int = 10) -> None:
    """Print sample simulated events without requiring a live Kafka broker."""
    logger.info("--- Dry Run: Generating %d Sample Telemetry Events ---", count)
    for idx in range(count):
        event = simulator.generate_batch(batch_size=1)[0]
        logger.info(
            "[%d/%d] Key=%-18s | Temp=%6.2f°C (Target: %6.2f°C) | Status=%-7s | Bat=%5.1f%% | Spike=%s",
            idx + 1,
            count,
            event.kafka_key,
            event.temperature,
            event.target_temp,
            event.compressor_status.value,
            event.battery_level,
            "YES" if event.compressor_status == "FAULT" or event.temperature > event.target_temp + 3.0 else "NO",
        )
    logger.info("--- Dry Run Completed Successfully ---")


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamForge Multi-Tenant IoT Telemetry Producer")
    parser.add_argument("--rate", type=int, default=settings.SIMULATION_RATE_PER_SEC, help="Events per second")
    parser.add_argument("--count", type=int, default=None, help="Total events to publish before exiting")
    parser.add_argument("--trucks", type=int, default=settings.SIMULATION_NUM_TRUCKS, help="Fleet size")
    parser.add_argument("--customers", type=int, default=settings.SIMULATION_NUM_CUSTOMERS, help="Tenant count")
    parser.add_argument("--anomalies", type=float, default=0.03, help="Anomaly injection probability (0.0 - 1.0)")
    parser.add_argument("--dry-run", action="store_true", help="Generate and print events locally without Kafka")
    args = parser.parse_args()

    simulator = TelemetrySimulator(
        num_customers=args.customers,
        num_trucks=args.trucks,
        anomaly_rate=args.anomalies,
    )

    if args.dry_run:
        asyncio.run(run_dry_run(simulator, count=args.count or 20))
        return

    producer = StreamForgeProducer(
        simulator=simulator,
        rate_per_sec=args.rate,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(producer.stop()))
        except NotImplementedError:
            # Signal handling on Windows event loop fallback
            pass

    async def run():
        try:
            await producer.start()
            await producer.run_stream(max_events=args.count)
        finally:
            await producer.stop()

    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
