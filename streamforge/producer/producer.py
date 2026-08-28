import argparse
import logging
import signal
import sys
import time
from typing import Callable, Dict, List, Optional

from confluent_kafka import KafkaError, Message, Producer

from streamforge.common.config import settings
from streamforge.common.models import RawTelemetryEvent
from streamforge.producer.simulator import TelemetrySimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("streamforge.producer")


# --- Broker connectivity probe -----------------------------------------

def check_broker_reachable(bootstrap_servers: str, timeout: float = 5.0) -> bool:
    """
    Performs a metadata fetch against the Kafka broker to verify connectivity
    before attempting to stream. Returns True if at least one broker responds.
    """
    from confluent_kafka import Consumer
    probe = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "_streamforge_probe",
        "session.timeout.ms": int(timeout * 1000),
        "socket.timeout.ms": int(timeout * 1000),
        "api.version.request.timeout.ms": int(timeout * 1000),
    })
    try:
        metadata = probe.list_topics(timeout=timeout)
        return len(metadata.brokers) > 0
    except Exception:
        return False
    finally:
        probe.close()


# -----------------------------------------------------------------------

class StreamForgeProducer:
    """
    High-throughput confluent-kafka event publisher for multi-tenant IoT telemetry.
    Supports continuous streaming, fine-grained rate control, delivery verification,
    pre-flight broker connectivity checks, and circuit-breaker error handling.
    """

    # Abort stream if error rate exceeds this fraction of total messages
    ERROR_RATE_CIRCUIT_BREAKER = 0.50
    # Minimum events before circuit-breaker can trip
    CIRCUIT_BREAKER_MIN_EVENTS = 50
    # Log at most this many delivery errors per stat window (suppress duplicates)
    MAX_ERROR_LOGS_PER_WINDOW = 3

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        simulator: Optional[TelemetrySimulator] = None,
        rate_per_sec: int = 100,
        flush_timeout: float = 10.0,
        producer_config: Optional[Dict] = None,
    ):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or settings.KAFKA_RAW_TOPIC
        self.simulator = simulator or TelemetrySimulator()
        self.rate_per_sec = max(1, rate_per_sec)
        self.flush_timeout = flush_timeout
        
        default_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "streamforge-telemetry-producer",
            "compression.type": "gzip",
            "linger.ms": 10,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 100000,
            "acks": "1",
            # Fail fast (5s) so we don't silently queue thousands before reporting errors
            "delivery.timeout.ms": 5000,
            "message.timeout.ms": 5000,
        }
        if producer_config:
            default_config.update(producer_config)
            
        self.config = default_config
        self.producer: Optional[Producer] = None
        self.is_running = False
        
        # Performance & telemetry statistics
        self.total_published = 0
        self.total_delivered = 0
        self.total_errors = 0
        self._error_logs_this_window = 0
        self.start_time = 0.0

    def start(self, probe_timeout: float = 5.0) -> None:
        """
        Probe broker connectivity, then initialize the confluent-kafka Producer.
        Raises RuntimeError immediately if the broker is unreachable.
        """
        logger.info("Probing Kafka broker at %s (timeout=%.1fs)...", self.bootstrap_servers, probe_timeout)
        if not check_broker_reachable(self.bootstrap_servers, timeout=probe_timeout):
            raise RuntimeError(
                f"\n\n"
                f"  \u2717  Kafka broker is UNREACHABLE at: {self.bootstrap_servers}\n"
                f"\n"
                f"  To start Kafka locally, run one of:\n"
                f"    docker-compose up -d kafka          (via docker-compose.yml)\n"
                f"    docker run -d -p 9092:9092 apache/kafka:latest\n"
                f"\n"
                f"  To test without a broker, use:\n"
                f"    python -m streamforge.producer.producer --dry-run\n"
            )
        logger.info("\u2713 Broker reachable. Initializing confluent-kafka producer for topic '%s'...", self.topic)
        self.producer = Producer(self.config)
        logger.info("Confluent-kafka Producer ready.")

    def stop(self, timeout: Optional[float] = None) -> None:
        """Gracefully stop producer, flushing all remaining queued messages."""
        self.is_running = False
        timeout = timeout if timeout is not None else self.flush_timeout
        if self.producer:
            queued = len(self.producer)
            if queued > 0:
                logger.info("Flushing %d pending messages (timeout=%.1fs)...", queued, timeout)
            remaining = self.producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning(
                    "%d messages remained un-flushed after %.1fs — broker may be unreachable.",
                    remaining, timeout,
                )
            else:
                logger.info("All messages successfully flushed and acknowledged.")
            self.producer = None

    def _delivery_callback(self, err: Optional[KafkaError], msg: Message) -> None:
        """Callback invoked once a message is acknowledged (or fails) at the broker."""
        if err is not None:
            self.total_errors += 1
            # Throttle error log output to avoid flooding the console
            if self._error_logs_this_window < self.MAX_ERROR_LOGS_PER_WINDOW:
                logger.error(
                    "Delivery failed [key=%s]: %s",
                    msg.key().decode("utf-8") if msg.key() else "?",
                    err,
                )
                self._error_logs_this_window += 1
            elif self._error_logs_this_window == self.MAX_ERROR_LOGS_PER_WINDOW:
                logger.error("(Further delivery errors suppressed until next stats window — see metrics log)")
                self._error_logs_this_window += 1
        else:
            self.total_delivered += 1

    def send_event(
        self,
        event: RawTelemetryEvent,
        on_delivery: Optional[Callable] = None,
    ) -> None:
        """
        Serialize and produce a single telemetry event to Kafka,
        partitioned by customer_id:truck_id.
        """
        if not self.producer:
            raise RuntimeError("Producer has not been started. Call producer.start() first.")

        key_bytes = event.kafka_key.encode("utf-8")
        value_bytes = event.model_dump_json().encode("utf-8")

        try:
            self.producer.produce(
                topic=self.topic,
                key=key_bytes,
                value=value_bytes,
                on_delivery=on_delivery or self._delivery_callback,
            )
        except BufferError:
            # Producer queue is full — flush a little and retry once
            self.producer.poll(0.1)
            self.producer.produce(
                topic=self.topic,
                key=key_bytes,
                value=value_bytes,
                on_delivery=on_delivery or self._delivery_callback,
            )

        self.total_published += 1
        # Non-blocking poll to serve delivery callbacks
        self.producer.poll(0)

    def set_rate(self, new_rate: int) -> None:
        """Dynamically adjust message generation rate at runtime."""
        self.rate_per_sec = max(1, new_rate)
        logger.info("Producer rate dynamically updated to %d ev/s.", self.rate_per_sec)

    def _check_circuit_breaker(self) -> bool:
        """
        Returns True (abort) if error rate exceeds threshold after minimum events.
        Prevents streaming silently into a dead broker for minutes.
        """
        if self.total_published < self.CIRCUIT_BREAKER_MIN_EVENTS:
            return False
        error_rate = self.total_errors / self.total_published
        if error_rate >= self.ERROR_RATE_CIRCUIT_BREAKER:
            logger.critical(
                "\n"
                "  \u2717  CIRCUIT BREAKER TRIPPED \u2014 %.0f%% delivery failure rate (threshold: %.0f%%)\n"
                "     Published: %d | Delivered: %d | Errors: %d\n"
                "     Kafka broker at '%s' is not acknowledging messages.\n"
                "     Aborting stream. Start Kafka or use --dry-run for offline testing.",
                error_rate * 100,
                self.ERROR_RATE_CIRCUIT_BREAKER * 100,
                self.total_published,
                self.total_delivered,
                self.total_errors,
                self.bootstrap_servers,
            )
            return True
        return False

    def run_stream(
        self,
        max_events: Optional[int] = None,
        duration_seconds: Optional[float] = None,
    ) -> None:
        """
        Stream events continuously with precise, configurable rate control.
        Aborts early if the circuit breaker detects a dead broker.
        """
        self.is_running = True
        self.start_time = time.time()
        logger.info(
            "Starting telemetry stream: rate=%d ev/s | trucks=%d | max_events=%s | duration=%s",
            self.rate_per_sec,
            self.simulator.total_trucks,
            max_events or "unlimited",
            f"{duration_seconds}s" if duration_seconds else "unlimited",
        )

        batch_interval = 0.05   # 50ms pacing slices
        last_stat_time = time.time()
        last_stat_published = 0

        try:
            while self.is_running:
                step_start = time.time()
                current_duration = step_start - self.start_time

                if duration_seconds and current_duration >= duration_seconds:
                    logger.info("Target duration of %.1fs reached. Ending stream.", duration_seconds)
                    break

                # Calculate batch for this interval
                batch_size = max(1, int(self.rate_per_sec * batch_interval))
                if max_events and (self.total_published + batch_size) > max_events:
                    batch_size = max_events - self.total_published
                if batch_size <= 0:
                    break

                events: List[RawTelemetryEvent] = self.simulator.generate_batch(batch_size=batch_size)
                for ev in events:
                    self.send_event(ev)

                if max_events and self.total_published >= max_events:
                    logger.info("Reached max event count: %d. Ending stream.", max_events)
                    break

                # Periodic performance metrics log (every 5 seconds)
                now = time.time()
                if now - last_stat_time >= 5.0:
                    elapsed = now - last_stat_time
                    delta = self.total_published - last_stat_published
                    current_rate = delta / elapsed
                    overall_rate = self.total_published / (now - self.start_time)
                    delivery_pct = (
                        100.0 * self.total_delivered / self.total_published
                        if self.total_published > 0 else 0.0
                    )
                    logger.info(
                        "Stream Metrics | Published=%d | Delivered=%d (%.0f%%) | Errors=%d | "
                        "Current: %.1f ev/s | Overall: %.1f ev/s",
                        self.total_published,
                        self.total_delivered,
                        delivery_pct,
                        self.total_errors,
                        current_rate,
                        overall_rate,
                    )
                    last_stat_time = now
                    last_stat_published = self.total_published
                    # Reset throttle counter for the next window
                    self._error_logs_this_window = 0

                    # Circuit-breaker check after each stats window
                    if self._check_circuit_breaker():
                        self.is_running = False
                        break

                # High-precision rate-limiter
                time_spent = time.time() - step_start
                sleep_needed = max(0.0, batch_interval - time_spent)
                if sleep_needed > 0:
                    time.sleep(sleep_needed)

        except KeyboardInterrupt:
            logger.info("Stream interrupted by user (Ctrl+C).")
        finally:
            self.is_running = False
            total_time = time.time() - self.start_time
            overall_rate = self.total_published / total_time if total_time > 0 else 0.0
            delivery_pct = (
                100.0 * self.total_delivered / self.total_published
                if self.total_published > 0 else 0.0
            )
            logger.info(
                "Stream finished. Published=%d | Delivered=%d (%.0f%%) | Errors=%d | "
                "Duration=%.2fs | Avg=%.1f ev/s",
                self.total_published,
                self.total_delivered,
                delivery_pct,
                self.total_errors,
                total_time,
                overall_rate,
            )


def run_dry_run(
    simulator: TelemetrySimulator,
    count: int = 10,
    rate_per_sec: int = 100,
    continuous: bool = False,
) -> None:
    """Print simulated events locally with rate control — no Kafka broker required."""
    label = "continuous" if continuous else str(count)
    logger.info("--- Dry Run | Rate=%d ev/s | Count=%s ---", rate_per_sec, label)
    generated = 0
    start = time.time()
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.01

    try:
        while continuous or (generated < count):
            step_start = time.time()
            batch = simulator.generate_batch(batch_size=1)
            if not batch:
                logger.info("[DROPPED] Simulated packet loss — telemetry event dropped")
                continue
            event = batch[0]
            generated += 1
            logger.info(
                "[%d%s] %-20s | Temp=%6.2f\u00b0C (target %6.2f\u00b0C) | %-7s | Bat=%5.1f%% | Pos=(%.4f, %.4f)",
                generated,
                f"/{count}" if not continuous else "",
                event.kafka_key,
                event.temperature,
                event.target_temp,
                event.compressor_status.value,
                event.battery_level,
                event.latitude,
                event.longitude,
            )
            spent = time.time() - step_start
            sleep_needed = max(0.0, interval - spent)
            if sleep_needed > 0:
                time.sleep(sleep_needed)
    except KeyboardInterrupt:
        logger.info("Dry run interrupted by user.")

    elapsed = time.time() - start
    logger.info(
        "--- Dry Run complete: %d events in %.2fs (%.1f ev/s) ---",
        generated, elapsed, (generated / elapsed) if elapsed > 0 else 0,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="StreamForge \u2014 Multi-Tenant IoT Telemetry Confluent-Kafka Producer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--bootstrap-servers", type=str, default=settings.KAFKA_BOOTSTRAP_SERVERS,
                        help="Kafka bootstrap broker address(es)")
    parser.add_argument("--topic", type=str, default=settings.KAFKA_RAW_TOPIC,
                        help="Target Kafka topic")
    parser.add_argument("--rate", type=int, default=settings.SIMULATION_RATE_PER_SEC,
                        help="Target events per second")
    parser.add_argument("--count", type=int, default=None,
                        help="Total events to publish then exit (default: unlimited)")
    parser.add_argument("--duration", type=float, default=None,
                        help="Seconds to run then exit (default: unlimited)")
    parser.add_argument("--trucks", type=int, default=settings.SIMULATION_NUM_TRUCKS,
                        help="Number of simulated trucks in fleet")
    parser.add_argument("--customers", type=int, default=settings.SIMULATION_NUM_CUSTOMERS,
                        help="Number of simulated fleet tenants")
    parser.add_argument("--anomalies", type=float, default=0.03,
                        help="Anomaly injection probability [0.0 \u2013 1.0]")
    parser.add_argument("--late-rate", type=float, default=0.05,
                        help="Late-arriving timestamp delay probability [0.0 \u2013 1.0]")
    parser.add_argument("--drop-rate", type=float, default=0.0,
                        help="Message packet loss / drop probability [0.0 \u2013 1.0]")
    parser.add_argument("--flush-timeout", type=float, default=10.0,
                        help="Seconds to wait for broker flush on shutdown")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate events locally with rate pacing \u2014 no Kafka required")
    args = parser.parse_args()

    simulator = TelemetrySimulator(
        num_customers=args.customers,
        num_trucks=args.trucks,
        anomaly_rate=args.anomalies,
        late_event_rate=args.late_rate,
        drop_rate=args.drop_rate,
    )

    if args.dry_run:
        run_dry_run(
            simulator=simulator,
            count=args.count or 20,
            rate_per_sec=args.rate,
            continuous=(args.count is None and args.duration is None),
        )
        return

    producer = StreamForgeProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        simulator=simulator,
        rate_per_sec=args.rate,
        flush_timeout=args.flush_timeout,
    )

    def handle_signal(sig, frame):
        logger.info("Signal %s received \u2014 stopping producer...", sig)
        producer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        producer.start()
        producer.run_stream(max_events=args.count, duration_seconds=args.duration)
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    finally:
        producer.stop()


if __name__ == "__main__":
    main()
