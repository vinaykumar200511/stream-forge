# Problem Statement

## 1. Business problem

FleetPulse manages refrigerated transport for temperature-sensitive cargo such as vaccines, dairy, frozen foods, and fresh produce. These assets are highly sensitive to thermal drift. If a trailer's compressor or refrigeration system fails, the load can exceed safe temperature limits within minutes, creating spoilage risk, customer claims, and regulatory exposure.

The current operational model relies on nightly batch processing. That creates a delay of roughly 12 hours between a truck deviating from its safe temperature band and the fleet operator becoming aware of it. In a cold-chain environment, that delay is too slow to prevent loss.

## 2. Operational impact

- Product spoilage before intervention.
- Lost revenue from damaged inventory.
- Additional costs from claims, replacements, and service disruption.
- Poor customer trust when temperature-sensitive deliveries fail.

## 3. Requirements for the solution

The required system must:

- consume high-volume telemetry from thousands of trucks in real time;
- aggregate temperature behavior over short windows, such as five minutes;
- detect threshold breaches and fault events immediately;
- tolerate delayed or out-of-order telemetry packets;
- keep state resilient after worker failures;
- expose a live operational view for fleet monitoring.

## 4. Why StreamForge

StreamForge addresses this by processing telemetry as a continuous event stream instead of a delayed batch. It maintains local state per vehicle, computes rolling averages and anomalies in near real time, and ensures recovery without losing data by replaying Kafka changelog state. This turns a slow postmortem process into a proactive operational control loop.
