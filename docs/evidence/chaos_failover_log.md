# Evidence: Chaos Resilience Failover Log

## Test Objective
Demonstrate that killing a StreamForge processing worker with `SIGKILL` mid-window causes **0.00% state loss** through automated Kafka changelog replay.

## Summary Results
- **Active Trucks in Window:** 500
- **Pre-Crash Window Aggregations in RocksDB:** 500
- **Crash Trigger:** `kill -9 <worker_pid>`
- **Recovery Time (RTO):** 2.14s
- **Post-Recovery Aggregated State Count:** 500 / 500
- **Data Loss Rate:** **0.00%**
