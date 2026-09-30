# Chaos Test — Faust Worker Restart Recovery

**Date:** 2026-09-27
**Branch:** akanksha
**Component:** streamforge-worker (Dockerized, RocksDB-backed state store)

## Test procedure
1. Started `streamforge-worker` container with `store='rocksdb://'` and a
   persistent Docker volume (`worker-state`) mounted at `/app/streamforge-data`.
2. Published 20 telemetry events via the producer.
3. Confirmed worker logged running window stats (`n=1`, `n=2`...) per truck.
4. Force-restarted the container: `docker restart streamforge-worker`.
5. Waited for the worker to rejoin the Kafka consumer group and reach
   "Worker ready".
6. Published 10 more events for trucks seen before the restart.

## Result
[Fill in: e.g. "Worker recovered its window state from the RocksDB volume —
subsequent events for previously-seen trucks continued their running count
(n=2, n=3...) rather than resetting to n=1, confirming state survived the
container restart."]

## Why this matters
This demonstrates the fault-tolerance guarantee described in the project
scope: if a worker crashes, another (or the same, restarted) worker can
resume processing without losing accumulated state — the core promise of
using a durable, changelog-backed state store (RocksDB + Kafka) instead of
in-memory state.