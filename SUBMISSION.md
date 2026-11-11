# Submission, Real-time Streaming Backend

**Your name:** Icham Lecorvaisier
**Email:** icham.lecorvaisier@gmail.com
**Link to your fork or solution:** https://github.com/ichoupinou/careers-challenge-streaming-backend

---

## Stack and storage

One Rust process (tokio + axum), event-sourced. Accepted events go to an append-only, CRC-framed **write-ahead log**, are applied in log order to in-memory state, and are snapshotted periodically. One machine handles 50k ev/s, and a single process keeps ordering, dedup and recovery easy to reason about. There is no Kafka or database: the WAL already *is* the durable, ordered, replayable log, and every query is served from memory. Transport is HTTP (`POST /events`, single or batched) and SSE for alarms, plus long-poll on `/alarms`.

## Ordering and late events

Every aggregate depends only on the **set** of accepted events, keyed by `ts`, never on arrival order (a property test checks this). Late events therefore need no fix-up code. Heartbeats set bits in a per-device bitmap with one bit per second. Room presence is kept as a `ts`-ordered history, and the 1m/5m/1h windows are calculated from it at read time, so a replayed event is just one more insert. Falls are deduplicated on exact `(device_id, ts)`, the signature of sensor jitter. Events outside ±1h are rejected with a reason.

## Backpressure

`fall_warn` has its own lane, which the WAL writer always drains first. A 202 means the event is in the WAL and applied. A full lane makes the request wait, then returns an explicit **503** with `Retry-After`. Nothing is dropped silently, and retries are safe because applying an event twice is a no-op.

## Restart correctness

On startup the service loads the newest valid snapshot (written atomically, two kept) and replays the WAL after it. A torn tail is truncated. A gap in offsets stops startup instead of serving incomplete state. Alarm ids are assigned in WAL order, so SSE clients resume with `Last-Event-ID` and receive every alarm from the gap.

## How to run it locally

```bash
make run              # service on :8080, data in ./data (needs a Rust toolchain)
make smoke            # or baseline / burst / offline / adversarial (make clean-data between runs)
make kill-test-full   # grader-shaped: 5k devices, 5 min, 2×10x bursts, 20% offline 60s, kill -9 at 180s
```

Details: [`service/README.md`](service/README.md).

## Reported metrics

`make kill-test-full`, on an Apple M2 laptop (8 GB), with the load generator on the same machine:

- **Sustained ingest rate:** 50.7k ev/s during both 30s bursts; 4.26M events, 0 rejected for overload (503), 0 lost. POST→202 latency p95 9.6 ms.
- **Alarm feed latency p50 / p95:** 0.53 ms / 11.6 ms from POST to SSE receipt (p99 21 ms, n = 55,371).
- **Behavior under hard kill + restart:** back in 0.30s (snapshot plus 45k WAL events in 241 ms). An independent SSE consumer saw all 55,371 alarms with no gaps and no duplicates.
- **Aggregation correctness on replayed events:** 62k buffered events replayed around the kill. Afterwards the recovered state was compared with a ground truth recomputed from every generated event: **7,503/7,503** occupancy windows (1m/5m/1h, all rooms), 2,501/2,501 `in_room`, 501/501 sampled device health. Alarms == distinct falls. Device 5,001 is visible without a redeploy.

## With another week

The single applier thread is the ceiling. For hundreds of thousands of devices I would shard by `device_id`, giving each shard its own WAL and applier, and merge room histories at read time. The "function of the event set" property carries over, so recovery and late-event logic stay the same. I would also harden dedup against jittered fall timestamps; a ±window mode exists but is off by default.
