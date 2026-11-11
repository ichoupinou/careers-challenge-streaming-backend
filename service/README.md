# stream-service

A single Rust process, event-sourced. Every accepted event is appended to a
write-ahead log, applied in log order to in-memory state, and snapshotted
periodically. See [`SUBMISSION.md`](../SUBMISSION.md) for the reasoning and
measured numbers.

```text
POST /events ─► validate ─► fall lane ─┐  writer drains falls first
                         └► bulk lane ─┴─► WAL writer (group commit, fsync policy)
                                               │
                                               ▼
                                         applier: State::apply in WAL order ─► 202
                                               │
                    snapshotter (every SNAPSHOT_SECS) ◄─ clone under a short read lock
                    alarm watch ─► SSE / long-poll subscribers read from the alarm store
```

A 202 means the events are in the WAL (they survive `kill -9`) and visible to
reads. A full lane makes the request wait up to `ENQUEUE_TIMEOUT_MS`, then
return `503` with `Retry-After`. Events are never dropped silently, and
retrying is safe because applying an event twice is a no-op.

## Layout

| File | What it does |
|---|---|
| `event.rs` | event model, validation, reject reasons (±1h `ts` bounds) |
| `ingest.rs` | priority lanes → WAL writer → applier |
| `wal.rs` | segmented, CRC-framed append-only log; torn-tail truncation |
| `snapshot.rs` | atomic snapshots (tmp + fsync + rename), two kept; WAL GC |
| `app.rs` | recovery (snapshot + WAL tail) and wiring |
| `state/` | read model: `device` + `ring` (health), `room` (occupancy), `falls` (dedup), `alarms` |
| `api.rs` | HTTP endpoints |
| `metrics.rs` | Prometheus counters/histograms and a 10s stats log line |
| `bin/loadgen.rs` | load generator and alarm-latency probe (`--features loadgen`) |

## Endpoints

| Endpoint | |
|---|---|
| `POST /events` | one event or a JSON array → `202 {accepted, rejected[]}`; `503` when overloaded |
| `GET /devices/{id}/health` | latest heartbeat, 5-minute availability, latest motion, sleep state and RSSI |
| `GET /rooms/{id}/occupancy?window=1m\|5m\|1h` | `in_room` + time-weighted `occupied_pct` |
| `GET /alarms?since=&limit=&room_id=&wait=` | deduplicated falls; `wait` long-polls (max 60s); `next_cursor` |
| `GET /alarms/stream?since=&room_id=` | SSE, `id` = `alarm_id`; resumes from `Last-Event-ID` |
| `GET /metrics` | Prometheus text |
| `GET /healthz`, `GET /readyz` | liveness; ready once recovery finished |

## Configuration (environment variables)

| Variable | Default | |
|---|---|---|
| `PORT` | `8080` | |
| `DATA_DIR` | `./data` | WAL segments + snapshots |
| `FSYNC` | `interval` | `batch` (every group commit), `interval`, `off` |
| `FSYNC_MS` | `50` | interval for `FSYNC=interval` |
| `SNAPSHOT_SECS` | `30` | |
| `BULK_LANE_CAPACITY` / `FALL_LANE_CAPACITY` | `500000` / `50000` | queued requests per lane |
| `ENQUEUE_TIMEOUT_MS` | `3000` | wait on a full lane before answering 503 |
| `DEDUP_WINDOW_MS` | `0` | `0` = exact `(device_id, ts)` match; >0 merges falls within ±window |
| `ALARM_RETENTION_H` | `24` | |
| `LOG_FORMAT` | text | `json` for structured logs |

`FSYNC=interval` survives a process kill (data is already in the OS page
cache). A power loss can lose at most the last `FSYNC_MS` of writes. Use
`FSYNC=batch` if that matters more than throughput.

## Testing

```bash
make test             # unit + property tests (aggregates are a function of the event set)
make load             # 5k devices, 50k ev/s burst, offline replay; alarm latency
make kill-test        # Python generator (adversarial) + kill -9 at 180s
make kill-test-full   # grader-shaped: 5k devices, 5 min, 2 bursts, offline 60s, kill -9 at 180s
```
