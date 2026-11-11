//! Service metrics: lock-free counters, gauges and fixed-bucket histograms,
//! rendered in the Prometheus text format at `GET /metrics`, plus a
//! periodic one-line summary in the logs.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use crate::event::{Kind, KIND_NAMES};

/// Bucket upper bounds, in seconds. Covers 0.1 ms to 5 s.
const BUCKETS: [f64; 14] = [0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0];

#[derive(Debug)]
pub struct Histogram {
    /// Per-bucket (non-cumulative) counts; last slot is +Inf.
    counts: [AtomicU64; BUCKETS.len() + 1],
    sum_us: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self { counts: std::array::from_fn(|_| AtomicU64::new(0)), sum_us: AtomicU64::new(0) }
    }
}

impl Histogram {
    pub fn observe(&self, d: Duration) {
        let secs = d.as_secs_f64();
        let i = BUCKETS.iter().position(|&b| secs <= b).unwrap_or(BUCKETS.len());
        self.counts[i].fetch_add(1, Relaxed);
        self.sum_us.fetch_add(d.as_micros() as u64, Relaxed);
    }

    pub fn counts(&self) -> Vec<u64> {
        self.counts.iter().map(|c| c.load(Relaxed)).collect()
    }

    /// Upper bound of the bucket holding quantile `q` among `counts`
    /// (e.g. a delta between two `counts()` calls). `None` if empty;
    /// `INFINITY` if it lands past the last bucket.
    pub fn quantile(counts: &[u64], q: f64) -> Option<f64> {
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return None;
        }
        let rank = (q * total as f64).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, c) in counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Some(BUCKETS.get(i).copied().unwrap_or(f64::INFINITY));
            }
        }
        Some(f64::INFINITY)
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} histogram");
        let counts = self.counts();
        let mut cum = 0;
        for (i, b) in BUCKETS.iter().enumerate() {
            cum += counts[i];
            let _ = writeln!(out, "{name}_bucket{{le=\"{b}\"}} {cum}");
        }
        cum += counts[BUCKETS.len()];
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cum}");
        let _ = writeln!(out, "{name}_sum {}", self.sum_us.load(Relaxed) as f64 / 1e6);
        let _ = writeln!(out, "{name}_count {cum}");
    }
}

/// Every metric the service exports. Shared as `Arc<Metrics>`.
#[derive(Debug)]
pub struct Metrics {
    started: Instant,
    // Ingest
    pub events_accepted: [AtomicU64; KIND_NAMES.len()],
    pub events_rejected: [AtomicU64; REJECT_REASONS.len()],
    pub requests_shed: AtomicU64,
    pub events_shed: AtomicU64,
    /// Time from request arrival to 202 (includes queueing under backpressure).
    pub ingest_ack: Histogram,
    // WAL
    pub wal_batches: AtomicU64,
    pub wal_bytes: AtomicU64,
    pub wal_fsyncs: AtomicU64,
    pub wal_fsync: Histogram,
    pub wal_errors: AtomicU64,
    // Alarms
    pub alarms: AtomicU64,
    pub falls_deduped: AtomicU64,
    /// Time from request arrival to the alarm being published to the feed.
    pub alarm_emit: Histogram,
    pub sse_subscribers: AtomicI64,
    // Snapshots and recovery
    pub snapshots: AtomicU64,
    pub snapshot_failures: AtomicU64,
    pub snapshot: Histogram,
    pub snapshot_bytes: AtomicU64,
    pub snapshot_offset: AtomicU64,
    pub recovery_replayed: AtomicU64,
    pub recovery_ms: AtomicU64,
}

/// Rejection reasons, matching `event::Reject::code` plus a whole-body one.
pub const REJECT_REASONS: [&str; 4] = ["malformed", "ts_future", "ts_too_old", "invalid_json"];

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            events_accepted: std::array::from_fn(|_| AtomicU64::new(0)),
            events_rejected: std::array::from_fn(|_| AtomicU64::new(0)),
            requests_shed: AtomicU64::new(0),
            events_shed: AtomicU64::new(0),
            ingest_ack: Histogram::default(),
            wal_batches: AtomicU64::new(0),
            wal_bytes: AtomicU64::new(0),
            wal_fsyncs: AtomicU64::new(0),
            wal_fsync: Histogram::default(),
            wal_errors: AtomicU64::new(0),
            alarms: AtomicU64::new(0),
            falls_deduped: AtomicU64::new(0),
            alarm_emit: Histogram::default(),
            sse_subscribers: AtomicI64::new(0),
            snapshots: AtomicU64::new(0),
            snapshot_failures: AtomicU64::new(0),
            snapshot: Histogram::default(),
            snapshot_bytes: AtomicU64::new(0),
            snapshot_offset: AtomicU64::new(0),
            recovery_replayed: AtomicU64::new(0),
            recovery_ms: AtomicU64::new(0),
        }
    }
}

/// Point-in-time values owned elsewhere, sampled when rendering.
#[derive(Debug, Clone, Default)]
pub struct Gauges {
    pub fall_lane_depth: usize,
    pub bulk_lane_depth: usize,
    pub last_offset: u64,
    pub devices: usize,
    pub rooms: usize,
    pub alarms_retained: usize,
}

impl Metrics {
    pub fn accepted(&self, kind: &Kind) {
        self.events_accepted[kind.index()].fetch_add(1, Relaxed);
    }

    pub fn rejected(&self, reason: &str) {
        if let Some(i) = REJECT_REASONS.iter().position(|r| *r == reason) {
            self.events_rejected[i].fetch_add(1, Relaxed);
        }
    }

    pub fn accepted_total(&self) -> u64 {
        self.events_accepted.iter().map(|c| c.load(Relaxed)).sum()
    }

    pub fn rejected_total(&self) -> u64 {
        self.events_rejected.iter().map(|c| c.load(Relaxed)).sum()
    }

    /// Prometheus text exposition format (version 0.0.4).
    pub fn render(&self, g: &Gauges) -> String {
        let mut o = String::with_capacity(8 * 1024);
        let counter = |o: &mut String, name: &str, help: &str, v: u64| {
            let _ = writeln!(o, "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}");
        };
        let gauge = |o: &mut String, name: &str, help: &str, v: f64| {
            let _ = writeln!(o, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}");
        };

        let _ = writeln!(o, "# HELP events_accepted_total Events written to the WAL and applied, by type.\n# TYPE events_accepted_total counter");
        for (name, c) in KIND_NAMES.iter().zip(&self.events_accepted) {
            let _ = writeln!(o, "events_accepted_total{{type=\"{name}\"}} {}", c.load(Relaxed));
        }
        let _ = writeln!(o, "# HELP events_rejected_total Events rejected at validation, by reason.\n# TYPE events_rejected_total counter");
        for (name, c) in REJECT_REASONS.iter().zip(&self.events_rejected) {
            let _ = writeln!(o, "events_rejected_total{{reason=\"{name}\"}} {}", c.load(Relaxed));
        }
        counter(&mut o, "ingest_requests_shed_total", "Requests answered 503 because a lane stayed full (never silent).", self.requests_shed.load(Relaxed));
        counter(&mut o, "ingest_events_shed_total", "Events in shed requests (the client was told to retry).", self.events_shed.load(Relaxed));
        let _ = writeln!(o, "# HELP ingest_lane_depth Requests queued per lane.\n# TYPE ingest_lane_depth gauge");
        let _ = writeln!(o, "ingest_lane_depth{{lane=\"fall\"}} {}\ningest_lane_depth{{lane=\"bulk\"}} {}", g.fall_lane_depth, g.bulk_lane_depth);
        self.ingest_ack.render(&mut o, "ingest_ack_seconds", "Request arrival to 202 (durable and applied).");

        counter(&mut o, "wal_batches_total", "Group commits written.", self.wal_batches.load(Relaxed));
        counter(&mut o, "wal_bytes_total", "Bytes appended to the WAL.", self.wal_bytes.load(Relaxed));
        counter(&mut o, "wal_fsyncs_total", "fsync calls on the WAL.", self.wal_fsyncs.load(Relaxed));
        counter(&mut o, "wal_errors_total", "Failed WAL appends or fsyncs.", self.wal_errors.load(Relaxed));
        self.wal_fsync.render(&mut o, "wal_fsync_seconds", "Duration of WAL fsyncs.");
        gauge(&mut o, "wal_last_offset", "Offset of the last applied event.", g.last_offset as f64);

        counter(&mut o, "alarms_total", "Distinct fall alarms emitted.", self.alarms.load(Relaxed));
        counter(&mut o, "falls_deduped_total", "Fall warnings dropped as duplicates of an emitted alarm.", self.falls_deduped.load(Relaxed));
        self.alarm_emit.render(&mut o, "alarm_emit_latency_seconds", "Request arrival to alarm published on the feed.");
        gauge(&mut o, "alarm_feed_subscribers", "Open SSE subscriptions.", self.sse_subscribers.load(Relaxed) as f64);
        gauge(&mut o, "alarms_retained", "Alarms held in the store.", g.alarms_retained as f64);

        counter(&mut o, "snapshots_total", "Snapshots written.", self.snapshots.load(Relaxed));
        counter(&mut o, "snapshot_failures_total", "Snapshot attempts that failed.", self.snapshot_failures.load(Relaxed));
        self.snapshot.render(&mut o, "snapshot_seconds", "Time to clone, write and GC one snapshot.");
        gauge(&mut o, "snapshot_bytes", "Size of the latest snapshot.", self.snapshot_bytes.load(Relaxed) as f64);
        gauge(&mut o, "snapshot_last_offset", "WAL offset covered by the latest snapshot.", self.snapshot_offset.load(Relaxed) as f64);
        gauge(&mut o, "recovery_replayed_events", "WAL events replayed at the last start.", self.recovery_replayed.load(Relaxed) as f64);
        gauge(&mut o, "recovery_seconds", "Duration of the last recovery.", self.recovery_ms.load(Relaxed) as f64 / 1e3);

        gauge(&mut o, "devices", "Devices seen.", g.devices as f64);
        gauge(&mut o, "rooms", "Rooms with presence history.", g.rooms as f64);
        gauge(&mut o, "uptime_seconds", "Seconds since start.", self.started.elapsed().as_secs_f64());
        o
    }
}

/// Keeps `alarm_feed_subscribers` accurate: increments on creation,
/// decrements when the stream (and this guard) is dropped.
pub struct SubscriberGuard(std::sync::Arc<Metrics>);

impl SubscriberGuard {
    pub fn new(m: std::sync::Arc<Metrics>) -> Self {
        m.sse_subscribers.fetch_add(1, Relaxed);
        Self(m)
    }
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        self.0.sse_subscribers.fetch_sub(1, Relaxed);
    }
}

/// Builds the periodic stats log line from deltas between calls.
pub struct StatsLogger {
    last_at: Instant,
    accepted: u64,
    rejected: u64,
    shed: u64,
    alarms: u64,
    alarm_emit: Vec<u64>,
    ingest_ack: Vec<u64>,
}

impl StatsLogger {
    pub fn new(m: &Metrics) -> Self {
        Self {
            last_at: Instant::now(),
            accepted: m.accepted_total(),
            rejected: m.rejected_total(),
            shed: m.requests_shed.load(Relaxed),
            alarms: m.alarms.load(Relaxed),
            alarm_emit: m.alarm_emit.counts(),
            ingest_ack: m.ingest_ack.counts(),
        }
    }

    pub fn log(&mut self, m: &Metrics, g: &Gauges) {
        let secs = self.last_at.elapsed().as_secs_f64().max(1e-3);
        let (accepted, rejected, shed, alarms) = (m.accepted_total(), m.rejected_total(), m.requests_shed.load(Relaxed), m.alarms.load(Relaxed));
        let (emit, ack) = (m.alarm_emit.counts(), m.ingest_ack.counts());
        let delta = |now: &[u64], before: &[u64]| now.iter().zip(before).map(|(a, b)| a - b).collect::<Vec<_>>();
        let ms = |q: Option<f64>| q.map(|s| format!("<={}ms", s * 1e3)).unwrap_or_else(|| "-".into());
        tracing::info!(
            events_per_sec = ((accepted - self.accepted) as f64 / secs).round(),
            rejected = rejected - self.rejected,
            shed = shed - self.shed,
            alarms = alarms - self.alarms,
            alarm_p95 = %ms(Histogram::quantile(&delta(&emit, &self.alarm_emit), 0.95)),
            ack_p95 = %ms(Histogram::quantile(&delta(&ack, &self.ingest_ack), 0.95)),
            lanes = %format!("fall={} bulk={}", g.fall_lane_depth, g.bulk_lane_depth),
            subscribers = m.sse_subscribers.load(Relaxed),
            last_offset = g.last_offset,
            devices = g.devices,
            "stats"
        );
        *self = Self { last_at: Instant::now(), accepted, rejected, shed, alarms, alarm_emit: emit, ingest_ack: ack };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_and_quantiles() {
        let h = Histogram::default();
        for _ in 0..90 {
            h.observe(Duration::from_micros(400)); // <= 0.5ms
        }
        for _ in 0..10 {
            h.observe(Duration::from_millis(30)); // <= 50ms
        }
        let c = h.counts();
        assert_eq!(Histogram::quantile(&c, 0.5), Some(0.0005));
        assert_eq!(Histogram::quantile(&c, 0.95), Some(0.05));
        assert_eq!(Histogram::quantile(&[0; 15], 0.95), None);
        h.observe(Duration::from_secs(10));
        assert_eq!(Histogram::quantile(&h.counts(), 1.0), Some(f64::INFINITY));
    }

    #[test]
    fn render_is_prometheus_text() {
        let m = Metrics::default();
        m.accepted(&Kind::Heartbeat);
        m.accepted(&Kind::FallWarn { confidence: 0.9 });
        m.rejected("ts_future");
        m.alarm_emit.observe(Duration::from_millis(2));
        let _guard = SubscriberGuard::new(std::sync::Arc::new(Metrics::default()));
        let out = m.render(&Gauges { bulk_lane_depth: 3, ..Default::default() });
        for line in [
            "events_accepted_total{type=\"heartbeat\"} 1",
            "events_accepted_total{type=\"fall_warn\"} 1",
            "events_rejected_total{reason=\"ts_future\"} 1",
            "ingest_lane_depth{lane=\"bulk\"} 3",
            "alarm_emit_latency_seconds_bucket{le=\"0.0025\"} 1",
            "alarm_emit_latency_seconds_bucket{le=\"+Inf\"} 1",
            "alarm_emit_latency_seconds_count 1",
        ] {
            assert!(out.contains(line), "missing {line:?}");
        }
        // Every non-comment line is `name[{labels}] value`.
        for l in out.lines().filter(|l| !l.starts_with('#')) {
            let (_, v) = l.rsplit_once(' ').unwrap();
            assert!(v.parse::<f64>().is_ok(), "bad sample line {l:?}");
        }
    }

    #[test]
    fn subscriber_guard_counts() {
        let m = std::sync::Arc::new(Metrics::default());
        let a = SubscriberGuard::new(m.clone());
        let b = SubscriberGuard::new(m.clone());
        assert_eq!(m.sse_subscribers.load(Relaxed), 2);
        drop(a);
        drop(b);
        assert_eq!(m.sse_subscribers.load(Relaxed), 0);
    }
}
