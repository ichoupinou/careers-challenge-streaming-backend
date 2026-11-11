//! Ingest pipeline: priority lanes → WAL writer (group commit) → applier.
//!
//! ```text
//! handlers ──► fall lane ─┐  biased: falls first
//!          └─► bulk lane ─┴─► writer thread: one write() per batch, fsync per policy
//!                                  │  small bounded channel
//!                                  ▼
//!                          applier thread: State::apply in WAL order, then ack
//! ```
//!
//! A request is acknowledged once its events are in the WAL (written to
//! the OS, so they survive a process kill) *and* applied, so a 202 means
//! durable and readable. Backpressure builds up in the lanes, where
//! `fall_warn` jumps the queue. When a lane stays full past
//! `enqueue_timeout`, the request fails with an explicit `Overloaded`
//! (HTTP 503), never a silent drop.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};

use crate::clock::Millis;
use crate::config::{Config, FsyncPolicy};
use crate::event::Event;
use crate::state::{Applied, State};
use crate::wal::{Wal, WalRecord};

/// Most events written in one group commit.
pub const MAX_BATCH_EVENTS: usize = 4096;
/// Batches in flight between writer and applier. Kept small so backlog
/// stays in the lanes, where priority applies.
const APPLY_QUEUE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IngestError {
    #[error("ingest queue full, retry later")]
    Overloaded,
    #[error("ingest pipeline is shut down")]
    ShuttingDown,
    #[error("WAL write failed: {0}")]
    Wal(String),
}

type Ack = oneshot::Sender<Result<(), IngestError>>;

struct Submission {
    events: Vec<Event>,
    recv_ms: Millis,
    ack: Ack,
}

#[derive(Debug, Default)]
pub struct IngestStats {
    pub accepted_events: AtomicU64,
    pub shed_requests: AtomicU64,
    pub batches: AtomicU64,
    pub fsyncs: AtomicU64,
    pub alarms: AtomicU64,
    /// Sum and count of recv→applied latency for new alarms, in µs.
    pub alarm_latency_us_sum: AtomicU64,
    pub alarm_latency_us_max: AtomicU64,
}

/// Cloneable handle used by HTTP handlers.
#[derive(Clone)]
pub struct Ingest {
    fall_tx: mpsc::Sender<Submission>,
    bulk_tx: mpsc::Sender<Submission>,
    enqueue_timeout: Duration,
    pub stats: Arc<IngestStats>,
}

impl Ingest {
    /// Enqueue validated events and wait until they're durable and applied.
    /// Falls go through the priority lane; a mixed batch is split and the
    /// call returns once both halves are done.
    pub async fn submit(&self, events: Vec<Event>, recv_ms: Millis) -> Result<(), IngestError> {
        if events.is_empty() {
            return Ok(());
        }
        let (falls, bulk): (Vec<_>, Vec<_>) = events.into_iter().partition(|e| e.kind.is_fall());
        let n = (falls.len() + bulk.len()) as u64;
        let mut acks = Vec::with_capacity(2);
        for (lane, events) in [(&self.fall_tx, falls), (&self.bulk_tx, bulk)] {
            if events.is_empty() {
                continue;
            }
            let (ack, done) = oneshot::channel();
            match lane.send_timeout(Submission { events, recv_ms, ack }, self.enqueue_timeout).await {
                Ok(()) => acks.push(done),
                Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                    self.stats.shed_requests.fetch_add(1, Ordering::Relaxed);
                    return Err(IngestError::Overloaded);
                }
                Err(mpsc::error::SendTimeoutError::Closed(_)) => return Err(IngestError::ShuttingDown),
            }
        }
        for done in acks {
            done.await.map_err(|_| IngestError::ShuttingDown)??;
        }
        self.stats.accepted_events.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }

    /// Queued requests per lane: `(fall, bulk)`.
    pub fn lane_depths(&self) -> (usize, usize) {
        let depth = |tx: &mpsc::Sender<Submission>| tx.max_capacity() - tx.capacity();
        (depth(&self.fall_tx), depth(&self.bulk_tx))
    }
}

/// Receiving ends of the lanes, consumed by `start`.
pub struct Lanes {
    fall_rx: mpsc::Receiver<Submission>,
    bulk_rx: mpsc::Receiver<Submission>,
}

/// Create the handle and the lanes. Nothing is processed until `start`.
pub fn lanes(config: &Config) -> (Ingest, Lanes) {
    let (fall_tx, fall_rx) = mpsc::channel(config.fall_lane_capacity.max(1));
    let (bulk_tx, bulk_rx) = mpsc::channel(config.bulk_lane_capacity.max(1));
    let ingest = Ingest { fall_tx, bulk_tx, enqueue_timeout: config.enqueue_timeout, stats: Arc::default() };
    (ingest, Lanes { fall_rx, bulk_rx })
}

/// Running writer + applier threads.
pub struct Pipeline {
    shutdown: Option<oneshot::Sender<()>>,
    writer: Option<JoinHandle<()>>,
    applier: Option<JoinHandle<()>>,
}

impl Pipeline {
    /// Stop accepting, drain what's queued, fsync, and join both threads.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        for h in [self.writer.take(), self.applier.take()].into_iter().flatten() {
            let _ = h.join();
        }
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Batch {
    records: Vec<WalRecord>,
    acks: Vec<Ack>,
}

/// Spawn the writer and applier threads.
pub fn start(lanes: Lanes, wal: Wal, state: Arc<RwLock<State>>, stats: Arc<IngestStats>, config: &Config) -> Pipeline {
    let (apply_tx, apply_rx) = std::sync::mpsc::sync_channel::<Batch>(APPLY_QUEUE);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let fsync = (config.fsync, config.fsync_interval);
    let writer_stats = stats.clone();
    let writer = std::thread::Builder::new()
        .name("wal-writer".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().expect("writer runtime");
            rt.block_on(writer_loop(lanes, wal, apply_tx, shutdown_rx, fsync, writer_stats));
        })
        .expect("spawn wal-writer");

    let applier = std::thread::Builder::new()
        .name("applier".into())
        .spawn(move || applier_loop(apply_rx, state, stats))
        .expect("spawn applier");

    Pipeline { shutdown: Some(shutdown_tx), writer: Some(writer), applier: Some(applier) }
}

async fn writer_loop(
    mut lanes: Lanes,
    mut wal: Wal,
    apply_tx: std::sync::mpsc::SyncSender<Batch>,
    mut shutdown: oneshot::Receiver<()>,
    (policy, interval): (FsyncPolicy, Duration),
    stats: Arc<IngestStats>,
) {
    let mut dirty = false;
    let mut last_sync = Instant::now();
    loop {
        // Wait for work. Falls win ties; an idle-but-dirty WAL is fsynced
        // once the interval elapses.
        let first = tokio::select! {
            biased;
            Some(s) = lanes.fall_rx.recv() => Some(s),
            Some(s) = lanes.bulk_rx.recv() => Some(s),
            _ = tokio::time::sleep(interval.saturating_sub(last_sync.elapsed())), if dirty && policy == FsyncPolicy::Interval => None,
            _ = &mut shutdown => break,
            else => break,
        };
        let mut subs = Vec::new();
        if let Some(s) = first {
            subs.push(s);
            fill_batch(&mut lanes, &mut subs);
        }
        if !subs.is_empty() {
            if !write_batch(&mut wal, subs, &apply_tx, &stats, policy == FsyncPolicy::Batch) {
                break;
            }
            dirty = policy != FsyncPolicy::Batch;
        }
        if dirty && policy == FsyncPolicy::Interval && last_sync.elapsed() >= interval {
            sync(&mut wal, &stats);
            dirty = false;
            last_sync = Instant::now();
        }
    }

    // Shutdown: refuse new work, drain what's already queued, fsync.
    lanes.fall_rx.close();
    lanes.bulk_rx.close();
    loop {
        let mut subs = Vec::new();
        fill_batch(&mut lanes, &mut subs);
        if subs.is_empty() || !write_batch(&mut wal, subs, &apply_tx, &stats, policy == FsyncPolicy::Batch) {
            break;
        }
    }
    if policy != FsyncPolicy::Off {
        sync(&mut wal, &stats);
    }
    tracing::info!(next_offset = wal.next_offset(), "WAL writer stopped");
}

/// Top up `subs` from the lanes without waiting: all queued falls first,
/// then bulk, up to `MAX_BATCH_EVENTS`.
fn fill_batch(lanes: &mut Lanes, subs: &mut Vec<Submission>) {
    let mut n: usize = subs.iter().map(|s| s.events.len()).sum();
    for rx in [&mut lanes.fall_rx, &mut lanes.bulk_rx] {
        while n < MAX_BATCH_EVENTS {
            let Ok(s) = rx.try_recv() else { break };
            n += s.events.len();
            subs.push(s);
        }
    }
}

/// Write one group commit (and fsync it first if `sync_before_ack`), then
/// hand it to the applier, which acknowledges. Returns false if the
/// pipeline can't continue.
fn write_batch(
    wal: &mut Wal,
    subs: Vec<Submission>,
    apply_tx: &std::sync::mpsc::SyncSender<Batch>,
    stats: &IngestStats,
    sync_before_ack: bool,
) -> bool {
    let mut acks = Vec::with_capacity(subs.len());
    let mut events = Vec::new();
    for s in subs {
        events.extend(s.events.into_iter().map(|e| (e, s.recv_ms)));
        acks.push(s.ack);
    }
    match wal.append(events) {
        Ok(records) => {
            stats.batches.fetch_add(1, Ordering::Relaxed);
            if sync_before_ack {
                sync(wal, stats);
            }
            apply_tx.send(Batch { records, acks }).is_ok()
        }
        Err(e) => {
            // Fail loudly: callers get a 500 and nothing is acknowledged.
            tracing::error!(error = %e, "WAL append failed; refusing writes");
            for ack in acks {
                let _ = ack.send(Err(IngestError::Wal(e.to_string())));
            }
            false
        }
    }
}

fn sync(wal: &mut Wal, stats: &IngestStats) {
    match wal.sync() {
        Ok(()) => {
            stats.fsyncs.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => tracing::error!(error = %e, "WAL fsync failed"),
    }
}

fn applier_loop(rx: std::sync::mpsc::Receiver<Batch>, state: Arc<RwLock<State>>, stats: Arc<IngestStats>) {
    while let Ok(batch) = rx.recv() {
        let mut alarms = Vec::new();
        {
            let mut st = state.write();
            for rec in &batch.records {
                if let Applied::Alarm(a) = st.apply(&rec.event, rec.offset, rec.recv_ms) {
                    alarms.push(a);
                }
            }
        }
        let now = chrono::Utc::now().timestamp_millis();
        for a in &alarms {
            let us = ((now - a.recv_ms).max(0) as u64) * 1000;
            stats.alarms.fetch_add(1, Ordering::Relaxed);
            stats.alarm_latency_us_sum.fetch_add(us, Ordering::Relaxed);
            stats.alarm_latency_us_max.fetch_max(us, Ordering::Relaxed);
            tracing::debug!(alarm_id = a.alarm_id, room = %a.room_id, device = %a.device_id, "alarm");
        }
        // TODO(milestone 5): publish `alarms` to the live feed.
        for ack in batch.acks {
            let _ = ack.send(Ok(()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Kind;

    fn ev(device: &str, kind: Kind) -> Event {
        Event { device_id: device.into(), room_id: "room_000".into(), ts_ms: 1_000, seq: 1, kind }
    }

    fn cfg(bulk: usize, timeout_ms: u64) -> Config {
        Config { bulk_lane_capacity: bulk, fall_lane_capacity: 4, enqueue_timeout: Duration::from_millis(timeout_ms), ..Config::default() }
    }

    #[tokio::test]
    async fn full_lane_times_out_with_overloaded() {
        let (ingest, _lanes) = lanes(&cfg(1, 50)); // nothing draining the lanes
        let i2 = ingest.clone();
        let first = tokio::spawn(async move { i2.submit(vec![ev("a", Kind::Heartbeat)], 0).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(ingest.lane_depths(), (0, 1));
        let r = ingest.submit(vec![ev("b", Kind::Heartbeat)], 0).await;
        assert_eq!(r, Err(IngestError::Overloaded));
        assert_eq!(ingest.stats.shed_requests.load(Ordering::Relaxed), 1);
        // The fall lane is separate: a fall can still be queued.
        let i3 = ingest.clone();
        tokio::spawn(async move { i3.submit(vec![ev("c", Kind::FallWarn { confidence: 0.9 })], 0).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(ingest.lane_depths(), (1, 1));
        first.abort();
    }

    #[tokio::test]
    async fn falls_are_written_before_queued_bulk() {
        let tmp = tempfile::tempdir().unwrap();
        let config = cfg(1000, 1000);
        let (ingest, lanes) = lanes(&config);
        // Queue bulk first, then a fall, while nothing is draining.
        let mut tasks = Vec::new();
        for i in 0..5 {
            let ing = ingest.clone();
            tasks.push(tokio::spawn(async move { ing.submit(vec![ev(&format!("hb{i}"), Kind::Heartbeat)], 0).await }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let ing = ingest.clone();
        tasks.push(tokio::spawn(async move { ing.submit(vec![ev("faller", Kind::FallWarn { confidence: 0.9 })], 0).await }));
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (wal, _) = Wal::open(tmp.path()).unwrap();
        let state = Arc::new(RwLock::new(State::new(0)));
        let pipeline = start(lanes, wal, state.clone(), ingest.stats.clone(), &config);
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        pipeline.shutdown();

        let mut order = Vec::new();
        crate::wal::replay(tmp.path(), 0, |r| order.push(r.event.device_id)).unwrap();
        assert_eq!(order[0], "faller", "fall jumped the queue: {order:?}");
        assert_eq!(order.len(), 6);
        assert_eq!(state.read().last_offset, 6);
        assert_eq!(state.read().alarms().len(), 1);
    }

    #[tokio::test]
    async fn submit_returns_after_apply_and_shutdown_drains() {
        let tmp = tempfile::tempdir().unwrap();
        let config = Config { fsync: FsyncPolicy::Batch, ..cfg(1000, 1000) };
        let (ingest, lanes) = lanes(&config);
        let (wal, _) = Wal::open(tmp.path()).unwrap();
        let state = Arc::new(RwLock::new(State::new(0)));
        let pipeline = start(lanes, wal, state.clone(), ingest.stats.clone(), &config);

        let mixed = vec![ev("a", Kind::Heartbeat), ev("a", Kind::FallWarn { confidence: 0.9 }), ev("b", Kind::Heartbeat)];
        ingest.submit(mixed, 0).await.unwrap();
        assert_eq!(state.read().stats.applied, 3, "visible as soon as submit returns");
        assert!(ingest.stats.fsyncs.load(Ordering::Relaxed) >= 1);

        pipeline.shutdown();
        assert_eq!(ingest.submit(vec![ev("c", Kind::Heartbeat)], 0).await, Err(IngestError::ShuttingDown));
    }
}
