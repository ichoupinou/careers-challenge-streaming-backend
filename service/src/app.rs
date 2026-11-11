//! Service assembly: recover state (snapshot + WAL tail), start the pipeline.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use tokio::sync::watch;

use crate::api::AppState;
use crate::clock::Clock;
use crate::config::Config;
use crate::ingest::{self, Pipeline};
use crate::metrics::Metrics;
use crate::snapshot::{self, Snapshotter};
use crate::state::State;
use crate::wal::{self, Wal, SEGMENT_BYTES};

/// A running service: shared state for the HTTP layer, the pipeline threads
/// and the snapshotter. Call `shutdown` to drain, fsync and snapshot on exit.
pub struct Service {
    pub app: Arc<AppState>,
    pub pipeline: Pipeline,
    pub snapshotter: Arc<Snapshotter>,
}

impl Service {
    /// Graceful stop: drain queued events into the WAL, fsync, then write a
    /// final snapshot so the next start replays nothing.
    pub fn shutdown(self) {
        self.pipeline.shutdown();
        take_snapshot(&self.snapshotter, &self.app.metrics);
    }
}

/// Run one snapshot, recording metrics and logging the outcome.
pub fn take_snapshot(snapshotter: &Snapshotter, metrics: &Metrics) {
    use std::sync::atomic::Ordering::Relaxed;
    match snapshotter.run_once() {
        Ok(Some(info)) => {
            metrics.snapshots.fetch_add(1, Relaxed);
            metrics.snapshot.observe(std::time::Duration::from_millis(info.millis));
            metrics.snapshot_bytes.store(info.bytes, Relaxed);
            metrics.snapshot_offset.store(info.last_offset, Relaxed);
            tracing::info!(
                last_offset = info.last_offset,
                bytes = info.bytes,
                millis = info.millis,
                wal_segments_deleted = info.wal_segments_deleted,
                "snapshot written"
            );
        }
        Ok(None) => {}
        Err(e) => {
            metrics.snapshot_failures.fetch_add(1, Relaxed);
            tracing::error!(error = %e, "snapshot failed");
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryReport {
    pub snapshot_offset: Option<u64>,
    pub replayed: u64,
    pub last_offset: u64,
    pub truncated_bytes: u64,
}

/// Rebuild state from `data_dir`: newest valid snapshot, then every WAL
/// record after it. Fails if the WAL has a gap after the snapshot (missing
/// data must not be served as if it were complete).
pub fn recover(data_dir: &Path, dedup_window_ms: i64) -> io::Result<(State, Wal, RecoveryReport)> {
    let (snap_dir, wal_dir) = dirs(data_dir);
    let mut state = snapshot::load_latest(&snap_dir)?.unwrap_or_else(|| State::new(dedup_window_ms));
    state.set_dedup_window(dedup_window_ms);
    let snapshot_offset = (state.last_offset > 0).then_some(state.last_offset);

    // Opening first truncates a torn tail left by a crash.
    let (wal, open) = Wal::open_with(&wal_dir, SEGMENT_BYTES, state.last_offset)?;

    let mut expected = state.last_offset + 1;
    let mut gap = None;
    // Replayed events were validated when accepted: apply them as-is.
    let replayed = wal::replay(&wal_dir, state.last_offset, |rec| {
        if gap.is_some() {
            return;
        }
        if rec.offset != expected {
            gap = Some((expected, rec.offset));
            return;
        }
        state.apply(&rec.event, rec.offset, rec.recv_ms);
        expected += 1;
    })?;
    if let Some((want, got)) = gap {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("WAL gap: expected offset {want}, found {got}; refusing to start with missing events"),
        ));
    }
    let report = RecoveryReport { snapshot_offset, replayed, last_offset: state.last_offset, truncated_bytes: open.truncated_bytes };
    Ok((state, wal, report))
}

fn dirs(data_dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    (data_dir.join("snapshots"), data_dir.join("wal"))
}

/// Recover from `config.data_dir`, then start ingesting.
pub fn start(config: Config, clock: Arc<dyn Clock>) -> io::Result<Service> {
    let started = Instant::now();
    let (state, wal, report) = recover(&config.data_dir, config.dedup_window_ms)?;
    tracing::info!(
        snapshot_offset = report.snapshot_offset,
        replayed = report.replayed,
        last_offset = report.last_offset,
        truncated_bytes = report.truncated_bytes,
        alarms = state.alarms().len(),
        devices = state.device_count(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "recovered"
    );

    let metrics = Arc::new(Metrics::default());
    metrics.recovery_replayed.store(report.replayed, Ordering::Relaxed);
    metrics.recovery_ms.store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
    if let Some(off) = report.snapshot_offset {
        metrics.snapshot_offset.store(off, Ordering::Relaxed);
    }

    let (snap_dir, wal_dir) = dirs(&config.data_dir);
    let state = Arc::new(RwLock::new(state));
    let snapshotter = Arc::new(Snapshotter::new(snap_dir, wal_dir, state.clone()));
    let (alarm_tx, alarm_rx) = watch::channel(state.read().alarms().last_id());
    let (ingest, lanes) = ingest::lanes(&config, metrics.clone());
    let pipeline = ingest::start(lanes, wal, state.clone(), metrics.clone(), alarm_tx, &config);
    let app = Arc::new(AppState {
        config,
        clock,
        ready: AtomicBool::new(false),
        state,
        ingest,
        alarm_rx,
        closing: watch::channel(false).0,
        metrics,
    });
    app.ready.store(true, Ordering::Release);
    Ok(Service { app, pipeline, snapshotter })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Kind};
    use crate::wal::SEGMENT_BYTES;

    fn ev(i: u64) -> (Event, i64) {
        let kind = match i % 5 {
            0 => Kind::FallWarn { confidence: 0.9 },
            1 => Kind::Presence { in_room: i % 10 == 1 },
            2 => Kind::NetStatus { rssi: -60 },
            _ => Kind::Heartbeat,
        };
        // Out-of-order timestamps and repeated falls on purpose.
        let ts = 1_000_000 + ((i * 7919) % 500) as i64 * 1_000;
        (Event { device_id: format!("dev_{}", i % 7), room_id: format!("room_{}", i % 3), ts_ms: ts, seq: i, kind }, 2_000_000)
    }

    fn write_events(data: &Path, range: std::ops::RangeInclusive<u64>, limit: u64) {
        let (mut wal, _) = Wal::open_with(data.join("wal"), limit, 0).unwrap();
        for i in range {
            wal.append([ev(i)]).unwrap();
        }
    }

    fn full_replay(n: u64) -> State {
        let mut s = State::new(0);
        for i in 1..=n {
            let (e, recv) = ev(i);
            s.apply(&e, i, recv);
        }
        s
    }

    #[test]
    fn snapshot_plus_tail_equals_full_replay() {
        let tmp = tempfile::tempdir().unwrap();
        write_events(tmp.path(), 1..=300, 512);
        // Snapshot taken at offset 180 (as if the applier had reached it).
        let (snap_dir, wal_dir) = dirs(tmp.path());
        let snapper = Snapshotter::new(snap_dir.clone(), wal_dir.clone(), Arc::new(RwLock::new(full_replay(180))));
        snapper.run_once().unwrap().unwrap();
        // ... then more events arrive.
        write_events(tmp.path(), 301..=400, 512);

        let (state, mut wal, report) = recover(tmp.path(), 0).unwrap();
        assert_eq!(report.snapshot_offset, Some(180));
        assert_eq!(report.replayed, 220);
        assert_eq!(state, full_replay(400), "byte-for-byte the same state");
        assert_eq!(wal.append([ev(401)]).unwrap()[0].offset, 401);
    }

    #[test]
    fn gc_keeps_enough_wal_for_the_older_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let (snap_dir, wal_dir) = dirs(tmp.path());
        write_events(tmp.path(), 1..=100, 256);
        let shared = Arc::new(RwLock::new(full_replay(100)));
        let snapper = Snapshotter::new(snap_dir.clone(), wal_dir.clone(), shared.clone());
        assert_eq!(snapper.run_once().unwrap().unwrap().wal_segments_deleted, 0, "one snapshot: no GC");
        assert_eq!(snapper.run_once().unwrap(), None, "nothing new: skipped");

        write_events(tmp.path(), 101..=200, 256);
        *shared.write() = full_replay(200);
        let info = snapper.run_once().unwrap().unwrap();
        assert!(info.wal_segments_deleted > 0);

        // Even if the newest snapshot is lost, older snapshot + WAL suffice.
        fs_remove(&snap_dir.join("snap-00000000000000000200.bin"));
        let (state, _, report) = recover(tmp.path(), 0).unwrap();
        assert_eq!(report.snapshot_offset, Some(100));
        assert_eq!(state, full_replay(200));

        // A third snapshot drops the oldest.
        write_events(tmp.path(), 201..=250, 256);
        *shared.write() = full_replay(250);
        snapper.run_once().unwrap();
        snapper.run_once().unwrap();
        assert_eq!(snapshot::list(&snap_dir).unwrap().len(), snapshot::KEEP);
    }

    #[test]
    fn wal_gap_after_snapshot_refuses_to_start() {
        let tmp = tempfile::tempdir().unwrap();
        let (_, wal_dir) = dirs(tmp.path());
        write_events(tmp.path(), 1..=50, 256);
        // Lose the WAL head with no snapshot covering it.
        let first = wal::list_segments(&wal_dir).unwrap().remove(0);
        fs_remove(&first);
        let Err(err) = recover(tmp.path(), 0) else { panic!("expected a WAL gap error") };
        assert!(err.to_string().contains("WAL gap"), "{err}");
    }

    #[test]
    fn snapshot_ahead_of_wal_keeps_offsets_unique() {
        let tmp = tempfile::tempdir().unwrap();
        let (snap_dir, wal_dir) = dirs(tmp.path());
        write_events(tmp.path(), 1..=10, SEGMENT_BYTES);
        Snapshotter::new(snap_dir, wal_dir, Arc::new(RwLock::new(full_replay(15)))).run_once().unwrap();
        let (state, mut wal, report) = recover(tmp.path(), 0).unwrap();
        assert_eq!(report.replayed, 0);
        assert_eq!(state.last_offset, 15);
        assert_eq!(wal.append([ev(16)]).unwrap()[0].offset, 16);
    }

    fn fs_remove(p: &Path) {
        std::fs::remove_file(p).unwrap();
    }
}
