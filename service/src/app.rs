//! Service assembly: open the WAL, rebuild state, start the pipeline.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::api::AppState;
use crate::clock::Clock;
use crate::config::Config;
use crate::ingest::{self, Pipeline};
use crate::state::State;
use crate::wal::{self, Wal};

/// A running service: shared state for the HTTP layer plus the pipeline
/// threads (call `Pipeline::shutdown` to drain and fsync on exit).
pub struct Service {
    pub app: Arc<AppState>,
    pub pipeline: Pipeline,
}

/// Recover state from the WAL in `config.data_dir`, then start ingesting.
///
/// TODO(milestone 4): load the latest snapshot first and only replay the
/// WAL tail after it.
pub fn start(config: Config, clock: Arc<dyn Clock>) -> io::Result<Service> {
    let wal_dir = config.data_dir.join("wal");
    let started = Instant::now();

    // Opening first truncates a torn tail left by a crash.
    let (wal, report) = Wal::open(&wal_dir)?;
    let mut state = State::new(config.dedup_window_ms);
    // Replayed events were validated when accepted: apply them as-is.
    let replayed = wal::replay(&wal_dir, 0, |rec| {
        state.apply(&rec.event, rec.offset, rec.recv_ms);
    })?;
    tracing::info!(
        segments = report.segments,
        replayed,
        last_offset = report.last_offset,
        truncated_bytes = report.truncated_bytes,
        alarms = state.alarms().len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "recovered from WAL"
    );

    let state = Arc::new(RwLock::new(state));
    let (ingest, lanes) = ingest::lanes(&config);
    let pipeline = ingest::start(lanes, wal, state.clone(), ingest.stats.clone(), &config);
    let app = Arc::new(AppState { config, clock, ready: AtomicBool::new(false), state, ingest });
    app.ready.store(true, Ordering::Release);
    Ok(Service { app, pipeline })
}
