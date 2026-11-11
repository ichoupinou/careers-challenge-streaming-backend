//! Injectable wall clock, so time-window logic is testable.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Milliseconds since the Unix epoch.
pub type Millis = i64;

pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> Millis;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> Millis {
        chrono::Utc::now().timestamp_millis()
    }
}

/// Manually driven clock for tests.
#[derive(Debug, Clone)]
pub struct MockClock(Arc<AtomicI64>);

impl MockClock {
    pub fn new(start_ms: Millis) -> Self {
        Self(Arc::new(AtomicI64::new(start_ms)))
    }

    pub fn set(&self, ms: Millis) {
        self.0.store(ms, Ordering::SeqCst);
    }

    pub fn advance(&self, delta_ms: Millis) {
        self.0.fetch_add(delta_ms, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_ms(&self) -> Millis {
        self.0.load(Ordering::SeqCst)
    }
}
