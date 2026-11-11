//! Fall-warning deduplication.
//!
//! The generator's jitter copies share `device_id`, `ts` (to the ms) and
//! `confidence`; only `seq` differs. With `window_ms = 0` the key is the exact
//! `(device_id, ts)` pair, which is order-independent and never merges real
//! falls. A wider window treats any fall within ±window of an accepted fall
//! on the same device as a duplicate. That copes with timestamp jitter but
//! can merge distinct falls during bursts, and the result can depend on
//! arrival order when falls form chains.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::clock::Millis;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FallDedup {
    window_ms: Millis,
    /// device_id → accepted fall ts → alarm_id.
    accepted: HashMap<String, BTreeMap<Millis, u64>>,
}

impl FallDedup {
    pub fn new(window_ms: Millis) -> Self {
        Self { window_ms: window_ms.max(0), accepted: HashMap::new() }
    }

    /// Change the window (e.g. config changed since the snapshot was taken).
    /// Applies to falls seen from now on.
    pub fn set_window(&mut self, window_ms: Millis) {
        self.window_ms = window_ms.max(0);
    }

    pub fn window_ms(&self) -> Millis {
        self.window_ms
    }

    /// The alarm this fall duplicates, if any.
    pub fn find(&self, device_id: &str, ts_ms: Millis) -> Option<u64> {
        self.accepted
            .get(device_id)?
            .range(ts_ms - self.window_ms..=ts_ms + self.window_ms)
            .next()
            .map(|(_, id)| *id)
    }

    pub fn record(&mut self, device_id: &str, ts_ms: Millis, alarm_id: u64) {
        self.accepted.entry(device_id.to_owned()).or_default().insert(ts_ms, alarm_id);
    }

    /// Forget falls older than `cutoff`. Only safe once events that old can
    /// no longer be accepted.
    pub fn prune(&mut self, cutoff: Millis) {
        self.accepted.retain(|_, m| {
            *m = m.split_off(&cutoff);
            !m.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match_by_default() {
        let mut d = FallDedup::new(0);
        assert_eq!(d.find("a", 1_000), None);
        d.record("a", 1_000, 1);
        assert_eq!(d.find("a", 1_000), Some(1));
        assert_eq!(d.find("a", 1_001), None, "1ms apart is a different fall");
        assert_eq!(d.find("b", 1_000), None, "different device");
    }

    #[test]
    fn window_matches_either_side() {
        let mut d = FallDedup::new(2_000);
        d.record("a", 10_000, 7);
        assert_eq!(d.find("a", 8_000), Some(7));
        assert_eq!(d.find("a", 12_000), Some(7));
        assert_eq!(d.find("a", 12_001), None);
    }

    #[test]
    fn prune_drops_old_entries() {
        let mut d = FallDedup::new(0);
        d.record("a", 100, 1);
        d.record("a", 200, 2);
        d.prune(150);
        assert_eq!(d.find("a", 100), None);
        assert_eq!(d.find("a", 200), Some(2));
        d.prune(1_000);
        assert!(d.accepted.is_empty());
    }
}
