//! Per-device heartbeat bitmap: one bit per epoch second.
//!
//! Covers the `RING_SECS` seconds ending at `head_sec` (the newest second
//! seen). Marking is idempotent, so duplicates and replays never inflate
//! availability, and late heartbeats up to ~2h old still land. Advancing the
//! head lazily clears the bits that fall out of range. 1 KiB per device.

use serde::{Deserialize, Serialize};

/// 8192 s ≈ 2h16m: covers the full ±1h acceptance window around "now".
pub const RING_SECS: i64 = 8192;
const WORDS: usize = (RING_SECS / 64) as usize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatRing {
    bits: Vec<u64>,
    /// Newest second covered; `None` until the first mark.
    head_sec: Option<i64>,
}

impl Default for HeartbeatRing {
    fn default() -> Self {
        Self { bits: vec![0; WORDS], head_sec: None }
    }
}

impl HeartbeatRing {
    /// Record a heartbeat in second `sec`. Returns false if `sec` is older
    /// than the ring covers (ignored).
    pub fn mark(&mut self, sec: i64) -> bool {
        match self.head_sec {
            None => self.head_sec = Some(sec),
            Some(head) if sec > head => {
                self.clear_range(head + 1, sec);
                self.head_sec = Some(sec);
            }
            Some(head) if sec <= head - RING_SECS => return false,
            Some(_) => {}
        }
        let (w, b) = Self::slot(sec);
        self.bits[w] |= 1 << b;
        true
    }

    /// Number of marked seconds in `(from_excl, to_incl]`.
    pub fn count(&self, from_excl: i64, to_incl: i64) -> u32 {
        let Some(head) = self.head_sec else { return 0 };
        let lo = (from_excl + 1).max(head - RING_SECS + 1);
        let hi = to_incl.min(head);
        (lo..=hi).filter(|&s| self.is_set(s)).count() as u32
    }

    fn is_set(&self, sec: i64) -> bool {
        let (w, b) = Self::slot(sec);
        self.bits[w] & (1 << b) != 0
    }

    /// Clear seconds `lo..=hi` (which are all newer than the old head).
    fn clear_range(&mut self, lo: i64, hi: i64) {
        if hi - lo + 1 >= RING_SECS {
            self.bits.iter_mut().for_each(|w| *w = 0);
            return;
        }
        for s in lo..=hi {
            let (w, b) = Self::slot(s);
            self.bits[w] &= !(1 << b);
        }
    }

    fn slot(sec: i64) -> (usize, u32) {
        let i = sec.rem_euclid(RING_SECS);
        ((i / 64) as usize, (i % 64) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotent_and_windowed() {
        let mut r = HeartbeatRing::default();
        assert_eq!(r.count(0, 1_000), 0);
        for s in [100, 100, 101, 105, 105] {
            assert!(r.mark(s));
        }
        assert_eq!(r.count(99, 105), 3);
        assert_eq!(r.count(100, 105), 2, "lower bound is exclusive");
        assert_eq!(r.count(99, 104), 2, "upper bound is inclusive");
    }

    #[test]
    fn late_marks_land_until_out_of_range() {
        let mut r = HeartbeatRing::default();
        let now = 1_000_000;
        r.mark(now);
        assert!(r.mark(now - 3_600), "1h-late heartbeat lands");
        assert_eq!(r.count(now - 3_601, now), 2);
        assert!(!r.mark(now - RING_SECS), "older than the ring is ignored");
    }

    #[test]
    fn advancing_head_clears_wrapped_slots() {
        let mut r = HeartbeatRing::default();
        r.mark(10);
        // Same slot as 10 one full lap later: the old bit must not survive.
        r.mark(10 + RING_SECS + 1);
        assert_eq!(r.count(RING_SECS, 10 + RING_SECS), 0);
        assert_eq!(r.count(0, 20 + RING_SECS), 1);
        // Big jump forward clears everything.
        r.mark(10 * RING_SECS);
        assert_eq!(r.count(0, 10 * RING_SECS), 1);
    }

    #[test]
    fn negative_seconds() {
        let mut r = HeartbeatRing::default();
        r.mark(-5);
        r.mark(-1);
        assert_eq!(r.count(-10, 0), 2);
    }
}
