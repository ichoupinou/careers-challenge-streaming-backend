//! Per-room presence history and time-weighted occupancy.
//!
//! All presence events for a room are kept in a map ordered by
//! `(ts, device_id, seq)`. The room's state at any instant is the value of the
//! latest event at or before it ("latest presence wins", by `ts`). Windows
//! are integrated at read time, so a late event is just one more insert and
//! the history is correct by construction.
//!
//! Events older than the prune cutoff are folded into `carry_in`, which
//! keeps the max key among everything pruned. That stays correct even when
//! an old event arrives after pruning (e.g. WAL replay), so the result
//! never depends on arrival order.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::clock::Millis;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PresenceKey {
    pub ts_ms: Millis,
    pub device_id: String,
    pub seq: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RoomPresence {
    transitions: BTreeMap<PresenceKey, bool>,
    /// Latest event among those pruned (all older than `pruned_before`).
    carry_in: Option<(PresenceKey, bool)>,
    /// Everything with `ts < pruned_before` lives in `carry_in`.
    pruned_before: Millis,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Occupancy {
    pub in_room: bool,
    pub occupied_pct: f64,
}

impl RoomPresence {
    pub fn insert(&mut self, key: PresenceKey, in_room: bool) {
        if key.ts_ms < self.pruned_before {
            Self::fold(&mut self.carry_in, key, in_room);
        } else {
            self.transitions.insert(key, in_room);
        }
    }

    /// Latest presence by `ts` wins. No presence ever seen → not occupied.
    pub fn in_room(&self) -> bool {
        self.transitions
            .last_key_value()
            .map(|(_, v)| *v)
            .or(self.carry_in.as_ref().map(|(_, v)| *v))
            .unwrap_or(false)
    }

    /// Room state at instant `t`, from the latest event with `ts <= t`.
    fn state_at(&self, t: Millis) -> bool {
        let upper = PresenceKey { ts_ms: t, device_id: String::new(), seq: 0 };
        // Every key with ts <= t is < (t+1, "", 0).
        let upper_excl = PresenceKey { ts_ms: t + 1, ..upper };
        self.transitions
            .range(..upper_excl)
            .next_back()
            .map(|(_, v)| *v)
            .or(self.carry_in.as_ref().map(|(_, v)| *v))
            .unwrap_or(false)
    }

    /// Fraction of `[now - window, now]` during which the room was occupied.
    pub fn occupancy(&self, now: Millis, window_ms: Millis) -> Occupancy {
        let start = now - window_ms;
        let mut state = self.state_at(start);
        let mut cursor = start;
        let mut occupied: Millis = 0;
        let lo = PresenceKey { ts_ms: start + 1, device_id: String::new(), seq: 0 };
        let hi = PresenceKey { ts_ms: now + 1, device_id: String::new(), seq: 0 };
        for (k, &v) in self.transitions.range(lo..hi) {
            if state {
                occupied += k.ts_ms - cursor;
            }
            cursor = k.ts_ms;
            state = v;
        }
        if state {
            occupied += now - cursor;
        }
        let pct = if window_ms > 0 { occupied as f64 / window_ms as f64 } else { 0.0 };
        Occupancy { in_room: self.in_room(), occupied_pct: pct.clamp(0.0, 1.0) }
    }

    /// Fold every transition with `ts < cutoff` into `carry_in`.
    pub fn prune(&mut self, cutoff: Millis) {
        if cutoff <= self.pruned_before {
            return;
        }
        let split = PresenceKey { ts_ms: cutoff, device_id: String::new(), seq: 0 };
        let keep = self.transitions.split_off(&split);
        let old = std::mem::replace(&mut self.transitions, keep);
        if let Some((k, v)) = old.into_iter().next_back() {
            Self::fold(&mut self.carry_in, k, v);
        }
        self.pruned_before = cutoff;
    }

    pub fn is_empty(&self) -> bool {
        self.transitions.is_empty() && self.carry_in.is_none()
    }

    fn fold(carry: &mut Option<(PresenceKey, bool)>, key: PresenceKey, v: bool) {
        if carry.as_ref().is_none_or(|(ck, _)| key > *ck) {
            *carry = Some((key, v));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: Millis = 60_000;
    const NOW: Millis = 1_000 * MIN;

    fn key(ts: Millis, dev: &str) -> PresenceKey {
        PresenceKey { ts_ms: ts, device_id: dev.into(), seq: 0 }
    }

    #[test]
    fn empty_room_is_unoccupied() {
        let r = RoomPresence::default();
        assert_eq!(r.occupancy(NOW, 60 * MIN), Occupancy { in_room: false, occupied_pct: 0.0 });
    }

    #[test]
    fn integrates_step_function() {
        let mut r = RoomPresence::default();
        r.insert(key(NOW - 90 * MIN, "a"), true); // before the 1h window: carries in
        r.insert(key(NOW - 30 * MIN, "a"), false);
        r.insert(key(NOW - 10 * MIN, "b"), true);
        let o = r.occupancy(NOW, 60 * MIN);
        assert!(o.in_room);
        // occupied [-60,-30] + [-10,0] = 40 of 60 minutes
        assert!((o.occupied_pct - 40.0 / 60.0).abs() < 1e-9);
        assert!((r.occupancy(NOW, 5 * MIN).occupied_pct - 1.0).abs() < 1e-9);
    }

    #[test]
    fn late_event_fixes_history() {
        // Device was offline 20 minutes; its "occupied" event arrives late.
        let mut r = RoomPresence::default();
        r.insert(key(NOW - 60 * MIN, "a"), false);
        r.insert(key(NOW - 5 * MIN, "a"), false);
        assert_eq!(r.occupancy(NOW, 60 * MIN).occupied_pct, 0.0);
        r.insert(key(NOW - 25 * MIN, "a"), true); // replayed on reconnect
        let pct = r.occupancy(NOW, 60 * MIN).occupied_pct;
        assert!((pct - 20.0 / 60.0).abs() < 1e-9, "got {pct}");
    }

    #[test]
    fn latest_by_ts_wins_not_arrival() {
        let mut r = RoomPresence::default();
        r.insert(key(NOW - MIN, "a"), true);
        r.insert(key(NOW - 2 * MIN, "b"), false); // arrives later but is older
        assert!(r.in_room());
    }

    #[test]
    fn prune_keeps_results_and_handles_post_prune_inserts() {
        let mut a = RoomPresence::default();
        a.insert(key(NOW - 150 * MIN, "x"), true);
        a.insert(key(NOW - 130 * MIN, "x"), false);
        a.insert(key(NOW - 30 * MIN, "x"), true);
        let before = a.occupancy(NOW, 60 * MIN);
        a.prune(NOW - 120 * MIN);
        assert_eq!(a.occupancy(NOW, 60 * MIN), before);

        // An old event arriving after the prune is folded by key, not arrival.
        a.insert(key(NOW - 140 * MIN, "x"), true);
        let mut b = RoomPresence::default();
        b.insert(key(NOW - 130 * MIN, "x"), false);
        b.insert(key(NOW - 30 * MIN, "x"), true);
        assert_eq!(a.occupancy(NOW, 60 * MIN), b.occupancy(NOW, 60 * MIN));
        assert!(!a.state_at(NOW - 61 * MIN));
    }

    #[test]
    fn future_events_count_for_in_room_but_not_window() {
        let mut r = RoomPresence::default();
        r.insert(key(NOW + 10 * MIN, "a"), true); // skewed clock
        let o = r.occupancy(NOW, 60 * MIN);
        assert!(o.in_room);
        assert_eq!(o.occupied_pct, 0.0);
    }
}
