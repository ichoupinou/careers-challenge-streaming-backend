//! Per-device health: latest heartbeat plus 5-minute availability.

use serde::{Deserialize, Serialize};

use super::ring::HeartbeatRing;
use crate::clock::Millis;
use crate::event::SleepState;

/// Heartbeats are expected at ~1 Hz, so availability is the fraction of
/// seconds in the window that saw at least one heartbeat.
pub const AVAILABILITY_WINDOW_SECS: i64 = 300;

/// A value with the `(ts, seq)` of the event that carried it. Updates keep
/// the greatest `(ts, seq)`, so arrival order doesn't matter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reading<T> {
    pub ts_ms: Millis,
    pub seq: u64,
    pub value: T,
}

fn keep_latest<T>(slot: &mut Option<Reading<T>>, r: Reading<T>) {
    if slot.as_ref().is_none_or(|cur| (r.ts_ms, r.seq) > (cur.ts_ms, cur.seq)) {
        *slot = Some(r);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DeviceState {
    pub last_heartbeat_ms: Option<Millis>,
    heartbeats: HeartbeatRing,
    /// Current room: from the device's latest event by `(ts, seq)`.
    pub room: Option<Reading<String>>,
    pub motion: Option<Reading<f64>>,
    pub sleep: Option<Reading<SleepState>>,
    pub rssi: Option<Reading<i32>>,
}

impl DeviceState {
    pub fn heartbeat(&mut self, ts_ms: Millis) {
        self.last_heartbeat_ms = Some(self.last_heartbeat_ms.map_or(ts_ms, |cur| cur.max(ts_ms)));
        self.heartbeats.mark(ts_ms.div_euclid(1000));
    }

    pub fn seen_in(&mut self, room_id: &str, ts_ms: Millis, seq: u64) {
        match &mut self.room {
            Some(cur) if (ts_ms, seq) <= (cur.ts_ms, cur.seq) => {}
            Some(cur) if cur.value == room_id => (cur.ts_ms, cur.seq) = (ts_ms, seq),
            slot => *slot = Some(Reading { ts_ms, seq, value: room_id.to_owned() }),
        }
    }

    pub fn motion(&mut self, r: Reading<f64>) {
        keep_latest(&mut self.motion, r);
    }

    pub fn sleep(&mut self, r: Reading<SleepState>) {
        keep_latest(&mut self.sleep, r);
    }

    pub fn rssi(&mut self, r: Reading<i32>) {
        keep_latest(&mut self.rssi, r);
    }

    /// Seconds with a heartbeat in `(now - 300s, now]`.
    pub fn heartbeats_5m(&self, now_ms: Millis) -> u32 {
        let now_sec = now_ms.div_euclid(1000);
        self.heartbeats.count(now_sec - AVAILABILITY_WINDOW_SECS, now_sec)
    }

    pub fn availability_5m(&self, now_ms: Millis) -> f64 {
        (self.heartbeats_5m(now_ms) as f64 / AVAILABILITY_WINDOW_SECS as f64).min(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: Millis = 1_779_562_429_123;

    #[test]
    fn availability_counts_distinct_seconds_in_window() {
        let mut d = DeviceState::default();
        // 150 heartbeats at 1 Hz in the last 5 minutes, each sent twice.
        for i in 0..150 {
            d.heartbeat(NOW - i * 1_000);
            d.heartbeat(NOW - i * 1_000);
        }
        // Outside the window: ignored.
        d.heartbeat(NOW - 301_000);
        assert_eq!(d.heartbeats_5m(NOW), 150);
        assert!((d.availability_5m(NOW) - 0.5).abs() < 1e-9);
        assert_eq!(d.last_heartbeat_ms, Some(NOW));
    }

    #[test]
    fn late_heartbeats_fill_gaps_and_last_is_max_ts() {
        let mut d = DeviceState::default();
        d.heartbeat(NOW);
        d.heartbeat(NOW - 60_000); // replayed after reconnect
        assert_eq!(d.last_heartbeat_ms, Some(NOW));
        assert_eq!(d.heartbeats_5m(NOW), 2);
        assert_eq!(d.availability_5m(NOW + 10 * 60_000), 0.0);
    }

    #[test]
    fn latest_reading_by_ts_then_seq() {
        let mut d = DeviceState::default();
        d.rssi(Reading { ts_ms: 10, seq: 2, value: -60 });
        d.rssi(Reading { ts_ms: 5, seq: 9, value: -90 });
        d.rssi(Reading { ts_ms: 10, seq: 1, value: -70 });
        assert_eq!(d.rssi.as_ref().unwrap().value, -60);

        d.seen_in("room_1", 10, 1);
        d.seen_in("room_2", 5, 1);
        assert_eq!(d.room.as_ref().unwrap().value, "room_1");
        d.seen_in("room_2", 11, 1);
        assert_eq!(d.room.as_ref().unwrap().value, "room_2");
    }
}
