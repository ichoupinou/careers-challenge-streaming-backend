//! Emitted (deduplicated) fall alarms, in emission order.
//!
//! `alarm_id` is a dense, monotonic counter starting at 1, assigned in apply
//! (WAL) order. Replaying the WAL therefore rebuilds the same ids, and they
//! act as the resumable cursor for `/alarms?since=` and the SSE feed.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::clock::Millis;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Alarm {
    pub alarm_id: u64,
    pub event_id: String,
    pub device_id: String,
    pub room_id: String,
    /// Original device timestamp of the fall.
    pub ts_ms: Millis,
    pub confidence: f64,
    /// Server time the event was accepted (for latency metrics and retention).
    pub recv_ms: Millis,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlarmStore {
    alarms: VecDeque<Alarm>,
    next_id: u64,
}

impl Default for AlarmStore {
    fn default() -> Self {
        Self { alarms: VecDeque::new(), next_id: 1 }
    }
}

impl AlarmStore {
    pub fn push(&mut self, device_id: &str, room_id: &str, ts_ms: Millis, confidence: f64, recv_ms: Millis) -> &Alarm {
        let alarm_id = self.next_id;
        self.next_id += 1;
        self.alarms.push_back(Alarm {
            alarm_id,
            event_id: format!("{device_id}:{ts_ms}"),
            device_id: device_id.to_owned(),
            room_id: room_id.to_owned(),
            ts_ms,
            confidence,
            recv_ms,
        });
        self.alarms.back().expect("just pushed")
    }

    /// Alarms with `alarm_id > cursor`, oldest first, at most `limit`.
    pub fn after(&self, cursor: u64, limit: usize) -> impl Iterator<Item = &Alarm> {
        let start = self.alarms.partition_point(|a| a.alarm_id <= cursor);
        self.alarms.range(start..).take(limit)
    }

    /// Alarms whose original `ts >= ts_ms`, in emission order, at most `limit`.
    pub fn since_ts(&self, ts_ms: Millis, limit: usize) -> impl Iterator<Item = &Alarm> {
        self.alarms.iter().filter(move |a| a.ts_ms >= ts_ms).take(limit)
    }

    /// Highest id handed out so far (0 if none).
    pub fn last_id(&self) -> u64 {
        self.next_id - 1
    }

    pub fn len(&self) -> usize {
        self.alarms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.alarms.is_empty()
    }

    /// Drop alarms accepted before `cutoff`. Ids keep counting up.
    pub fn prune(&mut self, cutoff: Millis) {
        while self.alarms.front().is_some_and(|a| a.recv_ms < cutoff) {
            self.alarms.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_and_ts_reads() {
        let mut s = AlarmStore::default();
        assert_eq!(s.last_id(), 0);
        s.push("a", "r", 300, 0.9, 10);
        s.push("b", "r", 100, 0.8, 20);
        s.push("a", "r", 200, 0.7, 30);
        let ids = |it: Vec<&Alarm>| it.iter().map(|a| a.alarm_id).collect::<Vec<_>>();
        assert_eq!(ids(s.after(0, usize::MAX).collect()), [1, 2, 3]);
        assert_eq!(ids(s.after(2, usize::MAX).collect()), [3]);
        assert_eq!(ids(s.after(1, 1).collect()), [2]);
        assert_eq!(ids(s.since_ts(200, usize::MAX).collect()), [1, 3]);
        assert_eq!(s.after(0, 10).next().unwrap().event_id, "a:300");
    }

    #[test]
    fn prune_keeps_ids_monotonic() {
        let mut s = AlarmStore::default();
        s.push("a", "r", 1, 0.9, 10);
        s.push("a", "r", 2, 0.9, 20);
        s.prune(15);
        assert_eq!(s.len(), 1);
        assert_eq!(s.push("a", "r", 3, 0.9, 30).alarm_id, 3);
        assert_eq!(s.after(0, 10).map(|a| a.alarm_id).collect::<Vec<_>>(), [2, 3]);
    }
}
