//! In-memory materialized state, built by applying accepted events.
//!
//! Invariant: device health and room occupancy depend only on the *set* of
//! applied events (keyed by `ts`), never on arrival order. With the default
//! exact-match dedup (`window_ms = 0`), the set of emitted alarms
//! (`event_id`s) is order-independent too. Only `alarm_id` (emission order)
//! depends on order, and it's deterministic given the WAL order.

pub mod alarms;
pub mod device;
pub mod falls;
pub mod ring;
pub mod room;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::clock::Millis;
use crate::event::{Event, Kind, MAX_PAST_MS};
use alarms::{Alarm, AlarmStore};
use device::{DeviceState, Reading};
use falls::FallDedup;
use room::{Occupancy, PresenceKey, RoomPresence};

/// Presence history kept: the 1h max window plus the 1h late-event allowance.
pub const PRESENCE_RETENTION_MS: Millis = 2 * 3_600_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct State {
    devices: HashMap<String, DeviceState>,
    rooms: HashMap<String, RoomPresence>,
    falls: FallDedup,
    alarms: AlarmStore,
    /// Offset of the last applied event (0 = none).
    pub last_offset: u64,
    pub stats: Stats,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    pub applied: u64,
    pub falls_deduped: u64,
}

/// Effect of applying one event.
#[derive(Debug, Clone, PartialEq)]
pub enum Applied {
    Plain,
    /// A new distinct fall: emit this alarm.
    Alarm(Alarm),
    /// A fall that duplicates `alarm_id`.
    DuplicateFall { alarm_id: u64 },
}

/// Read model for `/devices/{id}/health`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeviceHealth {
    pub last_heartbeat_ms: Option<Millis>,
    pub availability_5m: f64,
    pub heartbeats_5m: u32,
    pub room_id: Option<String>,
    pub motion: Option<f64>,
    pub sleep_state: Option<crate::event::SleepState>,
    pub rssi: Option<i32>,
}

impl State {
    pub fn new(dedup_window_ms: Millis) -> Self {
        Self {
            devices: HashMap::new(),
            rooms: HashMap::new(),
            falls: FallDedup::new(dedup_window_ms),
            alarms: AlarmStore::default(),
            last_offset: 0,
            stats: Stats::default(),
        }
    }

    /// Config wins over whatever window a restored snapshot carried.
    pub fn set_dedup_window(&mut self, window_ms: Millis) {
        self.falls.set_window(window_ms);
    }

    pub fn apply(&mut self, ev: &Event, offset: u64, recv_ms: Millis) -> Applied {
        self.last_offset = offset;
        self.stats.applied += 1;

        let dev = self.devices.entry(ev.device_id.clone()).or_default();
        dev.seen_in(&ev.room_id, ev.ts_ms, ev.seq);

        match ev.kind {
            Kind::Heartbeat => dev.heartbeat(ev.ts_ms),
            Kind::Motion { magnitude } => dev.motion(reading(ev, magnitude)),
            Kind::SleepState { state } => dev.sleep(reading(ev, state)),
            Kind::NetStatus { rssi } => dev.rssi(reading(ev, rssi)),
            Kind::Presence { in_room } => {
                let key = PresenceKey { ts_ms: ev.ts_ms, device_id: ev.device_id.clone(), seq: ev.seq };
                self.rooms.entry(ev.room_id.clone()).or_default().insert(key, in_room);
            }
            Kind::FallWarn { confidence } => {
                if let Some(alarm_id) = self.falls.find(&ev.device_id, ev.ts_ms) {
                    self.stats.falls_deduped += 1;
                    return Applied::DuplicateFall { alarm_id };
                }
                let alarm = self.alarms.push(&ev.device_id, &ev.room_id, ev.ts_ms, confidence, recv_ms).clone();
                self.falls.record(&ev.device_id, ev.ts_ms, alarm.alarm_id);
                return Applied::Alarm(alarm);
            }
        }
        Applied::Plain
    }

    /// Drop history no read or future insert can need.
    pub fn prune(&mut self, now: Millis, alarm_retention_ms: Millis) {
        for room in self.rooms.values_mut() {
            room.prune(now - PRESENCE_RETENTION_MS);
        }
        // A new fall is at most 1h old, so it can only match falls down to
        // `now - 1h - window`. A minute of slack covers clock jitter.
        self.falls.prune(now - MAX_PAST_MS - self.falls.window_ms() - 60_000);
        self.alarms.prune(now - alarm_retention_ms);
    }

    pub fn device_health(&self, device_id: &str, now: Millis) -> DeviceHealth {
        let d = self.devices.get(device_id);
        DeviceHealth {
            last_heartbeat_ms: d.and_then(|d| d.last_heartbeat_ms),
            availability_5m: d.map_or(0.0, |d| d.availability_5m(now)),
            heartbeats_5m: d.map_or(0, |d| d.heartbeats_5m(now)),
            room_id: d.and_then(|d| d.room.as_ref().map(|r| r.value.clone())),
            motion: d.and_then(|d| d.motion.as_ref().map(|r| r.value)),
            sleep_state: d.and_then(|d| d.sleep.as_ref().map(|r| r.value)),
            rssi: d.and_then(|d| d.rssi.as_ref().map(|r| r.value)),
        }
    }

    pub fn room_occupancy(&self, room_id: &str, now: Millis, window_ms: Millis) -> Occupancy {
        self.rooms
            .get(room_id)
            .map_or(Occupancy { in_room: false, occupied_pct: 0.0 }, |r| r.occupancy(now, window_ms))
    }

    pub fn alarms(&self) -> &AlarmStore {
        &self.alarms
    }

    pub fn device_count(&self) -> usize {
        self.devices.len()
    }

    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }
}

fn reading<T>(ev: &Event, value: T) -> Reading<T> {
    Reading { ts_ms: ev.ts_ms, seq: ev.seq, value }
}

#[cfg(test)]
mod tests;
