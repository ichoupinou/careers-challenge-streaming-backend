//! `State`-level tests, including the core property: the read model is a
//! function of the event *set*, not of the arrival order.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::*;
use crate::event::{Event, Kind, SleepState};

const MIN: Millis = 60_000;
const NOW: Millis = 1_779_562_429_123;

fn ev(device: &str, room: &str, ts_ms: Millis, seq: u64, kind: Kind) -> Event {
    Event { device_id: device.into(), room_id: room.into(), ts_ms, seq, kind }
}

fn apply_all(events: &[Event], window_ms: Millis) -> State {
    let mut s = State::new(window_ms);
    for (i, e) in events.iter().enumerate() {
        s.apply(e, i as u64 + 1, NOW);
    }
    s
}

#[test]
fn fall_jitter_copies_emit_one_alarm() {
    let mut s = State::new(0);
    let fall = |seq| ev("dev_1", "room_0", NOW - 1_000, seq, Kind::FallWarn { confidence: 0.92 });
    let first = s.apply(&fall(1), 1, NOW);
    let Applied::Alarm(alarm) = first else { panic!("expected alarm, got {first:?}") };
    assert_eq!(alarm.alarm_id, 1);
    assert_eq!(alarm.ts_ms, NOW - 1_000, "keeps the original ts");
    assert_eq!(s.apply(&fall(2), 2, NOW), Applied::DuplicateFall { alarm_id: 1 });
    assert_eq!(s.apply(&fall(3), 3, NOW), Applied::DuplicateFall { alarm_id: 1 });
    assert_eq!(s.alarms().len(), 1);
    assert_eq!(s.stats.falls_deduped, 2);

    // A different fall on the same device 1ms later is distinct.
    let other = ev("dev_1", "room_0", NOW - 999, 4, Kind::FallWarn { confidence: 0.9 });
    assert!(matches!(s.apply(&other, 4, NOW), Applied::Alarm(a) if a.alarm_id == 2));
}

#[test]
fn health_and_occupancy_end_to_end() {
    let mut s = State::new(0);
    for i in 0..300 {
        s.apply(&ev("dev_1", "room_0", NOW - i * 1_000, i as u64, Kind::Heartbeat), 1, NOW);
    }
    s.apply(&ev("dev_1", "room_0", NOW - 30 * MIN, 400, Kind::Presence { in_room: true }), 1, NOW);
    s.apply(&ev("dev_1", "room_0", NOW - 2_000, 401, Kind::NetStatus { rssi: -68 }), 1, NOW);

    let h = s.device_health("dev_1", NOW);
    assert_eq!(h.availability_5m, 1.0);
    assert_eq!(h.last_heartbeat_ms, Some(NOW));
    assert_eq!(h.room_id.as_deref(), Some("room_0"));
    assert_eq!(h.rssi, Some(-68));

    let o = s.room_occupancy("room_0", NOW, 60 * MIN);
    assert!(o.in_room);
    assert!((o.occupied_pct - 0.5).abs() < 1e-9);

    // Unknown ids are empty, not errors.
    assert_eq!(s.device_health("dev_9999", NOW).availability_5m, 0.0);
    assert!(!s.room_occupancy("room_999", NOW, MIN).in_room);
}

#[test]
fn prune_preserves_reads() {
    let mut s = State::new(0);
    s.apply(&ev("d", "r", NOW - 3 * 60 * MIN, 1, Kind::Presence { in_room: true }), 1, NOW - 3 * 60 * MIN);
    s.apply(&ev("d", "r", NOW - 10 * MIN, 2, Kind::Presence { in_room: false }), 2, NOW);
    s.apply(&ev("d", "r", NOW - 3 * 60 * MIN, 3, Kind::FallWarn { confidence: 0.9 }), 3, NOW - 3 * 60 * MIN);
    let before = s.room_occupancy("r", NOW, 60 * MIN);
    s.prune(NOW, 2 * 60 * MIN);
    assert_eq!(s.room_occupancy("r", NOW, 60 * MIN), before);
    assert!(s.alarms().is_empty(), "alarm older than retention pruned");
    assert_eq!(s.alarms().last_id(), 1, "ids keep counting");
}

// ---------------------------------------------------------------- property

const DEVICES: [&str; 4] = ["dev_0", "dev_1", "dev_2", "dev_3"];

fn arb_kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        4 => Just(Kind::Heartbeat),
        3 => any::<bool>().prop_map(|in_room| Kind::Presence { in_room }),
        2 => (0u8..=3).prop_map(|c| Kind::FallWarn { confidence: 0.7 + c as f64 / 10.0 }),
        1 => (0u8..=10).prop_map(|m| Kind::Motion { magnitude: m as f64 / 10.0 }),
        1 => prop_oneof![Just(SleepState::Asleep), Just(SleepState::Awake)].prop_map(|state| Kind::SleepState { state }),
        1 => (-90i32..=-50).prop_map(|rssi| Kind::NetStatus { rssi }),
    ]
}

/// Events across 4 devices / 2 rooms, `ts` coarsened to 500ms steps over
/// the last ~65 minutes plus some future skew, so collisions (jitter
/// duplicates, same-ts presence from two devices) actually happen.
fn arb_events() -> impl Strategy<Value = Vec<Event>> {
    prop::collection::vec((0usize..4, -7_800i64..=120, arb_kind()), 1..120).prop_map(|raw| {
        raw.into_iter()
            .enumerate()
            .map(|(i, (d, half_secs, kind))| {
                let room = if d < 2 { "room_0" } else { "room_1" };
                // Same device + ts + fall must look like a jitter copy.
                ev(DEVICES[d], room, NOW + half_secs * 500, i as u64, kind)
            })
            .collect()
    })
}

/// Everything a client can read, in a comparable form.
fn read_model(s: &State) -> (Vec<String>, BTreeSet<String>) {
    let mut out = Vec::new();
    for d in DEVICES {
        let h = s.device_health(d, NOW);
        out.push(format!("{d} {:?} {} {:?} {:?} {:?} {:?}", h.last_heartbeat_ms, h.heartbeats_5m, h.room_id, h.motion, h.sleep_state, h.rssi));
    }
    for r in ["room_0", "room_1"] {
        for w in [MIN, 5 * MIN, 60 * MIN] {
            let o = s.room_occupancy(r, NOW, w);
            out.push(format!("{r} {w} {} {:.9}", o.in_room, o.occupied_pct));
        }
    }
    let alarms = s.alarms().after(0, usize::MAX).map(|a| a.event_id.clone()).collect();
    (out, alarms)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn reads_are_independent_of_arrival_order(
        events in arb_events(),
        perm_seed in any::<u64>(),
    ) {
        let mut shuffled = events.clone();
        // Fisher-Yates driven by the seed (keeps the case reproducible).
        let mut x = perm_seed | 1;
        for i in (1..shuffled.len()).rev() {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            shuffled.swap(i, (x % (i as u64 + 1)) as usize);
        }
        let a = apply_all(&events, 0);
        let b = apply_all(&shuffled, 0);
        prop_assert_eq!(read_model(&a), read_model(&b));
        prop_assert_eq!(a.alarms().len(), b.alarms().len());
    }

    #[test]
    fn pruning_mid_stream_does_not_change_reads(events in arb_events(), split in 0usize..120) {
        let full = apply_all(&events, 0);
        let mut pruned = State::new(0);
        for (i, e) in events.iter().enumerate() {
            if i == split.min(events.len()) {
                pruned.prune(NOW, 24 * 60 * MIN);
            }
            pruned.apply(e, i as u64 + 1, NOW);
        }
        pruned.prune(NOW, 24 * 60 * MIN);
        prop_assert_eq!(read_model(&full).0, read_model(&pruned).0);
    }
}
