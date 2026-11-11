//! Event model and validation (see `docs/event_schema.md`).
//!
//! Validation works on a `serde_json::Value` instead of a derived struct so
//! every rejection carries a precise, loggable reason.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::clock::Millis;

/// Events may be at most this far in the future (broken clock otherwise),
/// and at most this far in the past (offline buffer replay).
pub const MAX_FUTURE_MS: Millis = 3_600_000;
pub const MAX_PAST_MS: Millis = 3_600_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SleepState {
    Asleep,
    Awake,
    Unknown,
}

/// Event type plus its type-specific payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Kind {
    Heartbeat,
    Presence { in_room: bool },
    Motion { magnitude: f64 },
    SleepState { state: SleepState },
    FallWarn { confidence: f64 },
    NetStatus { rssi: i32 },
}

/// Event type names, indexed by `Kind::index`.
pub const KIND_NAMES: [&str; 6] = ["heartbeat", "presence", "motion", "sleep_state", "fall_warn", "net_status"];

impl Kind {
    pub fn index(&self) -> usize {
        match self {
            Kind::Heartbeat => 0,
            Kind::Presence { .. } => 1,
            Kind::Motion { .. } => 2,
            Kind::SleepState { .. } => 3,
            Kind::FallWarn { .. } => 4,
            Kind::NetStatus { .. } => 5,
        }
    }

    pub fn name(&self) -> &'static str {
        KIND_NAMES[self.index()]
    }

    pub fn is_fall(&self) -> bool {
        matches!(self, Kind::FallWarn { .. })
    }
}

/// A validated event. `ts_ms` is the device wall clock, authoritative for
/// ordering and aggregation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub device_id: String,
    pub room_id: String,
    pub ts_ms: Millis,
    /// Per-device monotonic sequence. The schema lists it in the envelope but
    /// the README examples omit it, so it is optional (0 when absent).
    pub seq: u64,
    pub kind: Kind,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum Reject {
    #[error("malformed event: {0}")]
    Malformed(String),
    #[error("ts is more than 1h in the future")]
    TsInFuture,
    #[error("ts is more than 1h in the past")]
    TsTooOld,
}

impl Reject {
    /// Stable short code for metrics and API responses.
    pub fn code(&self) -> &'static str {
        match self {
            Reject::Malformed(_) => "malformed",
            Reject::TsInFuture => "ts_future",
            Reject::TsTooOld => "ts_too_old",
        }
    }
}

fn malformed(msg: impl Into<String>) -> Reject {
    Reject::Malformed(msg.into())
}

/// Validate one JSON value against the schema and the acceptance window
/// `[now - 1h, now + 1h]`.
pub fn validate(v: &Value, now_ms: Millis) -> Result<Event, Reject> {
    let obj = v.as_object().ok_or_else(|| malformed("event must be a JSON object"))?;

    let device_id = non_empty_str(obj, "device_id")?;
    let room_id = non_empty_str(obj, "room_id")?;
    let ts_raw = obj
        .get("ts")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("missing or non-string field `ts`"))?;
    let ts_ms = parse_ts(ts_raw).ok_or_else(|| malformed(format!("`ts` is not RFC 3339: {ts_raw:?}")))?;
    let seq = match obj.get("seq") {
        None | Some(Value::Null) => 0,
        Some(s) => s.as_u64().ok_or_else(|| malformed("`seq` must be a non-negative integer"))?,
    };

    let ty = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("missing or non-string field `type`"))?;
    let kind = match ty {
        "heartbeat" => Kind::Heartbeat,
        "presence" => Kind::Presence {
            in_room: obj
                .get("in_room")
                .and_then(Value::as_bool)
                .ok_or_else(|| malformed("presence requires boolean `in_room`"))?,
        },
        "motion" => Kind::Motion { magnitude: unit_f64(obj, "magnitude")? },
        "sleep_state" => Kind::SleepState {
            state: match obj.get("state").and_then(Value::as_str) {
                Some("asleep") => SleepState::Asleep,
                Some("awake") => SleepState::Awake,
                Some("unknown") => SleepState::Unknown,
                _ => return Err(malformed("sleep_state requires `state` in asleep|awake|unknown")),
            },
        },
        "fall_warn" => Kind::FallWarn { confidence: unit_f64(obj, "confidence")? },
        "net_status" => Kind::NetStatus {
            rssi: obj
                .get("rssi")
                .and_then(Value::as_i64)
                .filter(|r| (-150..=0).contains(r))
                .ok_or_else(|| malformed("net_status requires integer `rssi` in -150..=0"))?
                as i32,
        },
        other => return Err(malformed(format!("unknown event type {other:?}"))),
    };

    if ts_ms > now_ms + MAX_FUTURE_MS {
        return Err(Reject::TsInFuture);
    }
    if ts_ms < now_ms - MAX_PAST_MS {
        return Err(Reject::TsTooOld);
    }

    Ok(Event { device_id, room_id, ts_ms, seq, kind })
}

fn non_empty_str(obj: &Map<String, Value>, key: &str) -> Result<String, Reject> {
    match obj.get(key).and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(s.to_owned()),
        _ => Err(malformed(format!("missing or empty string field `{key}`"))),
    }
}

fn unit_f64(obj: &Map<String, Value>, key: &str) -> Result<f64, Reject> {
    obj.get(key)
        .and_then(Value::as_f64)
        .filter(|x| (0.0..=1.0).contains(x))
        .ok_or_else(|| malformed(format!("`{key}` must be a number in 0..=1")))
}

/// Parse an RFC 3339 timestamp into epoch milliseconds.
pub fn parse_ts(s: &str) -> Option<Millis> {
    chrono::DateTime::parse_from_rfc3339(s).ok().map(|dt| dt.timestamp_millis())
}

/// Format epoch milliseconds the way devices send them: `2026-05-23T18:53:49.123Z`.
pub fn format_ts(ms: Millis) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: Millis = 1_779_562_429_123; // 2026-05-23T18:53:49.123Z

    fn base(ty: &str) -> Value {
        json!({ "device_id": "dev_0001", "room_id": "room_14", "type": ty,
                "ts": "2026-05-23T18:53:49.123Z", "seq": 7 })
    }

    fn with(mut v: Value, k: &str, val: Value) -> Value {
        v[k] = val;
        v
    }

    #[test]
    fn ts_roundtrip() {
        assert_eq!(parse_ts("2026-05-23T18:53:49.123Z"), Some(NOW));
        assert_eq!(format_ts(NOW), "2026-05-23T18:53:49.123Z");
        assert_eq!(parse_ts("2026-05-23T20:53:49.123+02:00"), Some(NOW));
        assert_eq!(parse_ts("yesterday"), None);
    }

    #[test]
    fn accepts_every_type() {
        let cases = [
            (base("heartbeat"), Kind::Heartbeat),
            (with(base("presence"), "in_room", json!(true)), Kind::Presence { in_room: true }),
            (with(base("motion"), "magnitude", json!(0.81)), Kind::Motion { magnitude: 0.81 }),
            (with(base("sleep_state"), "state", json!("asleep")), Kind::SleepState { state: SleepState::Asleep }),
            (with(base("fall_warn"), "confidence", json!(0.92)), Kind::FallWarn { confidence: 0.92 }),
            (with(base("net_status"), "rssi", json!(-68)), Kind::NetStatus { rssi: -68 }),
        ];
        for (v, kind) in cases {
            let e = validate(&v, NOW).unwrap();
            assert_eq!(e.kind, kind);
            assert_eq!(e.device_id, "dev_0001");
            assert_eq!(e.room_id, "room_14");
            assert_eq!(e.ts_ms, NOW);
            assert_eq!(e.seq, 7);
        }
    }

    #[test]
    fn seq_is_optional_and_extra_fields_ignored() {
        let mut v = base("heartbeat");
        v.as_object_mut().unwrap().remove("seq");
        v["firmware"] = json!("1.2.3");
        assert_eq!(validate(&v, NOW).unwrap().seq, 0);
    }

    #[test]
    fn rejects_malformed() {
        let bad = [
            json!("not an object"),
            json!([]),
            with(base("heartbeat"), "device_id", json!("")),
            with(base("heartbeat"), "room_id", json!(14)),
            with(base("heartbeat"), "ts", json!(1779562429)),
            with(base("heartbeat"), "ts", json!("2026-05-23 18:53")),
            with(base("heartbeat"), "seq", json!(-1)),
            base("teleport"),
            base("presence"),
            with(base("presence"), "in_room", json!("yes")),
            with(base("motion"), "magnitude", json!(1.5)),
            with(base("sleep_state"), "state", json!("dreaming")),
            base("fall_warn"),
            with(base("fall_warn"), "confidence", json!(-0.1)),
            with(base("net_status"), "rssi", json!(-68.5)),
            with(base("net_status"), "rssi", json!(10)),
        ];
        for v in bad {
            let r = validate(&v, NOW);
            assert!(matches!(r, Err(Reject::Malformed(_))), "expected malformed for {v}, got {r:?}");
        }
        let mut missing = base("heartbeat");
        missing.as_object_mut().unwrap().remove("type");
        assert!(matches!(validate(&missing, NOW), Err(Reject::Malformed(_))));
    }

    #[test]
    fn acceptance_window_is_inclusive_one_hour() {
        let hb = base("heartbeat");
        // Event ts == NOW. Shift the server clock instead of the event.
        assert!(validate(&hb, NOW - MAX_FUTURE_MS).is_ok(), "exactly +1h accepted");
        assert_eq!(validate(&hb, NOW - MAX_FUTURE_MS - 1), Err(Reject::TsInFuture));
        assert!(validate(&hb, NOW + MAX_PAST_MS).is_ok(), "exactly -1h accepted");
        assert_eq!(validate(&hb, NOW + MAX_PAST_MS + 1), Err(Reject::TsTooOld));
    }
}
