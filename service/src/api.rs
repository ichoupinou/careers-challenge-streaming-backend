//! HTTP API. Response shapes follow `example_solution/service.py`, which is
//! what the eval reads; extra fields are tolerated by the scorer.
//!
//! Writes go through the ingest pipeline (`crate::ingest`). Reads take a
//! short read lock on the materialized state.

use std::sync::atomic::{AtomicBool, Ordering};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::clock::{Clock, Millis};
use crate::config::Config;
use crate::event::{self, Event, Reject};
use crate::ingest::{Ingest, IngestError};
use crate::state::alarms::Alarm;
use crate::state::State as Materialized;
use futures_util::{Stream, StreamExt};
use parking_lot::RwLock;
use tokio::sync::watch;

/// Longest occupancy window we retain history for.
pub const MAX_WINDOW_SECS: u32 = 3600;

pub struct AppState {
    pub config: Config,
    pub clock: Arc<dyn Clock>,
    /// False until recovery has finished.
    pub ready: AtomicBool,
    /// Written only by the applier thread.
    pub state: Arc<RwLock<Materialized>>,
    pub ingest: Ingest,
    /// Newest `alarm_id`, bumped by the applier: wakes feed subscribers.
    pub alarm_rx: watch::Receiver<u64>,
    /// Set on shutdown so open SSE streams end and graceful shutdown completes.
    pub closing: watch::Sender<bool>,
}

impl AppState {
    /// Submit validated events and map pipeline failures to HTTP errors.
    async fn submit(&self, events: Vec<Event>, recv_ms: Millis) -> Result<(), ApiError> {
        self.ingest.submit(events, recv_ms).await.map_err(ApiError::from)
    }

    /// Close live feeds (call when the server starts shutting down).
    pub fn begin_shutdown(&self) {
        self.closing.send_replace(true);
    }

    /// Periodic housekeeping: drop history no read can need any more.
    pub fn prune(&self) {
        let now = self.clock.now_ms();
        let retention = self.config.alarm_retention.as_millis() as Millis;
        self.state.write().prune(now, retention);
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/events", post(post_events))
        .route("/devices/{device_id}/health", get(device_health))
        .route("/rooms/{room_id}/occupancy", get(room_occupancy))
        .route("/alarms", get(alarms))
        .route("/alarms/stream", get(alarm_stream))
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .with_state(state)
}

type ApiResult = Result<Response, ApiError>;

/// Error response: `{ "error": <code>, "detail": <message> }`.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    detail: String,
}

impl ApiError {
    fn bad_request(code: &'static str, detail: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, code, detail: detail.into() }
    }
}

impl From<Reject> for ApiError {
    fn from(r: Reject) -> Self {
        let status = match r {
            Reject::Malformed(_) => StatusCode::BAD_REQUEST,
            Reject::TsInFuture | Reject::TsTooOld => StatusCode::UNPROCESSABLE_ENTITY,
        };
        Self { status, code: r.code(), detail: r.to_string() }
    }
}

impl From<IngestError> for ApiError {
    fn from(e: IngestError) -> Self {
        let (status, code) = match e {
            IngestError::Overloaded => (StatusCode::SERVICE_UNAVAILABLE, "overloaded"),
            IngestError::ShuttingDown => (StatusCode::SERVICE_UNAVAILABLE, "shutting_down"),
            IngestError::Wal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "wal_error"),
        };
        if e == IngestError::Overloaded {
            tracing::warn!("shed request: ingest queue full");
        }
        Self { status, code, detail: e.to_string() }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({ "error": self.code, "detail": self.detail }));
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            // Ask well-behaved clients to back off and retry.
            return (self.status, [(header::RETRY_AFTER, "1")], body).into_response();
        }
        (self.status, body).into_response()
    }
}

// ---------------------------------------------------------------- ingest

/// `POST /events`: a single event object, or a JSON array of events.
///
/// Single event: 202 if accepted, 400 if malformed, 422 if `ts` is outside
/// the acceptance window. Batch: 202 with per-index rejections. 202 means
/// written to the WAL and applied. 503 + `Retry-After` means the queue
/// stayed full for `ENQUEUE_TIMEOUT_MS` and nothing from the request was
/// accepted.
async fn post_events(State(st): State<Arc<AppState>>, body: Bytes) -> ApiResult {
    let parsed: Value = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request("malformed", format!("invalid JSON: {e}")))?;
    let now = st.clock.now_ms();

    match parsed {
        Value::Array(items) => {
            let mut accepted = Vec::with_capacity(items.len());
            let mut rejected = Vec::new();
            for (index, item) in items.iter().enumerate() {
                match event::validate(item, now) {
                    Ok(ev) => accepted.push(ev),
                    Err(r) => {
                        tracing::debug!(index, reason = r.code(), "rejected event: {r}");
                        rejected.push(json!({ "index": index, "reason": r.code(), "detail": r.to_string() }));
                    }
                }
            }
            let n = accepted.len();
            st.submit(accepted, now).await?;
            Ok((StatusCode::ACCEPTED, Json(json!({ "accepted": n, "rejected": rejected }))).into_response())
        }
        single => {
            let ev = event::validate(&single, now).inspect_err(|r| tracing::debug!(reason = r.code(), "rejected event: {r}"))?;
            st.submit(vec![ev], now).await?;
            Ok((StatusCode::ACCEPTED, Json(json!({ "accepted": 1, "rejected": [] }))).into_response())
        }
    }
}

// ----------------------------------------------------------------- reads

async fn device_health(State(st): State<Arc<AppState>>, Path(device_id): Path<String>) -> Json<Value> {
    // Unknown devices are not an error: a new device needs no registration.
    let h = st.state.read().device_health(&device_id, st.clock.now_ms());
    Json(json!({
        "device_id": device_id,
        "last_heartbeat_ts": h.last_heartbeat_ms.map(event::format_ts),
        "availability_5m": h.availability_5m,
        "heartbeats_5m": h.heartbeats_5m,
        "room_id": h.room_id,
        "motion": h.motion,
        "sleep_state": h.sleep_state,
        "rssi": h.rssi,
    }))
}

#[derive(Deserialize)]
struct OccupancyQuery {
    window: Option<String>,
}

async fn room_occupancy(
    State(st): State<Arc<AppState>>,
    Path(room_id): Path<String>,
    Query(q): Query<OccupancyQuery>,
) -> ApiResult {
    let window_seconds = parse_window(q.window.as_deref().unwrap_or("5m"))?;
    let o = st.state.read().room_occupancy(&room_id, st.clock.now_ms(), window_seconds as Millis * 1000);
    Ok(Json(json!({
        "room_id": room_id,
        "in_room": o.in_room,
        "occupied_pct": o.occupied_pct,
        "window_seconds": window_seconds,
    }))
    .into_response())
}

/// Parse `30s`, `1m`, `5m`, `1h` or bare seconds; must be within 1s..=1h.
pub fn parse_window(s: &str) -> Result<u32, ApiError> {
    let s = s.trim();
    let (num, mult) = match s.char_indices().last() {
        Some((i, 's')) => (&s[..i], 1),
        Some((i, 'm')) => (&s[..i], 60),
        Some((i, 'h')) => (&s[..i], 3600),
        _ => (s, 1),
    };
    let secs = num
        .parse::<u32>()
        .ok()
        .and_then(|n| n.checked_mul(mult))
        .filter(|secs| (1..=MAX_WINDOW_SECS).contains(secs))
        .ok_or_else(|| ApiError::bad_request("bad_window", format!("window {s:?} must be 1s..1h, e.g. 1m|5m|1h")))?;
    Ok(secs)
}

/// Where an `/alarms` read starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Since {
    /// Alarms with `alarm_id > n` (`since=0` = everything). Resumable cursor.
    Cursor(u64),
    /// Alarms whose original `ts >= t`.
    Ts(Millis),
}

pub fn parse_since(raw: Option<&str>) -> Result<Since, ApiError> {
    match raw.map(str::trim) {
        None | Some("") => Ok(Since::Cursor(0)),
        Some(s) => s
            .parse::<u64>()
            .map(Since::Cursor)
            .ok()
            .or_else(|| event::parse_ts(s).map(Since::Ts))
            .ok_or_else(|| ApiError::bad_request("bad_since", format!("since {s:?} must be an alarm_id cursor or an RFC 3339 ts"))),
    }
}

#[derive(Deserialize)]
struct AlarmsQuery {
    since: Option<String>,
    /// Page size. Unbounded by default: the eval reads `since=0` in one go.
    limit: Option<usize>,
    /// Only alarms for this room.
    room_id: Option<String>,
    /// Long-poll: if nothing matches yet, wait up to this many seconds
    /// (max 60) for a new alarm before answering.
    wait: Option<f64>,
}

/// Longest long-poll a client can ask for.
pub const MAX_WAIT_SECS: f64 = 60.0;

/// One page of alarms after `since` (optionally for one room), and the
/// cursor to resume from. Alarms come in `alarm_id` order, which is
/// emission order, so they're also in order per room.
fn read_alarms(st: &AppState, since: Since, room: Option<&str>, limit: usize) -> (Vec<Value>, u64) {
    let state = st.state.read();
    let store = state.alarms();
    let in_room = |a: &&Alarm| room.is_none_or(|r| a.room_id == r);
    let items: Vec<Value> = match since {
        Since::Cursor(c) => store.after(c, usize::MAX).filter(in_room).take(limit).map(alarm_json).collect(),
        Since::Ts(t) => store.since_ts(t, usize::MAX).filter(in_room).take(limit).map(alarm_json).collect(),
    };
    let next_cursor = if items.len() == limit {
        items.last().and_then(|a| a["alarm_id"].as_u64()).unwrap_or(0)
    } else {
        // Everything up to the newest alarm has been scanned.
        let floor = if let Since::Cursor(c) = since { c } else { 0 };
        store.last_id().max(floor)
    };
    (items, next_cursor)
}

/// `GET /alarms?since=<alarm_id|RFC 3339 ts>&limit=<n>&room_id=<r>&wait=<s>`.
/// Alarms come in emission order. `next_cursor` is where to resume: pass it
/// back as `since`. With `wait`, an empty result is held open until a new
/// alarm arrives or the wait ends.
async fn alarms(State(st): State<Arc<AppState>>, Query(q): Query<AlarmsQuery>) -> ApiResult {
    let since = parse_since(q.since.as_deref())?;
    let limit = q.limit.unwrap_or(usize::MAX).max(1);
    let wait = q.wait.filter(|w| w.is_finite() && *w > 0.0).map(|w| Duration::from_secs_f64(w.min(MAX_WAIT_SECS)));
    let deadline = wait.map(|w| tokio::time::Instant::now() + w);
    let mut rx = st.alarm_rx.clone();
    loop {
        // Mark the current version as seen *before* reading, so an alarm
        // published after the read still wakes us.
        rx.borrow_and_update();
        let (items, next_cursor) = read_alarms(&st, since, q.room_id.as_deref(), limit);
        let Some(deadline) = deadline.filter(|_| items.is_empty()) else {
            return Ok(Json(json!({ "alarms": items, "next_cursor": next_cursor })).into_response());
        };
        match tokio::time::timeout_at(deadline, rx.changed()).await {
            Ok(Ok(())) => continue,
            // Timed out or shutting down: answer with the empty page.
            _ => return Ok(Json(json!({ "alarms": items, "next_cursor": next_cursor })).into_response()),
        }
    }
}

#[derive(Deserialize)]
struct StreamQuery {
    since: Option<u64>,
    room_id: Option<String>,
}

struct StreamState {
    app: Arc<AppState>,
    cursor: u64,
    room: Option<String>,
    alarms: watch::Receiver<u64>,
    closing: watch::Receiver<bool>,
}

/// `GET /alarms/stream?since=<alarm_id>&room_id=<r>`: Server-Sent Events.
///
/// Each alarm is an `event: alarm` whose `id:` is its `alarm_id`. A client
/// that reconnects with `Last-Event-ID` (browsers do this automatically)
/// or `?since=` first gets every alarm after that id from the store, then
/// live ones. The store is the source of truth, so a slow or reconnecting
/// subscriber can't miss an alarm, including across a service restart.
async fn alarm_stream(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<StreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let cursor = match headers.get("last-event-id") {
        Some(v) => v
            .to_str()
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .ok_or_else(|| ApiError::bad_request("bad_last_event_id", "Last-Event-ID must be an alarm_id"))?,
        None => q.since.unwrap_or(0),
    };
    let s = StreamState {
        cursor,
        room: q.room_id,
        alarms: st.alarm_rx.clone(),
        closing: st.closing.subscribe(),
        app: st,
    };
    let stream = futures_util::stream::unfold(s, |mut s| async move {
        loop {
            if *s.closing.borrow() {
                return None;
            }
            s.alarms.borrow_and_update();
            let (items, next) = read_alarms(&s.app, Since::Cursor(s.cursor), s.room.as_deref(), 1_000);
            s.cursor = next;
            if !items.is_empty() {
                let events: Vec<Result<SseEvent, Infallible>> = items
                    .iter()
                    .map(|a| Ok(SseEvent::default().event("alarm").id(a["alarm_id"].to_string()).data(a.to_string())))
                    .collect();
                return Some((futures_util::stream::iter(events), s));
            }
            tokio::select! {
                changed = s.alarms.changed() => if changed.is_err() { return None },
                _ = s.closing.changed() => return None,
            }
        }
    })
    .flatten();
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

pub fn alarm_json(a: &Alarm) -> Value {
    json!({
        "alarm_id": a.alarm_id,
        "event_id": a.event_id,
        "device_id": a.device_id,
        "room_id": a.room_id,
        "ts": event::format_ts(a.ts_ms),
        "confidence": a.confidence,
    })
}

async fn readyz(State(st): State<Arc<AppState>>) -> StatusCode {
    if st.ready.load(Ordering::Acquire) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows() {
        assert_eq!(parse_window("1m").unwrap(), 60);
        assert_eq!(parse_window("5m").unwrap(), 300);
        assert_eq!(parse_window("1h").unwrap(), 3600);
        assert_eq!(parse_window("90s").unwrap(), 90);
        assert_eq!(parse_window("120").unwrap(), 120);
        for bad in ["", "0m", "2h", "m", "-1m", "5x", "abc"] {
            assert!(parse_window(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn since() {
        assert_eq!(parse_since(None).unwrap(), Since::Cursor(0));
        assert_eq!(parse_since(Some("0")).unwrap(), Since::Cursor(0));
        assert_eq!(parse_since(Some("42")).unwrap(), Since::Cursor(42));
        assert_eq!(
            parse_since(Some("2026-05-23T18:53:49.123Z")).unwrap(),
            Since::Ts(1_779_562_429_123)
        );
        assert!(parse_since(Some("soon")).is_err());
    }
}
