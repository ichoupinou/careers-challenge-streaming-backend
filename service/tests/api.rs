//! HTTP contract tests: routes, status codes and response shapes the eval reads.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use stream_service::api::router;
use stream_service::app;
use stream_service::clock::MockClock;
use stream_service::config::Config;
use tower::ServiceExt;

const NOW: i64 = 1_779_562_429_123; // 2026-05-23T18:53:49.123Z

/// A running service on a temp data dir. Dropping it stops the pipeline.
struct TestApp {
    router: axum::Router,
    _svc: app::Service,
    _dir: tempfile::TempDir,
}

fn app() -> TestApp {
    let dir = tempfile::tempdir().unwrap();
    let config = Config { data_dir: dir.path().to_path_buf(), ..Config::default() };
    let svc = app::start(config, Arc::new(MockClock::new(NOW))).unwrap();
    TestApp { router: router(svc.app.clone()), _svc: svc, _dir: dir }
}

async fn call(req: Request<Body>) -> (StatusCode, Value) {
    call_on(&app(), req).await
}

async fn call_on(app: &TestApp, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

async fn get(uri: &str) -> (StatusCode, Value) {
    call(Request::get(uri).body(Body::empty()).unwrap()).await
}

async fn post(body: Value) -> (StatusCode, Value) {
    call(
        Request::post("/events")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
}

fn hb(ts: &str) -> Value {
    json!({ "device_id": "dev_0001", "room_id": "room_000", "type": "heartbeat", "ts": ts, "seq": 1 })
}

#[tokio::test]
async fn ingest_single_event_status_codes() {
    assert_eq!(post(hb("2026-05-23T18:53:49.123Z")).await.0, StatusCode::ACCEPTED);

    let (s, b) = post(json!({ "type": "heartbeat" })).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(b["error"], "malformed");

    let (s, b) = post(hb("2026-05-23T20:53:49.123Z")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(b["error"], "ts_future");

    let (s, b) = post(hb("2026-05-23T16:53:49.123Z")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(b["error"], "ts_too_old");
}

#[tokio::test]
async fn ingest_invalid_json_is_400() {
    let req = Request::post("/events").body(Body::from("{not json")).unwrap();
    assert_eq!(call(req).await.0, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn ingest_batch_reports_rejections_by_index() {
    let (s, b) = post(json!([hb("2026-05-23T18:53:49.123Z"), { "nope": 1 }, hb("2030-01-01T00:00:00.000Z")])).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(b["accepted"], 1);
    assert_eq!(b["rejected"][0]["index"], 1);
    assert_eq!(b["rejected"][0]["reason"], "malformed");
    assert_eq!(b["rejected"][1]["index"], 2);
    assert_eq!(b["rejected"][1]["reason"], "ts_future");
}

#[tokio::test]
async fn read_endpoints_have_eval_shapes() {
    let (s, b) = get("/devices/dev_0000/health").await;
    assert_eq!(s, StatusCode::OK);
    assert!(b.get("last_heartbeat_ts").is_some());
    assert!(b["availability_5m"].is_number());

    for (w, secs) in [("1m", 60), ("5m", 300), ("1h", 3600)] {
        let (s, b) = get(&format!("/rooms/room_000/occupancy?window={w}")).await;
        assert_eq!(s, StatusCode::OK);
        assert!(b["in_room"].is_boolean());
        assert!(b["occupied_pct"].is_number());
        assert_eq!(b["window_seconds"], secs);
    }
    assert_eq!(get("/rooms/room_000/occupancy?window=2h").await.0, StatusCode::BAD_REQUEST);

    let (s, b) = get("/alarms?since=0").await;
    assert_eq!(s, StatusCode::OK);
    assert!(b["alarms"].is_array());
    assert_eq!(get("/alarms?since=whenever").await.0, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn readiness_gate() {
    assert_eq!(get("/healthz").await.0, StatusCode::OK);
    // `app::start` only returns once recovery is done.
    assert_eq!(get("/readyz").await.0, StatusCode::OK);
}

fn post_req(body: Value) -> Request<Body> {
    Request::post("/events")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get_req(uri: &str) -> Request<Body> {
    Request::get(uri).body(Body::empty()).unwrap()
}

#[tokio::test]
async fn ingest_then_read_back() {
    let app = app();
    let ev = |ty: &str, ts: &str, seq: u64, extra: Value| {
        let mut e = json!({ "device_id": "dev_0000", "room_id": "room_000", "type": ty, "ts": ts, "seq": seq });
        e.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        e
    };
    // A fall sent three times (sensor jitter), a heartbeat, and presence
    // arriving out of order.
    let fall = ev("fall_warn", "2026-05-23T18:53:40.000Z", 1, json!({ "confidence": 0.92 }));
    for _ in 0..3 {
        assert_eq!(call_on(&app, post_req(fall.clone())).await.0, StatusCode::ACCEPTED);
    }
    let batch = json!([
        ev("heartbeat", "2026-05-23T18:53:49.000Z", 5, json!({})),
        ev("presence", "2026-05-23T18:53:19.123Z", 4, json!({ "in_room": false })),
        ev("presence", "2026-05-23T18:52:49.123Z", 3, json!({ "in_room": true })),
    ]);
    assert_eq!(call_on(&app, post_req(batch)).await.1["accepted"], 3);

    let (_, b) = call_on(&app, get_req("/alarms?since=0")).await;
    let alarms = b["alarms"].as_array().unwrap();
    assert_eq!(alarms.len(), 1, "jitter copies deduplicated: {b}");
    assert_eq!(alarms[0]["ts"], "2026-05-23T18:53:40.000Z");
    assert_eq!(alarms[0]["room_id"], "room_000");
    assert_eq!(b["next_cursor"], 1);
    let (_, b) = call_on(&app, get_req("/alarms?since=1")).await;
    assert!(b["alarms"].as_array().unwrap().is_empty());

    let (_, b) = call_on(&app, get_req("/devices/dev_0000/health")).await;
    assert_eq!(b["last_heartbeat_ts"], "2026-05-23T18:53:49.000Z");
    assert!((b["availability_5m"].as_f64().unwrap() - 1.0 / 300.0).abs() < 1e-9);

    // Occupied from -60s to -30s of the last minute; latest presence is false.
    let (_, b) = call_on(&app, get_req("/rooms/room_000/occupancy?window=1m")).await;
    assert_eq!(b["in_room"], false);
    assert!((b["occupied_pct"].as_f64().unwrap() - 0.5).abs() < 1e-3, "{b}");
}

#[tokio::test]
async fn state_survives_restart_via_wal() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config { data_dir: dir.path().to_path_buf(), ..Config::default() };
    let fall = json!({ "device_id": "dev_0007", "room_id": "room_003", "type": "fall_warn",
                       "ts": "2026-05-23T18:53:40.000Z", "seq": 1, "confidence": 0.9 });
    let start = || {
        let svc = app::start(config.clone(), Arc::new(MockClock::new(NOW))).unwrap();
        TestApp { router: router(svc.app.clone()), _svc: svc, _dir: tempfile::tempdir().unwrap() }
    };
    {
        let a = start();
        assert_eq!(call_on(&a, post_req(fall.clone())).await.0, StatusCode::ACCEPTED);
        assert_eq!(call_on(&a, post_req(hb("2026-05-23T18:53:49.000Z"))).await.0, StatusCode::ACCEPTED);
        // Dropped without a graceful shutdown: like a kill, the WAL alone must suffice.
        std::mem::forget(a);
    }
    let b = start();
    let (_, body) = call_on(&b, get_req("/alarms?since=0")).await;
    assert_eq!(body["alarms"].as_array().unwrap().len(), 1, "{body}");
    assert_eq!(body["alarms"][0]["alarm_id"], 1, "same id after replay");
    // A jitter copy arriving after the restart is still deduplicated.
    assert_eq!(call_on(&b, post_req(fall)).await.0, StatusCode::ACCEPTED);
    let (_, body) = call_on(&b, get_req("/alarms?since=0")).await;
    assert_eq!(body["alarms"].as_array().unwrap().len(), 1);
    let (_, body) = call_on(&b, get_req("/devices/dev_0001/health")).await;
    assert_eq!(body["last_heartbeat_ts"], "2026-05-23T18:53:49.000Z");
}

// ------------------------------------------------------------ alarm feed

fn fall(device: &str, room: &str, ts: &str) -> Value {
    json!({ "device_id": device, "room_id": room, "type": "fall_warn", "ts": ts, "seq": 1, "confidence": 0.9 })
}

fn start_on(dir: &std::path::Path) -> TestApp {
    let config = Config { data_dir: dir.to_path_buf(), ..Config::default() };
    let svc = app::start(config, Arc::new(MockClock::new(NOW))).unwrap();
    TestApp { router: router(svc.app.clone()), _svc: svc, _dir: tempfile::tempdir().unwrap() }
}

/// Read SSE `id:` values from a streaming body until `n` are collected.
async fn sse_ids(body: &mut Body, n: usize) -> Vec<u64> {
    let mut ids = Vec::new();
    let mut buf = String::new();
    while ids.len() < n {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
            .await
            .expect("SSE event within 2s")
            .expect("stream still open")
            .unwrap();
        if let Ok(data) = frame.into_data() {
            buf.push_str(std::str::from_utf8(&data).unwrap());
        }
        while let Some(end) = buf.find("\n\n") {
            let msg: String = buf.drain(..end + 2).collect();
            if let Some(id) = msg.lines().find_map(|l| l.strip_prefix("id:")) {
                ids.push(id.trim().parse().unwrap());
            }
        }
    }
    ids
}

async fn open_stream(app: &TestApp, uri: &str, last_event_id: Option<u64>) -> Body {
    let mut req = Request::get(uri);
    if let Some(id) = last_event_id {
        req = req.header("last-event-id", id.to_string());
    }
    let resp = app.router.clone().oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["content-type"], "text/event-stream");
    resp.into_body()
}

#[tokio::test]
async fn long_poll_wakes_on_new_alarm_and_times_out_otherwise() {
    let app = Arc::new(app());
    let started = std::time::Instant::now();
    let (_, b) = call_on(&app, get_req("/alarms?since=0&wait=0.2")).await;
    assert!(b["alarms"].as_array().unwrap().is_empty());
    assert!(started.elapsed() >= std::time::Duration::from_millis(190));

    let poller = {
        let app = app.clone();
        tokio::spawn(async move { call_on(&app, get_req("/alarms?since=0&wait=10")).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let posted = std::time::Instant::now();
    call_on(&app, post_req(fall("dev_0001", "room_000", "2026-05-23T18:53:40.000Z"))).await;
    let (_, b) = poller.await.unwrap();
    assert_eq!(b["alarms"].as_array().unwrap().len(), 1, "{b}");
    assert_eq!(b["next_cursor"], 1);
    assert!(posted.elapsed() < std::time::Duration::from_secs(1), "woke promptly");
}

#[tokio::test]
async fn alarms_filter_by_room_and_cursor_skips_other_rooms() {
    let app = app();
    call_on(&app, post_req(fall("dev_0001", "room_000", "2026-05-23T18:53:40.000Z"))).await;
    call_on(&app, post_req(fall("dev_0009", "room_004", "2026-05-23T18:53:41.000Z"))).await;
    call_on(&app, post_req(fall("dev_0001", "room_000", "2026-05-23T18:53:42.000Z"))).await;
    let (_, b) = call_on(&app, get_req("/alarms?since=0&room_id=room_004")).await;
    assert_eq!(b["alarms"].as_array().unwrap().len(), 1);
    assert_eq!(b["next_cursor"], 3, "scanned to the newest alarm");
    let (_, b) = call_on(&app, get_req("/alarms?since=0&limit=2")).await;
    assert_eq!(b["alarms"].as_array().unwrap().len(), 2);
    assert_eq!(b["next_cursor"], 2, "limit hit: resume after the last returned");
}

#[tokio::test]
async fn sse_backfills_then_streams_live_in_order() {
    let app = app();
    for ts in ["2026-05-23T18:53:40.000Z", "2026-05-23T18:53:41.000Z", "2026-05-23T18:53:42.000Z"] {
        call_on(&app, post_req(fall("dev_0001", "room_000", ts))).await;
    }
    let mut body = open_stream(&app, "/alarms/stream", Some(1)).await;
    assert_eq!(sse_ids(&mut body, 2).await, [2, 3], "backfill after Last-Event-ID");

    call_on(&app, post_req(fall("dev_0002", "room_001", "2026-05-23T18:53:43.000Z"))).await;
    // Jitter copy: must not produce another event.
    call_on(&app, post_req(fall("dev_0002", "room_001", "2026-05-23T18:53:43.000Z"))).await;
    call_on(&app, post_req(fall("dev_0003", "room_001", "2026-05-23T18:53:44.000Z"))).await;
    assert_eq!(sse_ids(&mut body, 2).await, [4, 5]);

    let mut room = open_stream(&app, "/alarms/stream?since=0&room_id=room_001", None).await;
    assert_eq!(sse_ids(&mut room, 2).await, [4, 5]);
}

#[tokio::test]
async fn sse_resumes_across_restart_without_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let delivered = {
        let a = start_on(dir.path());
        let mut body = open_stream(&a, "/alarms/stream", None).await;
        call_on(&a, post_req(fall("dev_0001", "room_000", "2026-05-23T18:53:40.000Z"))).await;
        let seen = sse_ids(&mut body, 1).await;
        // Alarms generated after the subscriber's last event, then a crash.
        call_on(&a, post_req(fall("dev_0002", "room_001", "2026-05-23T18:53:41.000Z"))).await;
        call_on(&a, post_req(fall("dev_0003", "room_001", "2026-05-23T18:53:42.000Z"))).await;
        std::mem::forget(a);
        seen
    };
    assert_eq!(delivered, [1]);
    let b = start_on(dir.path());
    let mut body = open_stream(&b, "/alarms/stream", Some(1)).await;
    assert_eq!(sse_ids(&mut body, 2).await, [2, 3], "nothing missed across the restart");
    call_on(&b, post_req(fall("dev_0004", "room_002", "2026-05-23T18:53:43.000Z"))).await;
    assert_eq!(sse_ids(&mut body, 1).await, [4]);
}

#[tokio::test]
async fn shutdown_ends_open_streams() {
    let app = app();
    let mut body = open_stream(&app, "/alarms/stream", None).await;
    app._svc.app.begin_shutdown();
    let end = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame()).await.expect("stream ends");
    assert!(end.is_none());
}

#[tokio::test]
async fn metrics_reflect_traffic() {
    let app = app();
    let f = fall("dev_0001", "room_000", "2026-05-23T18:53:40.000Z");
    call_on(&app, post_req(f.clone())).await;
    call_on(&app, post_req(f)).await; // jitter copy
    call_on(&app, post_req(hb("2026-05-23T18:53:49.000Z"))).await;
    call_on(&app, post_req(hb("2030-01-01T00:00:00.000Z"))).await; // rejected
    call_on(&app, Request::post("/events").body(Body::from("{oops")).unwrap()).await;
    let _sub = open_stream(&app, "/alarms/stream", None).await;

    let resp = app.router.clone().oneshot(get_req("/metrics")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers()["content-type"].to_str().unwrap().starts_with("text/plain"));
    let text = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    for line in [
        "events_accepted_total{type=\"fall_warn\"} 2",
        "events_accepted_total{type=\"heartbeat\"} 1",
        "events_rejected_total{reason=\"ts_future\"} 1",
        "events_rejected_total{reason=\"invalid_json\"} 1",
        "alarms_total 1",
        "falls_deduped_total 1",
        "alarm_emit_latency_seconds_count 1",
        "ingest_ack_seconds_count 3",
        "alarm_feed_subscribers 1",
        "wal_last_offset 3",
        "devices 1",
    ] {
        assert!(text.contains(line), "missing {line:?} in:\n{text}");
    }
}
