//! HTTP contract tests: routes, status codes and response shapes the eval reads.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use stream_service::api::{router, AppState};
use stream_service::clock::MockClock;
use stream_service::config::Config;
use tower::ServiceExt;

const NOW: i64 = 1_779_562_429_123; // 2026-05-23T18:53:49.123Z

fn app() -> axum::Router {
    router(AppState::new(Config::default(), Arc::new(MockClock::new(NOW))))
}

async fn call(req: Request<Body>) -> (StatusCode, Value) {
    call_on(&app(), req).await
}

async fn call_on(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
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
    // AppState starts not-ready until recovery completes.
    assert_eq!(get("/readyz").await.0, StatusCode::SERVICE_UNAVAILABLE);
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
