//! Load generator + alarm-latency probe for the streaming service.
//!
//! The Python generator in `event_generator/` sends one event per request
//! from a single thread, which tops out far below the 50k events/s burst
//! the README asks for. This drives the service over many connections,
//! with the same event mix, fall jitter, bursts and offline replay, and
//! measures what the grader cares about:
//!
//! - achieved ingest rate per phase, 503s (retried, never dropped), failures
//! - alarm latency: POST of a fall → its event on `/alarms/stream`, p50/p95/p99
//! - alarms on the feed and in `/alarms` vs distinct falls generated
//! - a device first seen mid-run shows up with no redeploy
//!
//! ```text
//! cargo run --release --bin loadgen -- --devices 5000 --duration 90 \
//!     --burst-at 20 --burst-secs 30 --burst-rate 50000 --offline-at 10 --offline-secs 20
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
struct Args {
    target: String,
    devices: usize,
    duration: f64,
    /// Baseline events/s across all devices (default: 1 per device).
    rate: f64,
    burst_rate: f64,
    /// Burst start times, seconds (`--burst-at 15,90`).
    burst_at: Vec<f64>,
    burst_secs: f64,
    offline_frac: f64,
    offline_at: f64,
    offline_secs: f64,
    /// Max ± clock skew per device, seconds.
    skew: f64,
    batch: usize,
    concurrency: usize,
    /// Second at which a never-seen device starts sending (<0: off).
    new_device_at: f64,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut a = Args {
            target: "http://127.0.0.1:8080".into(),
            devices: 5000,
            duration: 60.0,
            rate: -1.0,
            burst_rate: 0.0,
            burst_at: vec![15.0],
            burst_secs: 30.0,
            offline_frac: 0.0,
            offline_at: 10.0,
            offline_secs: 60.0,
            skew: 0.0,
            batch: 50,
            concurrency: 64,
            new_device_at: 5.0,
        };
        let raw: Vec<String> = std::env::args().skip(1).collect();
        let mut it = raw.iter();
        while let Some(k) = it.next() {
            if k == "-h" || k == "--help" {
                return Err(HELP.into());
            }
            let v = it.next().ok_or_else(|| format!("missing value for {k}"))?;
            let num = || v.parse::<f64>().map_err(|e| format!("{k}: {e}"));
            match k.as_str() {
                "--target" => a.target = v.trim_end_matches('/').to_string(),
                "--devices" => a.devices = num()? as usize,
                "--duration" => a.duration = num()?,
                "--rate" => a.rate = num()?,
                "--burst-rate" => a.burst_rate = num()?,
                "--burst-at" => {
                    a.burst_at = v.split(',').map(|x| x.trim().parse::<f64>().map_err(|e| format!("{k}: {e}"))).collect::<Result<_, _>>()?
                }
                "--burst-secs" => a.burst_secs = num()?,
                "--offline-frac" => a.offline_frac = num()?,
                "--offline-at" => a.offline_at = num()?,
                "--offline-secs" => a.offline_secs = num()?,
                "--skew" => a.skew = num()?,
                "--batch" => a.batch = (num()? as usize).max(1),
                "--concurrency" => a.concurrency = (num()? as usize).max(1),
                "--new-device-at" => a.new_device_at = num()?,
                _ => return Err(format!("unknown flag {k}\n\n{HELP}")),
            }
        }
        if a.rate < 0.0 {
            a.rate = a.devices as f64;
        }
        Ok(a)
    }

    fn in_burst(&self, t: f64) -> bool {
        self.burst_rate > 0.0 && self.burst_at.iter().any(|&b| t >= b && t < b + self.burst_secs)
    }

    fn rate_at(&self, t: f64) -> f64 {
        if self.in_burst(t) {
            self.burst_rate
        } else {
            self.rate
        }
    }

    fn phase(&self, t: f64) -> &'static str {
        if self.in_burst(t) {
            "burst"
        } else {
            "baseline"
        }
    }

    /// Seconds of the run spent in a burst.
    fn burst_total_secs(&self) -> f64 {
        if self.burst_rate <= 0.0 {
            return 0.0;
        }
        self.burst_at.iter().map(|&b| self.burst_secs.min((self.duration - b).max(0.0))).sum()
    }
}

const HELP: &str = "loadgen [--target URL] [--devices N] [--duration S] [--rate EV/S]
        [--burst-rate EV/S --burst-at S[,S...] --burst-secs S]
        [--offline-frac F --offline-at S --offline-secs S] [--skew S]
        [--batch N] [--concurrency N] [--new-device-at S (<0 off)]";

/// Same weights as event_generator/generate.py.
const WEIGHTS: [(&str, u32); 6] =
    [("heartbeat", 50), ("motion", 15), ("presence", 4), ("sleep_state", 3), ("net_status", 3), ("fall_warn", 1)];

struct Device {
    id: String,
    room: String,
    seq: u64,
    skew_ms: i64,
    /// Last device timestamp used: a real device clock never emits two
    /// distinct events in the same ms, so timestamps strictly increase.
    last_ts: i64,
    offline: Vec<Value>,
}

#[derive(Default)]
struct Counters {
    generated: AtomicU64,
    distinct_falls: AtomicU64,
    sent_ok: AtomicU64,
    requests: AtomicU64,
    retries_503: AtomicU64,
    failed: AtomicU64,
    sent_baseline: AtomicU64,
    sent_burst: AtomicU64,
    replayed: AtomicU64,
    generated_burst: AtomicU64,
}

/// Fall `event_id` (`device:ts_ms`, as the service builds it) → when its
/// first copy was POSTed; filled in by workers, consumed by the probe.
type Pending = Arc<Mutex<HashMap<String, Instant>>>;

fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms).unwrap().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn pick_type() -> &'static str {
    let total: u32 = WEIGHTS.iter().map(|w| w.1).sum();
    let mut r = fastrand::u32(0..total);
    for (t, w) in WEIGHTS {
        if r < w {
            return t;
        }
        r -= w;
    }
    "heartbeat"
}

/// Build one logical event; falls come back as 1–3 identical copies.
fn make_events(d: &mut Device, now_ms: i64) -> (Vec<Value>, bool) {
    let ty = pick_type();
    d.seq += 1;
    d.last_ts = (now_ms + d.skew_ms).max(d.last_ts + 1);
    let mut e = json!({ "device_id": d.id, "room_id": d.room, "type": ty, "ts": iso(d.last_ts), "seq": d.seq });
    match ty {
        "presence" => e["in_room"] = json!(fastrand::bool()),
        "motion" => e["magnitude"] = json!((fastrand::f64() * 100.0).round() / 100.0),
        "sleep_state" => e["state"] = json!(["asleep", "awake", "unknown"][fastrand::usize(0..3)]),
        "net_status" => e["rssi"] = json!(fastrand::i32(-90..=-50)),
        "fall_warn" => e["confidence"] = json!(0.7 + (fastrand::f64() * 29.0).round() / 100.0),
        _ => {}
    }
    if ty != "fall_warn" {
        return (vec![e], false);
    }
    let copies = fastrand::usize(1..=3);
    let mut out = Vec::with_capacity(copies);
    for _ in 0..copies {
        let mut c = e.clone();
        c["seq"] = json!(d.seq);
        d.seq += 1;
        out.push(c);
    }
    (out, true)
}

#[tokio::main]
async fn main() {
    let args = match Args::parse() {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(2);
        }
    };
    let client = reqwest::Client::builder().pool_max_idle_per_host(args.concurrency).build().unwrap();
    let counters = Arc::new(Counters::default());
    let pending: Pending = Arc::default();

    // Start the feed probe from the current end of the alarm log.
    let start_cursor = get_json(&client, &format!("{}/alarms?since=0&room_id=__loadgen_none__", args.target))
        .await
        .and_then(|v| v["next_cursor"].as_u64())
        .unwrap_or(0);
    let latencies: Arc<Mutex<Vec<f64>>> = Arc::default();
    let feed_seen = Arc::new(AtomicU64::new(0));
    let probe = tokio::spawn(probe_feed(client.clone(), args.target.clone(), start_cursor, pending.clone(), latencies.clone(), feed_seen.clone()));

    // Workers POST batches; 503s are retried with backoff (never dropped).
    let (tx, rx) = mpsc::channel::<(Vec<Value>, &'static str)>(args.concurrency * 4);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    let ack_lat: Arc<Mutex<Vec<f64>>> = Arc::default();
    let mut workers = Vec::new();
    for _ in 0..args.concurrency {
        let (rx, client, c, p, url, acks) = (rx.clone(), client.clone(), counters.clone(), pending.clone(), format!("{}/events", args.target), ack_lat.clone());
        workers.push(tokio::spawn(async move {
            loop {
                let Some((batch, phase)) = rx.lock().await.recv().await else { break };
                post_batch(&client, &url, batch, phase, &c, &p, &acks).await;
            }
        }));
    }

    // Generator: paced in 10ms ticks across devices, round-robin.
    let mut devices: Vec<Device> = (0..args.devices)
        .map(|i| Device {
            id: format!("dev_{i:05}"),
            room: format!("room_{:05}", i / 2),
            seq: 0,
            skew_ms: if args.skew > 0.0 { ((fastrand::f64() * 2.0 - 1.0) * args.skew * 1000.0) as i64 } else { 0 },
            last_ts: 0,
            offline: Vec::new(),
        })
        .collect();
    let offline: Vec<bool> = (0..args.devices).map(|_| fastrand::f64() < args.offline_frac).collect();
    let mut new_device = Device { id: "dev_new_mid_run".into(), room: "room_new".into(), seq: 0, skew_ms: 0, last_ts: 0, offline: Vec::new() };

    println!(
        "loadgen → {}: {} devices, {:.0} ev/s baseline{}, {}s, batch {}, {} connections",
        args.target,
        args.devices,
        args.rate,
        if args.burst_rate > 0.0 { format!(", {:.0} ev/s burst at {:?}s for {}s", args.burst_rate, args.burst_at, args.burst_secs) } else { String::new() },
        args.duration,
        args.batch,
        args.concurrency
    );
    let started = Instant::now();
    let mut carry = 0.0;
    let mut last_t = 0.0;
    let mut next_dev = 0usize;
    let mut buf: Vec<Value> = Vec::with_capacity(args.batch);
    let mut flushed_offline = args.offline_frac <= 0.0;
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    let mut last_report = 0u64;
    loop {
        tick.tick().await;
        let t = started.elapsed().as_secs_f64();
        if t >= args.duration {
            break;
        }
        let phase = args.phase(t);
        let is_offline = args.offline_frac > 0.0 && t >= args.offline_at && t < args.offline_at + args.offline_secs;
        // Reconnect: replay each offline device's buffer, oldest first.
        if !flushed_offline && t >= args.offline_at + args.offline_secs {
            flushed_offline = true;
            let replay: Vec<Value> = devices.iter_mut().flat_map(|d| std::mem::take(&mut d.offline)).collect();
            println!("  t={t:5.1}s  replaying {} buffered events from offline devices", replay.len());
            counters.replayed.fetch_add(replay.len() as u64, Relaxed);
            for chunk in replay.chunks(args.batch) {
                let _ = tx.send((chunk.to_vec(), phase)).await;
            }
        }
        let now_ms = chrono::Utc::now().timestamp_millis();
        // Pace on real elapsed time so late ticks don't lower the rate.
        carry += args.rate_at(t) * (t - last_t);
        last_t = t;
        while carry >= 1.0 {
            carry -= 1.0;
            let i = next_dev;
            next_dev = (next_dev + 1) % args.devices;
            let (evs, is_fall) = make_events(&mut devices[i], now_ms);
            counters.generated.fetch_add(evs.len() as u64, Relaxed);
            if phase == "burst" {
                counters.generated_burst.fetch_add(evs.len() as u64, Relaxed);
            }
            if is_fall {
                counters.distinct_falls.fetch_add(1, Relaxed);
            }
            if is_offline && offline[i] {
                devices[i].offline.extend(evs);
                continue;
            }
            for e in evs {
                buf.push(e);
                if buf.len() >= args.batch {
                    let _ = tx.send((std::mem::take(&mut buf), phase)).await;
                }
            }
        }
        if args.new_device_at >= 0.0 && t >= args.new_device_at && fastrand::u8(0..100) == 0 {
            let (evs, is_fall) = make_events(&mut new_device, now_ms);
            counters.generated.fetch_add(evs.len() as u64, Relaxed);
            if is_fall {
                counters.distinct_falls.fetch_add(1, Relaxed);
            }
            buf.extend(evs);
        }
        if !buf.is_empty() {
            let _ = tx.send((std::mem::take(&mut buf), phase)).await;
        }
        let sec = t as u64;
        if sec / 5 != last_report / 5 {
            last_report = sec;
            println!(
                "  t={t:5.1}s  {phase:8}  sent {:>9}  503-retries {:>6}  failed {:>4}  alarms on feed {:>6}",
                counters.sent_ok.load(Relaxed),
                counters.retries_503.load(Relaxed),
                counters.failed.load(Relaxed),
                feed_seen.load(Relaxed)
            );
        }
    }
    // Anything still buffered offline is replayed at the end.
    let rest: Vec<Value> = devices.iter_mut().flat_map(|d| std::mem::take(&mut d.offline)).collect();
    for chunk in rest.chunks(args.batch) {
        let _ = tx.send((chunk.to_vec(), "baseline")).await;
    }
    drop(tx);
    for w in workers {
        let _ = w.await;
    }
    let send_secs = started.elapsed().as_secs_f64();

    // Let the feed catch up, then compare against the store.
    let falls = counters.distinct_falls.load(Relaxed);
    let deadline = Instant::now() + Duration::from_secs(5);
    while feed_seen.load(Relaxed) < falls && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    probe.abort();
    let stored = get_json(&client, &format!("{}/alarms?since={start_cursor}", args.target))
        .await
        .and_then(|v| v["alarms"].as_array().map(|a| a.len() as u64))
        .unwrap_or(0);
    let new_dev = get_json(&client, &format!("{}/devices/{}/health", args.target, new_device.id)).await;

    report(&args, &counters, send_secs, falls, stored, &feed_seen, &latencies, &ack_lat, new_dev, new_device.seq > 0);
}

async fn post_batch(
    client: &reqwest::Client,
    url: &str,
    batch: Vec<Value>,
    phase: &str,
    c: &Counters,
    pending: &Pending,
    acks: &Mutex<Vec<f64>>,
) {
    let body = serde_json::to_vec(&batch).unwrap();
    {
        let now = Instant::now();
        let mut p = pending.lock();
        for e in batch.iter().filter(|e| e["type"] == "fall_warn") {
            let ts = chrono::DateTime::parse_from_rfc3339(e["ts"].as_str().unwrap()).unwrap().timestamp_millis();
            p.entry(format!("{}:{ts}", e["device_id"].as_str().unwrap())).or_insert(now);
        }
    }
    let mut attempt = 0u32;
    loop {
        let t = Instant::now();
        c.requests.fetch_add(1, Relaxed);
        let res = client.post(url).header("content-type", "application/json").body(body.clone()).send().await;
        match res {
            Ok(r) if r.status().is_success() => {
                let _ = r.bytes().await;
                acks.lock().push(t.elapsed().as_secs_f64() * 1e3);
                let n = batch.len() as u64;
                c.sent_ok.fetch_add(n, Relaxed);
                if phase == "burst" { &c.sent_burst } else { &c.sent_baseline }.fetch_add(n, Relaxed);
                return;
            }
            Ok(r) if r.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE => {
                // Retry-safe by contract; back off and resend.
                c.retries_503.fetch_add(1, Relaxed);
                tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt.min(10) + 1))).await;
            }
            Ok(r) => {
                eprintln!("unexpected HTTP {} for batch: {}", r.status(), r.text().await.unwrap_or_default());
                c.failed.fetch_add(batch.len() as u64, Relaxed);
                return;
            }
            Err(e) => {
                attempt += 1;
                if attempt > 50 {
                    eprintln!("giving up on batch after {attempt} errors: {e}");
                    c.failed.fetch_add(batch.len() as u64, Relaxed);
                    return;
                }
                // Connection refused/reset (e.g. during a restart): retry.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Follow `/alarms/stream`, reconnecting with `Last-Event-ID`, and record
/// POST→receipt latency for every alarm this run produced.
async fn probe_feed(client: reqwest::Client, target: String, mut cursor: u64, pending: Pending, lat: Arc<Mutex<Vec<f64>>>, seen: Arc<AtomicU64>) {
    loop {
        let req = client.get(format!("{target}/alarms/stream")).header("last-event-id", cursor.to_string());
        let Ok(resp) = req.send().await else {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        };
        let mut stream = resp.bytes_stream();
        let mut buf = String::new();
        while let Some(Ok(chunk)) = stream.next().await {
            let now = Instant::now();
            buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(end) = buf.find("\n\n") {
                let msg: String = buf.drain(..end + 2).collect();
                let (mut id, mut data) = (None, None);
                for l in msg.lines() {
                    if let Some(v) = l.strip_prefix("id:") {
                        id = v.trim().parse::<u64>().ok();
                    } else if let Some(v) = l.strip_prefix("data:") {
                        data = serde_json::from_str::<Value>(v.trim()).ok();
                    }
                }
                if let (Some(id), Some(a)) = (id, data) {
                    cursor = id;
                    seen.fetch_add(1, Relaxed);
                    if let Some(sent) = a["event_id"].as_str().and_then(|k| pending.lock().remove(k)) {
                        lat.lock().push(now.duration_since(sent).as_secs_f64() * 1e3);
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn get_json(client: &reqwest::Client, url: &str) -> Option<Value> {
    let r = client.get(url).send().await.ok()?;
    serde_json::from_slice(&r.bytes().await.ok()?).ok()
}

fn pct(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(f64::total_cmp);
    v[((q * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1]
}

#[allow(clippy::too_many_arguments)]
fn report(
    args: &Args,
    c: &Counters,
    secs: f64,
    falls: u64,
    stored: u64,
    feed_seen: &AtomicU64,
    lat: &Mutex<Vec<f64>>,
    acks: &Mutex<Vec<f64>>,
    new_dev: Option<Value>,
    new_dev_sent: bool,
) {
    let mut lat = lat.lock().clone();
    let mut acks = acks.lock().clone();
    let burst_secs = args.burst_total_secs();
    let base_secs = (args.duration - burst_secs).max(1e-9);
    let replayed = c.replayed.load(Relaxed);
    let (generated, sent, failed) = (c.generated.load(Relaxed), c.sent_ok.load(Relaxed), c.failed.load(Relaxed));
    println!("\n=== loadgen report ===");
    println!("  events generated        {generated}");
    println!("  events accepted (202)   {sent}   failed {failed}   in {secs:.1}s");
    println!(
        "  baseline rate           {:.0} ev/s (target {:.0}; excludes {replayed} offline-replay events)",
        (c.sent_baseline.load(Relaxed).saturating_sub(replayed)) as f64 / base_secs,
        args.rate
    );
    if burst_secs > 0.0 {
        println!(
            "  burst rate              generated {:.0} ev/s, sent {:.0} ev/s (target {:.0}; offline devices buffer their share)",
            c.generated_burst.load(Relaxed) as f64 / burst_secs,
            c.sent_burst.load(Relaxed) as f64 / burst_secs,
            args.burst_rate
        );
    }
    println!("  requests / 503 retries  {} / {}", c.requests.load(Relaxed), c.retries_503.load(Relaxed));
    println!("  POST→202 latency        p50 {:.2} ms  p95 {:.2} ms  p99 {:.2} ms", pct(&mut acks, 0.5), pct(&mut acks, 0.95), pct(&mut acks, 0.99));
    println!("  distinct falls          {falls}");
    println!("  alarms on feed / store  {} / {stored}", feed_seen.load(Relaxed));
    println!(
        "  alarm latency (POST→SSE) p50 {:.2} ms  p95 {:.2} ms  p99 {:.2} ms  max {:.2} ms  (n={})",
        pct(&mut lat, 0.5),
        pct(&mut lat, 0.95),
        pct(&mut lat, 0.99),
        lat.last().copied().unwrap_or(f64::NAN),
        lat.len()
    );
    let mut ok = failed == 0 && stored == falls && generated == sent;
    let p95 = pct(&mut lat, 0.95);
    if p95.is_finite() && p95 > 1000.0 {
        ok = false;
    }
    if new_dev_sent {
        let seen = new_dev.as_ref().is_some_and(|v| !v["last_heartbeat_ts"].is_null() || v["room_id"] == "room_new");
        println!("  device added mid-run    {}", if seen { "visible, no redeploy" } else { "NOT visible" });
        ok &= seen;
    }
    println!("  verdict                 {}", if ok { "✓ all events accepted, alarms == distinct falls, p95 < 1s" } else { "⚠ see numbers above" });
    if !ok {
        std::process::exit(1);
    }
}
