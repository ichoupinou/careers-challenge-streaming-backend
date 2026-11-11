//! Service configuration, read from environment variables.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every group-commit batch.
    Batch,
    /// fsync at most every `fsync_interval`.
    Interval,
    /// Never fsync explicitly (rely on the OS).
    Off,
}

impl FromStr for FsyncPolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "batch" => Ok(Self::Batch),
            "interval" => Ok(Self::Interval),
            "off" => Ok(Self::Off),
            other => Err(format!("unknown FSYNC policy {other:?} (batch|interval|off)")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub data_dir: PathBuf,
    pub fsync: FsyncPolicy,
    pub fsync_interval: Duration,
    pub bulk_lane_capacity: usize,
    pub fall_lane_capacity: usize,
    pub enqueue_timeout: Duration,
    pub snapshot_interval: Duration,
    /// Two fall warnings from the same device whose `ts` differ by at most
    /// this much are one logical fall. 0 = exact `(device_id, ts)` match.
    pub dedup_window_ms: i64,
    pub alarm_retention: Duration,
    pub log_json: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 8080,
            data_dir: PathBuf::from("./data"),
            fsync: FsyncPolicy::Interval,
            fsync_interval: Duration::from_millis(50),
            bulk_lane_capacity: 500_000,
            fall_lane_capacity: 50_000,
            enqueue_timeout: Duration::from_millis(3_000),
            snapshot_interval: Duration::from_secs(30),
            dedup_window_ms: 0,
            alarm_retention: Duration::from_secs(24 * 3600),
            log_json: false,
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Build from an arbitrary key lookup (testable without touching env).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let d = Self::default();
        Ok(Self {
            port: parse(&get, "PORT", d.port)?,
            data_dir: get("DATA_DIR").map(PathBuf::from).unwrap_or(d.data_dir),
            fsync: parse(&get, "FSYNC", d.fsync)?,
            fsync_interval: Duration::from_millis(parse(&get, "FSYNC_MS", 50u64)?),
            bulk_lane_capacity: parse(&get, "BULK_LANE_CAPACITY", d.bulk_lane_capacity)?,
            fall_lane_capacity: parse(&get, "FALL_LANE_CAPACITY", d.fall_lane_capacity)?,
            enqueue_timeout: Duration::from_millis(parse(&get, "ENQUEUE_TIMEOUT_MS", 3_000u64)?),
            snapshot_interval: Duration::from_secs(parse(&get, "SNAPSHOT_SECS", 30u64)?),
            dedup_window_ms: parse(&get, "DEDUP_WINDOW_MS", d.dedup_window_ms)?,
            alarm_retention: Duration::from_secs(3600 * parse(&get, "ALARM_RETENTION_H", 24u64)?),
            log_json: get("LOG_FORMAT").is_some_and(|v| v.eq_ignore_ascii_case("json")),
        })
    }
}

fn parse<T: FromStr>(get: &impl Fn(&str) -> Option<String>, key: &str, default: T) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    match get(key) {
        None => Ok(default),
        Some(raw) => raw.trim().parse().map_err(|e| format!("invalid {key}={raw:?}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn defaults_when_unset() {
        let c = Config::from_lookup(lookup(&[])).unwrap();
        assert_eq!(c.port, 8080);
        assert_eq!(c.fsync, FsyncPolicy::Interval);
        assert_eq!(c.dedup_window_ms, 0);
    }

    #[test]
    fn overrides_and_errors() {
        let c = Config::from_lookup(lookup(&[("PORT", "9090"), ("FSYNC", "batch"), ("LOG_FORMAT", "JSON")])).unwrap();
        assert_eq!(c.port, 9090);
        assert_eq!(c.fsync, FsyncPolicy::Batch);
        assert!(c.log_json);
        assert!(Config::from_lookup(lookup(&[("PORT", "nope")])).is_err());
        assert!(Config::from_lookup(lookup(&[("FSYNC", "sometimes")])).is_err());
    }
}
