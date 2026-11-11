//! Periodic state snapshots, so recovery replays only the WAL tail.
//!
//! File: `<dir>/snap-<last_offset:020>.bin`, written to a `.tmp` file,
//! fsynced, then renamed (atomic on POSIX), so a crash never leaves a
//! half-written snapshot under the real name. Layout:
//!
//! ```text
//! [magic "TSNAP001"][len: u64 LE][crc32(payload): u32 LE][payload: postcard(State)]
//! ```
//!
//! The two newest snapshots are kept. WAL segments are deleted only up to the
//! *older* one, so if the newest snapshot turns out to be unreadable,
//! recovery can still fall back to the older snapshot plus the WAL.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use crate::state::State;
use crate::wal;

const MAGIC: &[u8; 8] = b"TSNAP001";
const HEADER: usize = 8 + 8 + 4;
pub const KEEP: usize = 2;

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotInfo {
    pub last_offset: u64,
    pub bytes: u64,
    pub millis: u64,
    pub wal_segments_deleted: usize,
}

fn name(last_offset: u64) -> String {
    format!("snap-{last_offset:020}.bin")
}

fn offset_of(path: &Path) -> Option<u64> {
    path.file_name()?.to_str()?.strip_prefix("snap-")?.strip_suffix(".bin")?.parse().ok()
}

/// Snapshot paths, oldest first.
pub fn list(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd.filter_map(|e| e.ok().map(|e| e.path())).filter(|p| offset_of(p).is_some()).collect(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    v.sort_by_key(|p| offset_of(p));
    Ok(v)
}

/// Write `state` durably. Returns the path and its size in bytes.
pub fn save(dir: &Path, state: &State) -> io::Result<(PathBuf, u64)> {
    fs::create_dir_all(dir)?;
    let payload = postcard::to_stdvec(state).map_err(io::Error::other)?;
    let mut buf = Vec::with_capacity(HEADER + payload.len());
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    buf.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    buf.extend_from_slice(&payload);

    let path = dir.join(name(state.last_offset));
    let tmp = path.with_extension("bin.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    File::open(dir)?.sync_all()?;
    Ok((path, buf.len() as u64))
}

/// Read and verify one snapshot.
pub fn load(path: &Path) -> io::Result<State> {
    let mut data = Vec::new();
    File::open(path)?.read_to_end(&mut data)?;
    let bad = |why: &str| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {why}", path.display()));
    if data.len() < HEADER || &data[..8] != MAGIC {
        return Err(bad("bad magic or short header"));
    }
    let len = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(data[16..20].try_into().unwrap());
    let payload = data.get(HEADER..HEADER + len).ok_or_else(|| bad("truncated payload"))?;
    if crc32fast::hash(payload) != crc {
        return Err(bad("checksum mismatch"));
    }
    let state: State = postcard::from_bytes(payload).map_err(|e| bad(&e.to_string()))?;
    if Some(state.last_offset) != offset_of(path) {
        return Err(bad("offset in file name does not match contents"));
    }
    Ok(state)
}

/// Newest snapshot that loads cleanly, skipping (and logging) bad ones.
pub fn load_latest(dir: &Path) -> io::Result<Option<State>> {
    for path in list(dir)?.iter().rev() {
        match load(path) {
            Ok(state) => return Ok(Some(state)),
            Err(e) => tracing::warn!(error = %e, "skipping unreadable snapshot"),
        }
    }
    Ok(None)
}

/// Takes snapshots of the live state and garbage-collects old snapshots and
/// WAL segments. Safe to call from any thread; runs are serialized.
pub struct Snapshotter {
    snap_dir: PathBuf,
    wal_dir: PathBuf,
    state: Arc<RwLock<State>>,
    last_offset: Mutex<u64>,
}

impl Snapshotter {
    pub fn new(snap_dir: PathBuf, wal_dir: PathBuf, state: Arc<RwLock<State>>) -> Self {
        let last = list(&snap_dir).ok().and_then(|v| v.last().and_then(|p| offset_of(p))).unwrap_or(0);
        Self { snap_dir, wal_dir, state, last_offset: Mutex::new(last) }
    }

    /// Snapshot if anything was applied since the last one.
    pub fn run_once(&self) -> io::Result<Option<SnapshotInfo>> {
        let mut last = self.last_offset.lock();
        let started = Instant::now();
        // Clone under the read lock (a few ms for thousands of devices), then
        // serialize and write without blocking the applier.
        let state = {
            let st = self.state.read();
            if st.last_offset == *last {
                return Ok(None);
            }
            st.clone()
        };
        let (_, bytes) = save(&self.snap_dir, &state)?;
        *last = state.last_offset;

        // Keep the newest KEEP snapshots; WAL is needed from the oldest kept.
        let snaps = list(&self.snap_dir)?;
        let excess = snaps.len().saturating_sub(KEEP);
        for old in &snaps[..excess] {
            fs::remove_file(old)?;
        }
        let kept = &snaps[excess..];
        let wal_segments_deleted = if kept.len() == KEEP {
            wal::gc(&self.wal_dir, offset_of(&kept[0]).unwrap_or(0))?
        } else {
            0
        };
        Ok(Some(SnapshotInfo { last_offset: state.last_offset, bytes, millis: started.elapsed().as_millis() as u64, wal_segments_deleted }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Kind};

    fn state_with(n: u64) -> State {
        let mut s = State::new(0);
        for i in 1..=n {
            let kind = match i % 4 {
                0 => Kind::FallWarn { confidence: 0.9 },
                1 => Kind::Presence { in_room: i % 8 == 1 },
                _ => Kind::Heartbeat,
            };
            let ev = Event { device_id: format!("dev_{}", i % 5), room_id: format!("room_{}", i % 3), ts_ms: 1_000_000 + i as i64 * 700, seq: i, kind };
            s.apply(&ev, i, 2_000_000);
        }
        s
    }

    #[test]
    fn roundtrip_is_exact() {
        let tmp = tempfile::tempdir().unwrap();
        let s = state_with(200);
        let (path, bytes) = save(tmp.path(), &s).unwrap();
        assert!(bytes > 0);
        assert_eq!(load(&path).unwrap(), s);
        assert_eq!(load_latest(tmp.path()).unwrap(), Some(s));
    }

    #[test]
    fn corrupt_newest_falls_back_to_older() {
        let tmp = tempfile::tempdir().unwrap();
        let older = state_with(50);
        save(tmp.path(), &older).unwrap();
        let (newest, _) = save(tmp.path(), &state_with(80)).unwrap();
        let mut bytes = fs::read(&newest).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        fs::write(&newest, bytes).unwrap();
        assert!(load(&newest).is_err());
        assert_eq!(load_latest(tmp.path()).unwrap(), Some(older));
        // A leftover .tmp from a crash mid-write is ignored.
        fs::write(tmp.path().join("snap-00000000000000000099.bin.tmp"), b"junk").unwrap();
        assert_eq!(list(tmp.path()).unwrap().len(), 2);
    }

    #[test]
    fn empty_dir_has_no_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(load_latest(&tmp.path().join("missing")).unwrap(), None);
    }
}
