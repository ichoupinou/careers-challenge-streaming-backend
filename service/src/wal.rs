//! Append-only write-ahead log: the source of truth for accepted events.
//!
//! Layout: `<dir>/<first_offset:020>.wal` segments, rotated at
//! `SEGMENT_BYTES`. Each record is
//!
//! ```text
//! [len: u32 LE][crc32(payload): u32 LE][payload: postcard(WalRecord)]
//! ```
//!
//! Offsets are dense and start at 1. A crash can leave a torn record at the
//! tail of the last segment. The reader stops at the first short or
//! bad-CRC record, and `open` truncates it away before appending.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::clock::Millis;
use crate::event::Event;

pub const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const HEADER: usize = 8;
/// Upper bound on a single record, to reject garbage lengths quickly.
const MAX_RECORD: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    pub offset: u64,
    /// Server time the event was accepted.
    pub recv_ms: Millis,
    pub event: Event,
}

/// Appends records. Not thread-safe: owned by the single writer thread.
pub struct Wal {
    dir: PathBuf,
    file: File,
    segment_bytes: u64,
    segment_limit: u64,
    next_offset: u64,
    buf: Vec<u8>,
}

/// What `Wal::open` found on disk.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct OpenReport {
    pub segments: usize,
    pub last_offset: u64,
    pub truncated_bytes: u64,
}

impl Wal {
    /// Open `dir` for appending after the last valid record. Truncates a torn
    /// tail and always starts a fresh segment.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<(Self, OpenReport)> {
        Self::open_with_limit(dir, SEGMENT_BYTES)
    }

    pub fn open_with_limit(dir: impl Into<PathBuf>, segment_limit: u64) -> io::Result<(Self, OpenReport)> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let segments = list_segments(&dir)?;
        let mut report = OpenReport { segments: segments.len(), ..Default::default() };

        if let Some(last) = segments.last() {
            let scan = scan_segment(last, |_| Ok(()))?;
            if scan.torn {
                let len = fs::metadata(last)?.len();
                OpenOptions::new().write(true).open(last)?.set_len(scan.valid_bytes)?;
                report.truncated_bytes = len - scan.valid_bytes;
                tracing::warn!(segment = %last.display(), dropped_bytes = report.truncated_bytes, "truncated torn WAL tail");
            }
            // An empty or fully-torn last segment still carries its start offset.
            report.last_offset = scan.last_offset.unwrap_or(segment_start(last).saturating_sub(1));
        }

        let next_offset = report.last_offset + 1;
        let file = create_segment(&dir, next_offset)?;
        let wal = Self { dir, file, segment_bytes: 0, segment_limit, next_offset, buf: Vec::with_capacity(1 << 20) };
        Ok((wal, report))
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Append events as one `write()`, assigning consecutive offsets.
    /// Returns the records written (same order as `events`).
    pub fn append<I>(&mut self, events: I) -> io::Result<Vec<WalRecord>>
    where
        I: IntoIterator<Item = (Event, Millis)>,
    {
        self.buf.clear();
        let mut out = Vec::new();
        for (event, recv_ms) in events {
            let rec = WalRecord { offset: self.next_offset + out.len() as u64, recv_ms, event };
            encode_into(&rec, &mut self.buf)?;
            out.push(rec);
        }
        if out.is_empty() {
            return Ok(out);
        }
        self.file.write_all(&self.buf)?;
        self.next_offset += out.len() as u64;
        self.segment_bytes += self.buf.len() as u64;
        if self.segment_bytes >= self.segment_limit {
            self.rotate()?;
        }
        Ok(out)
    }

    /// fsync the current segment's data.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        self.file = create_segment(&self.dir, self.next_offset)?;
        self.segment_bytes = 0;
        Ok(())
    }
}

fn encode_into(rec: &WalRecord, buf: &mut Vec<u8>) -> io::Result<()> {
    let start = buf.len();
    buf.extend_from_slice(&[0; HEADER]);
    let payload = postcard::to_extend(rec, std::mem::take(buf)).map_err(io::Error::other)?;
    *buf = payload;
    let len = buf.len() - start - HEADER;
    let crc = crc32fast::hash(&buf[start + HEADER..]);
    buf[start..start + 4].copy_from_slice(&(len as u32).to_le_bytes());
    buf[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

fn segment_name(first_offset: u64) -> String {
    format!("{first_offset:020}.wal")
}

fn segment_start(path: &Path) -> u64 {
    path.file_stem().and_then(|s| s.to_str()).and_then(|s| s.parse().ok()).unwrap_or(0)
}

fn create_segment(dir: &Path, first_offset: u64) -> io::Result<File> {
    let path = dir.join(segment_name(first_offset));
    let file = OpenOptions::new().create(true).append(true).open(&path)?;
    // Make the new directory entry durable too.
    File::open(dir)?.sync_all()?;
    Ok(file)
}

/// Segment paths, oldest first.
pub fn list_segments(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "wal"))
            .collect(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    v.sort_by_key(|p| segment_start(p));
    Ok(v)
}

struct Scan {
    valid_bytes: u64,
    last_offset: Option<u64>,
    torn: bool,
}

/// Read records from one segment until EOF or the first invalid record.
fn scan_segment(path: &Path, mut f: impl FnMut(WalRecord) -> io::Result<()>) -> io::Result<Scan> {
    let mut data = Vec::new();
    File::open(path)?.read_to_end(&mut data)?;
    let mut pos = 0usize;
    let mut last_offset = None;
    while pos < data.len() {
        let Some(header) = data.get(pos..pos + HEADER) else { break };
        let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(header[4..].try_into().unwrap());
        if len == 0 || len > MAX_RECORD {
            break;
        }
        let Some(payload) = data.get(pos + HEADER..pos + HEADER + len) else { break };
        if crc32fast::hash(payload) != crc {
            break;
        }
        let Ok(rec) = postcard::from_bytes::<WalRecord>(payload) else { break };
        last_offset = Some(rec.offset);
        f(rec)?;
        pos += HEADER + len;
    }
    Ok(Scan { valid_bytes: pos as u64, last_offset, torn: pos < data.len() })
}

/// Replay every valid record with `offset > after`, oldest first. Returns
/// the number of records passed to `f`.
pub fn replay(dir: &Path, after: u64, mut f: impl FnMut(WalRecord)) -> io::Result<u64> {
    let segments = list_segments(dir)?;
    let mut n = 0;
    for (i, seg) in segments.iter().enumerate() {
        // Skip segments that end before `after`: the next one starts later.
        if segments.get(i + 1).is_some_and(|next| segment_start(next) <= after + 1) {
            continue;
        }
        let scan = scan_segment(seg, |rec| {
            if rec.offset > after {
                n += 1;
                f(rec);
            }
            Ok(())
        })?;
        if scan.torn && i + 1 < segments.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt record in non-final WAL segment {}", seg.display()),
            ));
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Kind;

    fn ev(i: u64) -> (Event, Millis) {
        let kind = if i.is_multiple_of(7) { Kind::FallWarn { confidence: 0.9 } } else { Kind::Heartbeat };
        let e = Event { device_id: format!("dev_{i:04}"), room_id: "room_000".into(), ts_ms: 1_000 + i as i64, seq: i, kind };
        (e, 5_000 + i as i64)
    }

    fn read_all(dir: &Path) -> Vec<WalRecord> {
        let mut v = Vec::new();
        replay(dir, 0, |r| v.push(r)).unwrap();
        v
    }

    #[test]
    fn append_and_replay_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut wal, rep) = Wal::open(tmp.path()).unwrap();
        assert_eq!(rep.last_offset, 0);
        let a = wal.append((1..=3).map(ev)).unwrap();
        let b = wal.append((4..=5).map(ev)).unwrap();
        assert_eq!(a.iter().chain(&b).map(|r| r.offset).collect::<Vec<_>>(), [1, 2, 3, 4, 5]);
        wal.sync().unwrap();

        let got = read_all(tmp.path());
        assert_eq!(got.len(), 5);
        assert_eq!(got[3], b[0]);
        assert_eq!(got[0].event, ev(1).0);

        let mut tail = Vec::new();
        assert_eq!(replay(tmp.path(), 3, |r| tail.push(r.offset)).unwrap(), 2);
        assert_eq!(tail, [4, 5]);
    }

    #[test]
    fn reopen_continues_offsets_and_rotates() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open_with_limit(tmp.path(), 256).unwrap();
            for i in 1..=20 {
                wal.append([ev(i)]).unwrap();
            }
        }
        assert!(list_segments(tmp.path()).unwrap().len() > 2, "rotated");
        let (mut wal, rep) = Wal::open_with_limit(tmp.path(), 256).unwrap();
        assert_eq!(rep.last_offset, 20);
        assert_eq!(wal.append([ev(21)]).unwrap()[0].offset, 21);
        let offsets: Vec<u64> = read_all(tmp.path()).iter().map(|r| r.offset).collect();
        assert_eq!(offsets, (1..=21).collect::<Vec<_>>());

        let mut tail = Vec::new();
        replay(tmp.path(), 17, |r| tail.push(r.offset)).unwrap();
        assert_eq!(tail, [18, 19, 20, 21], "segment skipping keeps the tail exact");
    }

    #[test]
    fn torn_tail_is_truncated_on_open() {
        let tmp = tempfile::tempdir().unwrap();
        let seg = {
            let (mut wal, _) = Wal::open(tmp.path()).unwrap();
            wal.append((1..=3).map(ev)).unwrap();
            list_segments(tmp.path()).unwrap().pop().unwrap()
        };
        // Simulate a crash mid-write: half a header plus garbage.
        let clean_len = fs::metadata(&seg).unwrap().len();
        OpenOptions::new().append(true).open(&seg).unwrap().write_all(&[9, 0, 0, 0, 1]).unwrap();
        assert_eq!(read_all(tmp.path()).len(), 3, "reader stops at the torn record");

        let (mut wal, rep) = Wal::open(tmp.path()).unwrap();
        assert_eq!(rep.last_offset, 3);
        assert_eq!(rep.truncated_bytes, 5);
        assert_eq!(fs::metadata(&seg).unwrap().len(), clean_len);
        wal.append([ev(4)]).unwrap();
        assert_eq!(read_all(tmp.path()).len(), 4);
    }

    #[test]
    fn bad_crc_stops_reading() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(tmp.path()).unwrap();
            wal.append((1..=3).map(ev)).unwrap();
        }
        let seg = list_segments(tmp.path()).unwrap().pop().unwrap();
        let mut bytes = fs::read(&seg).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff; // corrupt the last record's payload
        fs::write(&seg, bytes).unwrap();
        assert_eq!(read_all(tmp.path()).len(), 2);
        let (_, rep) = Wal::open(tmp.path()).unwrap();
        assert_eq!(rep.last_offset, 2);
    }
}
