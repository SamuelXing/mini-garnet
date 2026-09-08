//! Append-Only File — Garnet's Deterministic Operation Log (DOL), simplified
//! (paper §5.2; `AofHeader.cs`, `GarnetAppendOnlyFile.cs`, `AofProcessor.cs`).
//!
//! Every store-mutating command is logged as a *deterministic operation*: the
//! original RESP command bytes plus the captured timestamp used for expiration.
//! Replay re-executes the commands through the same dispatch path with the
//! logged timestamp, so it reproduces the exact state — including which keys
//! had expired — without consulting the wall clock. Redis/AOF logs the command
//! too; the determinism captured alongside it is the DOL idea.
//!
//! Framing (little-endian):
//! ```text
//!   [u32 payload_len][u8 version][u8 op_type][u16 _reserved][i64 timestamp_ms][payload...]
//! ```
//! `payload` is the raw RESP command array (`*N\r\n$..\r\n..`).
//!
//! Commit policy:
//! * `CommitMode::Always` fsyncs after every append (full durability: the
//!   server waits for commit before acking the write).
//! * `CommitMode::Interval(ms)` fsyncs from a background thread (lazy
//!   durability / group commit).
//! * `CommitMode::Never` keeps everything in the OS page cache (pure cache).

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub const AOF_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum AofOp {
    /// A single write command (SET/DEL/INCR/…): payload is its RESP bytes.
    Command = 1,
    /// Transaction bracket start.
    TxnStart = 2,
    /// Transaction bracket commit.
    TxnCommit = 3,
}

impl AofOp {
    fn from_u8(v: u8) -> Option<AofOp> {
        Some(match v {
            1 => AofOp::Command,
            2 => AofOp::TxnStart,
            3 => AofOp::TxnCommit,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CommitMode {
    Never,
    Interval(u64),
    Always,
}

pub struct AofEntry {
    pub op: AofOp,
    pub timestamp_ms: i64,
    pub payload: Vec<u8>,
}

const HEADER_LEN: usize = 4 + 1 + 1 + 2 + 8;

struct Inner {
    file: File,
    /// Bytes appended to the buffer but not yet written to the file.
    buf: Vec<u8>,
    /// Total bytes handed to the OS (tail address of the log).
    written: u64,
    /// Bytes fsynced.
    committed: u64,
}

pub struct Aof {
    path: PathBuf,
    inner: Mutex<Inner>,
    tail: AtomicU64,
    /// Parks on `inner`; woken when `inner.committed` advances.
    commit_cvar: Condvar,
    mode: CommitMode,
    shutdown: Arc<(Mutex<bool>, Condvar)>,
}

impl Aof {
    pub fn open(path: impl AsRef<Path>, mode: CommitMode) -> io::Result<Arc<Self>> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let written = file.metadata()?.len();
        let aof = Arc::new(Aof {
            path,
            inner: Mutex::new(Inner {
                file,
                buf: Vec::with_capacity(64 * 1024),
                written,
                committed: written,
            }),
            tail: AtomicU64::new(written),
            commit_cvar: Condvar::new(),
            mode,
            shutdown: Arc::new((Mutex::new(false), Condvar::new())),
        });
        if let CommitMode::Interval(ms) = mode {
            let a = aof.clone();
            let sd = aof.shutdown.clone();
            std::thread::spawn(move || {
                let (lock, cv) = &*sd;
                loop {
                    let mut stop = lock.lock().unwrap();
                    let res = cv
                        .wait_timeout(stop, std::time::Duration::from_millis(ms.max(1)))
                        .unwrap();
                    stop = res.0;
                    if *stop {
                        break;
                    }
                    drop(stop);
                    let _ = a.commit();
                }
            });
        }
        Ok(aof)
    }

    /// Tail address of the log (bytes handed to the OS).
    pub fn tail_address(&self) -> u64 {
        self.tail.load(Ordering::Acquire)
    }

    pub fn committed_address(&self) -> u64 {
        self.inner.lock().unwrap().committed
    }

    /// Append an entry. Returns the address just past it. With `CommitMode::Always`
    /// this also fsyncs before returning (the durability barrier).
    pub fn enqueue(&self, op: AofOp, timestamp_ms: i64, payload: &[u8]) -> io::Result<u64> {
        let end = {
            let mut inner = self.inner.lock().unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            hdr[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
            hdr[4] = AOF_VERSION;
            hdr[5] = op as u8;
            hdr[8..16].copy_from_slice(&timestamp_ms.to_le_bytes());
            inner.buf.extend_from_slice(&hdr);
            inner.buf.extend_from_slice(payload);
            let end = inner.written + inner.buf.len() as u64;
            self.tail.store(end, Ordering::Release);
            // Flush buffer to the file if it is getting large (still not fsynced).
            if inner.buf.len() >= 64 * 1024 {
                self.flush_buffer_locked(&mut inner)?;
            }
            end
        };
        if matches!(self.mode, CommitMode::Always) {
            self.commit()?;
        }
        Ok(end)
    }

    fn flush_buffer_locked(&self, inner: &mut Inner) -> io::Result<()> {
        if inner.buf.is_empty() {
            return Ok(());
        }
        inner.file.seek(SeekFrom::Start(inner.written))?;
        inner.file.write_all(&inner.buf)?;
        inner.written += inner.buf.len() as u64;
        inner.buf.clear();
        Ok(())
    }

    /// Flush buffered bytes to the file and fsync (group commit).
    pub fn commit(&self) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        self.flush_buffer_locked(&mut inner)?;
        if inner.committed < inner.written {
            inner.file.sync_data()?;
            inner.committed = inner.written;
            self.commit_cvar.notify_all();
        }
        Ok(())
    }

    /// Block until at least `addr` is committed (the `--aof-commit-wait` barrier).
    pub fn wait_committed(&self, addr: u64) -> io::Result<()> {
        if self.committed_address() >= addr {
            return Ok(());
        }
        self.commit()?;
        let mut inner = self.inner.lock().unwrap();
        while inner.committed < addr {
            inner = self.commit_cvar.wait(inner).unwrap();
        }
        Ok(())
    }

    /// Truncate the log to empty after a checkpoint has captured all prior
    /// state (`TruncateUntil` + fresh commit).
    pub fn truncate(&self) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.buf.clear();
        inner.file.set_len(0)?;
        inner.file.sync_all()?;
        inner.written = 0;
        inner.committed = 0;
        self.tail.store(0, Ordering::Release);
        self.commit_cvar.notify_all();
        Ok(())
    }

    /// Replay committed entries from the start, invoking `f` for each. Reads
    /// only the durable prefix (ignores a torn tail from a crash mid-write).
    pub fn replay(&self, mut f: impl FnMut(AofEntry)) -> io::Result<()> {
        self.commit()?;
        let durable = self.committed_address();
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let mut r = BufReader::new(file);
        let mut pos = 0u64;
        loop {
            if pos + HEADER_LEN as u64 > durable {
                break;
            }
            let mut hdr = [0u8; HEADER_LEN];
            if r.read_exact(&mut hdr).is_err() {
                break;
            }
            let plen = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
            let version = hdr[4];
            let Some(op) = AofOp::from_u8(hdr[5]) else {
                break;
            };
            let ts = i64::from_le_bytes(hdr[8..16].try_into().unwrap());
            if version != AOF_VERSION || pos + HEADER_LEN as u64 + plen as u64 > durable {
                break; // torn/partial entry
            }
            let mut payload = vec![0u8; plen];
            if r.read_exact(&mut payload).is_err() {
                break;
            }
            pos += HEADER_LEN as u64 + plen as u64;
            f(AofEntry {
                op,
                timestamp_ms: ts,
                payload,
            });
        }
        Ok(())
    }
}

impl Drop for Aof {
    fn drop(&mut self) {
        let (lock, cv) = &*self.shutdown;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        let _ = self.commit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("mini-garnet-aof-{}-{}", std::process::id(), name))
    }

    #[test]
    fn append_and_replay() {
        let path = tmp("basic.aof");
        let _ = std::fs::remove_file(&path);
        {
            let aof = Aof::open(&path, CommitMode::Always).unwrap();
            aof.enqueue(
                AofOp::Command,
                100,
                b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n",
            )
            .unwrap();
            aof.enqueue(AofOp::Command, 200, b"*2\r\n$4\r\nINCR\r\n$1\r\na\r\n")
                .unwrap();
        }
        let aof = Aof::open(&path, CommitMode::Never).unwrap();
        let mut got = Vec::new();
        aof.replay(|e| got.push((e.op, e.timestamp_ms, e.payload)))
            .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].1, 100);
        assert_eq!(got[1].0, AofOp::Command);
        assert!(got[1].2.windows(4).any(|w| w == b"INCR"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn commit_wait_barrier() {
        let path = tmp("commit.aof");
        let _ = std::fs::remove_file(&path);
        let aof = Aof::open(&path, CommitMode::Interval(1000)).unwrap();
        let end = aof
            .enqueue(AofOp::Command, 1, b"*1\r\n$4\r\nPING\r\n")
            .unwrap();
        assert!(aof.committed_address() < end);
        aof.wait_committed(end).unwrap();
        assert!(aof.committed_address() >= end);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn torn_tail_is_ignored() {
        let path = tmp("torn.aof");
        let _ = std::fs::remove_file(&path);
        {
            let aof = Aof::open(&path, CommitMode::Always).unwrap();
            aof.enqueue(AofOp::Command, 1, b"hello").unwrap();
        }
        // Append garbage that looks like a header but claims more than exists.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            hdr[0..4].copy_from_slice(&9999u32.to_le_bytes());
            hdr[4] = AOF_VERSION;
            hdr[5] = AofOp::Command as u8;
            f.write_all(&hdr).unwrap();
            f.write_all(b"short").unwrap();
        }
        let aof = Aof::open(&path, CommitMode::Never).unwrap();
        let mut n = 0;
        aof.replay(|_| n += 1).unwrap();
        assert_eq!(n, 1, "only the intact entry replays");
        std::fs::remove_file(&path).ok();
    }
}
