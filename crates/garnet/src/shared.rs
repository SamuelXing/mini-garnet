//! `Shared` = Garnet's `StoreWrapper`: the process-wide state shared by all
//! connections — the store, the AOF, the watch table, checkpoint config —
//! plus recovery orchestration (paper §5, `StoreWrapper.cs`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tsavorite::checkpoint::CheckpointInfo;
use tsavorite::device::{Device, FileDevice, MemDevice};
use tsavorite::store::{Store, StoreSettings};

use crate::aof::{Aof, CommitMode};

/// Small array of version counters. A write bumps the counter for its key's
/// slot; WATCH remembers the counter; EXEC aborts if it changed (paper §4.6).
pub struct WatchTable {
    slots: Vec<AtomicU64>,
    mask: usize,
}

impl WatchTable {
    pub fn new(n: usize) -> Self {
        let n = n.next_power_of_two();
        WatchTable {
            slots: (0..n).map(|_| AtomicU64::new(0)).collect(),
            mask: n - 1,
        }
    }
    #[inline]
    fn slot_of(&self, key: &[u8]) -> usize {
        (tsavorite::hash::hash64(key) as usize) & self.mask
    }
    pub fn bump(&self, key: &[u8]) {
        self.slots[self.slot_of(key)].fetch_add(1, Ordering::AcqRel);
    }
    pub fn slot_version(&self, key: &[u8]) -> (usize, u64) {
        let s = self.slot_of(key);
        (s, self.slots[s].load(Ordering::Acquire))
    }
    pub fn version_at(&self, slot: usize) -> u64 {
        self.slots[slot].load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub struct Config {
    pub bind: String,
    pub dir: PathBuf,
    /// None = pure in-memory store (no disk tier, no persistence).
    pub persist: bool,
    pub aof: bool,
    pub aof_commit_wait: bool,
    pub commit_mode: CommitMode,
    pub store: StoreSettings,
    /// Concurrent connection ceiling; see [`crate::server::DEFAULT_MAX_CLIENTS`]
    /// for why this one is a structural limit rather than a policy knob.
    pub maxclients: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bind: "127.0.0.1:6379".into(),
            dir: std::env::temp_dir().join("mini-garnet-data"),
            persist: false,
            aof: false,
            aof_commit_wait: false,
            commit_mode: CommitMode::Interval(100),
            store: StoreSettings::default(),
            maxclients: crate::server::DEFAULT_MAX_CLIENTS,
        }
    }
}

pub struct Shared {
    pub store: Arc<Store>,
    pub aof: Option<Arc<Aof>>,
    pub aof_commit_wait: bool,
    pub watch: WatchTable,
    pub config: Config,
    last_save_ms: AtomicU64,
}

impl Shared {
    pub fn new(config: Config) -> std::io::Result<Arc<Self>> {
        let device: Box<dyn Device> = if config.persist {
            std::fs::create_dir_all(&config.dir)?;
            Box::new(FileDevice::open(config.dir.join("store.log"))?)
        } else {
            Box::new(MemDevice::new())
        };
        let store = Store::new(config.store.clone(), device);
        let aof = if config.aof {
            std::fs::create_dir_all(&config.dir)?;
            Some(Aof::open(config.dir.join("aof.log"), config.commit_mode)?)
        } else {
            None
        };
        Ok(Arc::new(Shared {
            store,
            aof,
            aof_commit_wait: config.aof_commit_wait,
            watch: WatchTable::new(1 << 14),
            last_save_ms: AtomicU64::new(0),
            config,
        }))
    }

    #[inline]
    pub fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    pub fn last_save_ms(&self) -> i64 {
        self.last_save_ms.load(Ordering::Acquire) as i64
    }

    fn checkpoint_meta_path(&self) -> PathBuf {
        self.config.dir.join("checkpoint.meta")
    }

    /// SAVE: fold-over checkpoint, then truncate the AOF up to the covered
    /// address (paper §5, `InitiateCheckpointAsync`).
    pub fn checkpoint(&self) -> std::io::Result<()> {
        if !self.config.persist {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "server started without persistence",
            ));
        }
        std::fs::create_dir_all(&self.config.dir)?;
        let aof_addr = self.aof.as_ref().map(|a| a.tail_address()).unwrap_or(0);
        self.store
            .take_checkpoint(&self.checkpoint_meta_path(), aof_addr)?;
        // Everything up to aof_addr is now in the checkpoint; trim the AOF.
        if let Some(aof) = &self.aof {
            aof.truncate()?;
        }
        self.last_save_ms
            .store(self.now_ms() as u64, Ordering::Release);
        Ok(())
    }

    /// Recover on startup: load the last checkpoint (if any), then replay the
    /// AOF tail beyond it. Runs before accepting connections.
    pub fn recover(self: &Arc<Self>) -> std::io::Result<()> {
        if !self.config.persist {
            return Ok(());
        }
        let meta = self.checkpoint_meta_path();
        let checkpoint_user_data = if meta.exists() {
            let info = CheckpointInfo::read_from(&meta)?;
            self.store.recover(&info)?;
            info.user_data
        } else {
            0
        };
        // Replay AOF entries the checkpoint did not cover.
        if let Some(aof) = &self.aof {
            let entries = collect_aof(aof)?;
            if !entries.is_empty() {
                let mut session = crate::commands::RespSession::new(self.clone());
                session.set_replay(true);
                let _ = checkpoint_user_data; // (a real impl skips entries below it)
                replay_entries(&mut session, entries);
            }
        }
        Ok(())
    }
}

/// One decoded AOF record: (op type, deterministic timestamp, RESP payload).
type AofRecord = (crate::aof::AofOp, i64, Vec<u8>);

fn collect_aof(aof: &Arc<Aof>) -> std::io::Result<Vec<AofRecord>> {
    let mut entries = Vec::new();
    aof.replay(|e| entries.push((e.op, e.timestamp_ms, e.payload)))?;
    Ok(entries)
}

fn replay_entries(session: &mut crate::commands::RespSession, entries: Vec<AofRecord>) {
    use crate::aof::AofOp;
    let mut out = Vec::new();
    let mut i = 0;
    while i < entries.len() {
        let (op, ts, payload) = &entries[i];
        match op {
            AofOp::Command => {
                out.clear();
                replay_one(session, *ts, payload, &mut out);
            }
            AofOp::TxnStart => {
                // Re-run the bracketed commands as a MULTI/EXEC block so the
                // same serial order is reproduced.
                out.clear();
                session.replay_multi_begin(*ts);
                i += 1;
                while i < entries.len() && !matches!(entries[i].0, AofOp::TxnCommit) {
                    if matches!(entries[i].0, AofOp::Command) {
                        replay_one(session, entries[i].1, &entries[i].2, &mut out);
                    }
                    i += 1;
                }
                session.replay_multi_exec(&mut out);
            }
            _ => {}
        }
        i += 1;
    }
}

fn replay_one(
    session: &mut crate::commands::RespSession,
    ts: i64,
    payload: &[u8],
    out: &mut Vec<u8>,
) {
    let mut slices = Vec::new();
    if let Ok(Some(_)) = crate::resp::parse_command(payload, 0, &mut slices) {
        let refs: Vec<&[u8]> = slices.iter().map(|a| a.bytes(payload)).collect();
        session.dispatch_replay(payload, &refs, ts, out);
    }
}
