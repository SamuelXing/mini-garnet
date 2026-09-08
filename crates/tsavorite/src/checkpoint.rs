//! Non-blocking **fold-over checkpoint** driven by an epoch-protected state
//! machine, following Concurrent Prefix Recovery (paper §5.1, Fig. 2(b);
//! `HybridLogCheckpointSMTask.cs`, `FoldOverSMTask.cs`, `Recovery.cs`).
//!
//! ```text
//!  REST ──► PREPARE(v) ──► IN_PROGRESS(v+1) ──► WAIT_FLUSH(v+1) ──► REST(v+1)
//!            capture         all threads now      shift ReadOnly to
//!            start = tail    write v+1 records    final; flush; write
//!                                                 metadata
//! ```
//!
//! Each arrow is `transition_and_wait`: CAS the global state, bump the epoch,
//! and wait until every thread has left the previous epoch — i.e. has
//! *observed* the new phase (this is the EPSM primitive from paper §3.3).
//!
//! Why this yields a consistent snapshot without stopping anyone:
//! * After PREPARE→IN_PROGRESS drains, no version-v operation is in flight
//!   and every new write carries `InNewVersion`.
//! * A v+1 thread never updates a v record in place (`cpr_block` in
//!   `store.rs`); it copies to the tail instead, leaving the v value intact.
//! * `final = tail` is captured once all threads are v+1, so every v record
//!   lies below `final`; v+1 records below `final` are in the fuzzy region
//!   `[start, final)` and are undone at recovery.
//! * Fold-over then shifts ReadOnly to `final`, freezing everything below it
//!   (the drain guarantees in-flight in-place writes have completed) and
//!   flushes it. The log itself *is* the checkpoint: cheap, no second copy.
//!
//! Recovery rebuilds the index by scanning the on-disk log `[begin, final)`
//! and re-inserting every valid record in address order (later records win
//! naturally because chains are address-descending). A checkpoint of the
//! index itself would only shorten this scan, so the mini skips it.

use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::hash::hash64;
use crate::log::HybridLog;
use crate::record::SEALED_BIT;
use crate::store::{Phase, Store, SystemState};

const META_MAGIC: u64 = 0x5453_4156_4f52_4954; // "TSAVORIT"

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// Version `v` captured by this checkpoint (the store runs at v+1 afterwards).
    pub version: u64,
    pub begin_address: u64,
    /// Tail at PREPARE: start of the fuzzy region.
    pub start_address: u64,
    /// Tail at WAIT_FLUSH: everything below is durable; nothing above is in the checkpoint.
    pub final_address: u64,
    /// Opaque value supplied by the caller (Garnet stores the AOF address covered).
    pub user_data: u64,
}

impl CheckpointInfo {
    pub fn write_to(&self, path: &Path) -> io::Result<()> {
        let tmp = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            for v in [
                META_MAGIC,
                self.version,
                self.begin_address,
                self.start_address,
                self.final_address,
                self.user_data,
            ] {
                f.write_all(&v.to_le_bytes())?;
            }
            f.sync_all()?;
        }
        std::fs::rename(tmp, path)
    }

    pub fn read_from(path: &Path) -> io::Result<Self> {
        let mut f = std::fs::File::open(path)?;
        let mut buf = [0u8; 48];
        f.read_exact(&mut buf)?;
        let v = |i: usize| u64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap());
        if v(0) != META_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad checkpoint magic",
            ));
        }
        Ok(CheckpointInfo {
            version: v(1),
            begin_address: v(2),
            start_address: v(3),
            final_address: v(4),
            user_data: v(5),
        })
    }
}

impl Store {
    /// Move the global state machine to `new` and block until every thread
    /// has observed it (EPSM phase change). Caller must be unprotected.
    pub fn transition_and_wait(&self, new: SystemState) {
        let cur = self.system_state();
        assert!(
            self.try_set_system_state(cur, new),
            "concurrent state machine transition"
        );
        let observed = Arc::new(AtomicBool::new(false));
        let o = observed.clone();
        self.epoch
            .bump_with_action(move || o.store(true, Ordering::SeqCst));
        let mut spins = 0u64;
        while !observed.load(Ordering::SeqCst) {
            self.epoch.bump_current_epoch();
            spins += 1;
            if spins > 100 {
                std::thread::sleep(std::time::Duration::from_micros(100));
            } else {
                std::thread::yield_now();
            }
        }
    }

    /// Take a fold-over checkpoint. `user_data` is stored in the metadata.
    /// Runs the phases above; concurrent sessions keep going throughout.
    pub fn take_checkpoint(&self, meta_path: &Path, user_data: u64) -> io::Result<CheckpointInfo> {
        let s0 = self.system_state();
        if s0.phase != Phase::Rest {
            return Err(io::Error::other("checkpoint already in progress"));
        }
        let v = s0.version;

        // REST -> PREPARE: threads learn a checkpoint is coming; records they
        // now write are still v, but meeting a v+1 record makes them refresh.
        let start_address = self.log.tail_address();
        self.set_checkpoint_start_address(start_address);
        self.transition_and_wait(SystemState {
            phase: Phase::Prepare,
            version: v,
        });

        // PREPARE -> IN_PROGRESS: version shift. Once this returns, no v
        // operation is in flight and all new records carry InNewVersion.
        self.transition_and_wait(SystemState {
            phase: Phase::InProgress,
            version: v + 1,
        });

        // IN_PROGRESS -> WAIT_FLUSH: every v record is below the tail now.
        let final_address = self.log.tail_address();
        self.transition_and_wait(SystemState {
            phase: Phase::WaitFlush,
            version: v + 1,
        });

        // Fold-over: freeze [.., final) and flush it (after the drain that the
        // ReadOnly shift performs), then persist metadata.
        self.log.shift_read_only_address(final_address);
        self.log.wait_flushed(final_address);
        self.log.flush_until(final_address, true)?; // sync
        let info = CheckpointInfo {
            version: v,
            begin_address: self.log.begin_address(),
            start_address,
            final_address,
            user_data,
        };
        info.write_to(meta_path)?;

        // WAIT_FLUSH -> REST(v+1).
        self.transition_and_wait(SystemState {
            phase: Phase::Rest,
            version: v + 1,
        });
        Ok(info)
    }

    /// Rebuild an empty store from a checkpoint on the (already attached)
    /// device: reset the log so everything is on disk, then scan
    /// `[begin, final)` inserting records; undo v+1 records in the fuzzy
    /// region by marking them invalid on disk.
    pub fn recover(&self, info: &CheckpointInfo) -> io::Result<()> {
        self.index.clear();
        self.log
            .recover_addresses(info.begin_address, info.final_address);
        self.set_system_state(SystemState {
            phase: Phase::Rest,
            version: info.version + 1,
        });
        self.set_checkpoint_start_address(u64::MAX);

        let page_size = self.log.page_size() as u64;
        let mut page_start = (info.begin_address / page_size) * page_size;
        while page_start < info.final_address {
            let page = page_start / page_size;
            let limit = (info.final_address.min(page_start + page_size) - page_start) as usize;
            let mut buf = self.log.read_page_prefix_from_disk(page, limit)?;
            let mut dirty = false;
            let from_off = (info.begin_address.saturating_sub(page_start)) as usize;
            // SAFETY: `buf` is a page-sized buffer owned by this function, and
            // recovery is single-threaded (no sessions exist yet).
            unsafe {
                HybridLog::for_each_record_in_buf(buf.as_ptr(), from_off, limit, |off, rec| {
                    let info_word = rec.info().load();
                    let addr = page_start + off as u64;
                    if !info_word.is_valid() {
                        return;
                    }
                    if info_word.in_new_version() && addr >= info.start_address {
                        // v+1 record inside the fuzzy region: not part of version v.
                        rec.info().set_invalid();
                        dirty = true;
                        return;
                    }
                    // Clear stale seal bits on the disk image.
                    if info_word.is_sealed() {
                        rec.info().0.fetch_and(!SEALED_BIT, Ordering::AcqRel);
                        dirty = true;
                    }
                    let slot = self.index.find_or_create_tag(hash64(rec.key()));
                    let cur = slot.load();
                    if cur.address() < addr {
                        assert!(slot.try_cas(cur, addr));
                    }
                })
            };
            if dirty {
                let bytes = buf.as_bytes_mut(limit);
                self.log.device().write_at(page_start, &bytes[..limit])?;
            }
            page_start += page_size;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemDevice;
    use crate::log::LogSettings;
    use crate::record::{RecordPtr, RecordSizeInfo};
    use crate::store::{InPlaceResult, SessionFunctions, Status, StoreSettings};
    use std::sync::atomic::AtomicBool;

    struct Fns;
    impl SessionFunctions for Fns {
        type Input = i64;
        type Output = i64;
        fn reader(&mut self, _k: &[u8], _i: &i64, rec: RecordPtr, out: &mut i64) -> bool {
            *out = i64::from_le_bytes(rec.value().try_into().unwrap());
            true
        }
        fn upsert_size(&mut self, key: &[u8], _i: &i64) -> RecordSizeInfo {
            RecordSizeInfo {
                key_len: key.len(),
                value_cap: 8,
                has_expiration: false,
            }
        }
        fn initial_writer(&mut self, _k: &[u8], i: &i64, rec: RecordPtr, _o: &mut i64) {
            rec.value_buf_mut().copy_from_slice(&i.to_le_bytes());
            rec.set_value_len(8);
        }
        fn in_place_writer(&mut self, _k: &[u8], i: &i64, rec: RecordPtr, _o: &mut i64) -> bool {
            rec.value_buf_mut().copy_from_slice(&i.to_le_bytes());
            true
        }
        fn initial_size(&mut self, key: &[u8], _i: &i64) -> RecordSizeInfo {
            RecordSizeInfo {
                key_len: key.len(),
                value_cap: 8,
                has_expiration: false,
            }
        }
        fn initial_updater(&mut self, _k: &[u8], i: &i64, rec: RecordPtr, o: &mut i64) {
            rec.value_buf_mut().copy_from_slice(&i.to_le_bytes());
            rec.set_value_len(8);
            *o = *i;
        }
        fn in_place_updater(
            &mut self,
            _k: &[u8],
            i: &i64,
            rec: RecordPtr,
            o: &mut i64,
        ) -> InPlaceResult {
            let v = i64::from_le_bytes(rec.value().try_into().unwrap()) + i;
            rec.value_buf_mut().copy_from_slice(&v.to_le_bytes());
            *o = v;
            InPlaceResult::Updated
        }
        fn copy_size(&mut self, key: &[u8], _i: &i64, _old: RecordPtr) -> RecordSizeInfo {
            RecordSizeInfo {
                key_len: key.len(),
                value_cap: 8,
                has_expiration: false,
            }
        }
        fn copy_updater(
            &mut self,
            _k: &[u8],
            i: &i64,
            old: RecordPtr,
            new: RecordPtr,
            o: &mut i64,
        ) {
            let v = i64::from_le_bytes(old.value().try_into().unwrap()) + i;
            new.value_buf_mut().copy_from_slice(&v.to_le_bytes());
            new.set_value_len(8);
            *o = v;
        }
    }

    fn settings() -> StoreSettings {
        StoreSettings {
            index_buckets: 64,
            log: LogSettings {
                page_bits: 10,
                memory_pages: 4,
                mutable_fraction: 0.5,
            },
        }
    }

    #[test]
    fn checkpoint_and_recover_roundtrip() {
        let dev = Arc::new(MemDevice::new());
        let dir = std::env::temp_dir().join(format!("tsav-ckpt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let meta = dir.join("checkpoint.meta");

        let store = Store::new(settings(), Box::new(dev.clone()));
        let mut s = store.new_session(Fns);
        let mut out = 0;
        for i in 0..500i64 {
            s.upsert(format!("k{i}").as_bytes(), &i, &mut out);
        }
        s.delete(b"k7");
        s.rmw(b"k8", &100, &mut out); // 108
        let info = store.take_checkpoint(&meta, 42).unwrap();
        assert_eq!(info.version, 1);
        assert_eq!(store.current_version(), 2);
        // Writes after the checkpoint must not show up after recovery.
        s.upsert(b"k1", &-1, &mut out);
        s.upsert(b"new", &1, &mut out);
        store.epoch.release_thread();
        drop(s);
        drop(store);

        let store2 = Store::new(settings(), Box::new(dev.clone()));
        let info2 = CheckpointInfo::read_from(&meta).unwrap();
        assert_eq!(info2, info);
        store2.recover(&info2).unwrap();
        assert_eq!(store2.current_version(), 2);
        let mut s2 = store2.new_session(Fns);
        assert_eq!(s2.read(b"k1", &0, &mut out), Status::Found);
        assert_eq!(out, 1, "post-checkpoint write is not in the checkpoint");
        assert_eq!(s2.read(b"new", &0, &mut out), Status::NotFound);
        assert_eq!(
            s2.read(b"k7", &0, &mut out),
            Status::NotFound,
            "tombstone recovered"
        );
        assert_eq!(s2.read(b"k8", &0, &mut out), Status::Found);
        assert_eq!(out, 108);
        assert_eq!(s2.read(b"k499", &0, &mut out), Status::Found);
        assert_eq!(out, 499);
        assert_eq!(store2.count_live(), 499);
        // The recovered store keeps working (writes go to a fresh tail page).
        assert_eq!(s2.rmw(b"k8", &1, &mut out), Status::CopyUpdated);
        assert_eq!(out, 109);
        assert!(s2.upsert(b"after", &5, &mut out).is_updated());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn checkpoint_under_concurrent_updates_is_a_consistent_prefix() {
        let dev = Arc::new(MemDevice::new());
        let dir = std::env::temp_dir().join(format!("tsav-ckpt2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let meta = dir.join("checkpoint.meta");
        let store = Store::new(
            StoreSettings {
                index_buckets: 64,
                log: LogSettings {
                    page_bits: 12,
                    memory_pages: 8,
                    mutable_fraction: 0.5,
                },
            },
            Box::new(dev.clone()),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let mut hs = Vec::new();
        // Writers keep incrementing a handful of counters while we checkpoint.
        for t in 0..4 {
            let store = store.clone();
            let stop = stop.clone();
            hs.push(std::thread::spawn(move || {
                let mut s = store.new_session(Fns);
                let mut out = 0;
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let k = format!("c{}", (n + t) % 4);
                    assert!(s.rmw(k.as_bytes(), &1, &mut out).is_updated());
                    n += 1;
                }
                store.epoch.release_thread();
                n
            }));
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        let info = store.take_checkpoint(&meta, 0).unwrap();
        stop.store(true, Ordering::Relaxed);
        let total_after: u64 = hs.into_iter().map(|h| h.join().unwrap()).sum();
        drop(store);

        let store2 = Store::new(
            StoreSettings {
                index_buckets: 64,
                log: LogSettings {
                    page_bits: 12,
                    memory_pages: 8,
                    mutable_fraction: 0.5,
                },
            },
            Box::new(dev.clone()),
        );
        store2.recover(&info).unwrap();
        let mut s2 = store2.new_session(Fns);
        let mut out = 0;
        let mut sum = 0i64;
        for k in 0..4 {
            if s2.read(format!("c{k}").as_bytes(), &0, &mut out) == Status::Found {
                assert!(out >= 0);
                sum += out;
            }
        }
        assert!(sum > 0, "checkpoint captured some work");
        assert!(
            sum <= total_after as i64,
            "checkpoint is a prefix of the executed work"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
