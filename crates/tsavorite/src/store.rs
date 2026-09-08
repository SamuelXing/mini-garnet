//! The store: hash index + hybrid log + epoch, exposing the **RUMDS**
//! narrow-waist interface (Read, Upsert, Modify/RMW, Delete, Scan) with
//! application callbacks (paper §4.2, `ISessionFunctions.cs`,
//! `Internal{Read,Upsert,RMW,Delete}.cs`).
//!
//! # Region decision (the heart of Tsavorite)
//!
//! Every operation hashes the key, finds/creates the index entry, takes the
//! bucket latch (S for Read, X for updates), walks the chain to the newest
//! record for the key, then branches **purely on that record's address**:
//!
//! | record address is in…             | Read            | Upsert                | RMW                        | Delete            |
//! |-----------------------------------|-----------------|-----------------------|----------------------------|-------------------|
//! | `[ReadOnly, Tail)` mutable        | `reader`        | `in_place_writer`     | `in_place_updater`         | tombstone in place|
//! | `[SafeReadOnly, ReadOnly)` fuzzy  | `reader`        | (search floor is RO)  | **RETRY_LATER**            | RCU tombstone     |
//! | `[Head, SafeReadOnly)` immutable  | `reader`        | new record at tail    | `copy_updater` → tail, seal old | RCU tombstone |
//! | `[Begin, Head)` on disk           | read from disk  | blind insert at tail  | disk read, then copy-update| disk read, then tombstone |
//! | absent                            | NotFound        | `initial_writer`      | `initial_updater`          | NotFound          |
//!
//! Structure modifications are latch-free: allocate at the tail with
//! fetch-and-add, fill the record while it is *sealed+invalid*, then a single
//! CAS on the hash entry publishes it and prepends it to the chain. On CAS
//! failure the record is left invalid and the operation retries.
//!
//! # Concurrency rules
//! * Every op runs inside epoch protection (`protect`/`unprotect` per op,
//!   as `BasicContext` does) so pages it points into cannot be evicted.
//! * Bucket latches are *bounded-spin*; failure → `RetryLater` → refresh epoch.
//! * An RCU seals the old record; anyone else who then sees it retries.
//! * Transactions (`txn_mode`) hold bucket latches up front and skip the
//!   per-op ephemeral latching.
//!
//! # CPR checkpoint versioning (paper §5.1)
//! The store has a global `(phase, version)` word. During a checkpoint the
//! phase walks `REST → PREPARE → IN_PROGRESS → WAIT_FLUSH → REST` via the
//! epoch-protected state machine (`checkpoint.rs`). Records written while
//! in `IN_PROGRESS/WAIT_FLUSH` carry `InNewVersion`; a v+1 thread must never
//! update a v record in place (it copies instead), and a thread still in
//! PREPARE that meets a v+1 record refreshes and joins v+1 (`CPR_SHIFT`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::device::Device;
use crate::epoch::{EpochGuard, LightEpoch};
use crate::hash::hash64;
use crate::index::{HashEntry, HashIndex, SlotRef};
use crate::log::{AlignedBuf, HybridLog, LogSettings, FIRST_VALID_ADDRESS};
use crate::record::{RecordInfo, RecordPtr, RecordSizeInfo, SEALED_BIT};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    NotFound,
    Found,
    Created,
    InPlaceUpdated,
    CopyUpdated,
    /// The key existed and is now tombstoned.
    Deleted,
    /// The operation was cancelled by a `need_*` callback on an existing record.
    Canceled,
    /// Could not make progress (log full / IO error).
    Error,
}

impl Status {
    pub fn is_updated(self) -> bool {
        matches!(
            self,
            Status::Created | Status::InPlaceUpdated | Status::CopyUpdated | Status::Deleted
        )
    }
}

/// Result of an in-place update callback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InPlaceResult {
    Updated,
    /// Does not fit (or is otherwise not possible) in place: copy to the tail.
    NeedCopy,
    /// The record is logically expired: tombstone it and continue as if the
    /// key did not exist (`RMWAction.ExpireAndResume`).
    Expired,
    /// Do nothing (e.g. SETNX on an existing key); reports `Status::Found`.
    Cancel,
    /// Tombstone this record and stop, without falling through to the
    /// initial-update path (Tsavorite's `RMWAction.ExpireAndStop`). GETDEL is
    /// the motivating command: it needs the old value *and* the deletion in one
    /// atomic pass.
    Delete,
}

/// What the copy-to-tail path should do with a record it could not update in
/// place. The same three choices as [`InPlaceResult`], at the copy boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyDecision {
    /// Allocate a new record and run `copy_updater`.
    Copy,
    /// Leave the record alone (e.g. SETNX on an existing key).
    Cancel,
    /// Append a tombstone instead of a new value. The callback has already
    /// written whatever reply it owes out of the old record.
    Delete,
}

/// Application callbacks. `Input` carries the command and *all* its
/// non-determinism (timestamps, random numbers) so that the same input
/// replayed from the log evolves the record identically (paper §3.2).
pub trait SessionFunctions {
    type Input;
    type Output;

    // ---- Read ----
    /// Transform the found value into `out`. Return false to report NotFound
    /// (e.g. the record is expired).
    fn reader(
        &mut self,
        key: &[u8],
        input: &Self::Input,
        rec: RecordPtr,
        out: &mut Self::Output,
    ) -> bool;

    // ---- Upsert (blind write) ----
    fn upsert_size(&mut self, key: &[u8], input: &Self::Input) -> RecordSizeInfo;
    fn initial_writer(
        &mut self,
        key: &[u8],
        input: &Self::Input,
        rec: RecordPtr,
        out: &mut Self::Output,
    );
    fn post_initial_writer(
        &mut self,
        _key: &[u8],
        _input: &Self::Input,
        _rec: RecordPtr,
        _out: &mut Self::Output,
    ) {
    }
    /// Overwrite in place; return false if the new value does not fit.
    fn in_place_writer(
        &mut self,
        key: &[u8],
        input: &Self::Input,
        rec: RecordPtr,
        out: &mut Self::Output,
    ) -> bool;

    // ---- RMW (Modify) ----
    fn need_initial_update(
        &mut self,
        _key: &[u8],
        _input: &Self::Input,
        _out: &mut Self::Output,
    ) -> bool {
        true
    }
    fn initial_size(&mut self, key: &[u8], input: &Self::Input) -> RecordSizeInfo;
    fn initial_updater(
        &mut self,
        key: &[u8],
        input: &Self::Input,
        rec: RecordPtr,
        out: &mut Self::Output,
    );
    fn post_initial_updater(
        &mut self,
        _key: &[u8],
        _input: &Self::Input,
        _rec: RecordPtr,
        _out: &mut Self::Output,
    ) {
    }
    fn in_place_updater(
        &mut self,
        key: &[u8],
        input: &Self::Input,
        rec: RecordPtr,
        out: &mut Self::Output,
    ) -> InPlaceResult;
    fn need_copy_update(
        &mut self,
        _key: &[u8],
        _input: &Self::Input,
        _old: RecordPtr,
        _out: &mut Self::Output,
    ) -> CopyDecision {
        CopyDecision::Copy
    }
    fn copy_size(&mut self, key: &[u8], input: &Self::Input, old: RecordPtr) -> RecordSizeInfo;
    fn copy_updater(
        &mut self,
        key: &[u8],
        input: &Self::Input,
        old: RecordPtr,
        new: RecordPtr,
        out: &mut Self::Output,
    );
    fn post_copy_updater(
        &mut self,
        _key: &[u8],
        _input: &Self::Input,
        _new: RecordPtr,
        _out: &mut Self::Output,
    ) {
    }

    // ---- Delete ----
    /// Called after a record was tombstoned (in place or via a new tombstone record).
    fn post_deleter(&mut self, _key: &[u8]) {}
}

// ---------------------------------------------------------------------------
// System state (phase + version) for CPR
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    Rest = 0,
    Prepare = 1,
    InProgress = 2,
    WaitFlush = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SystemState {
    pub phase: Phase,
    pub version: u64,
}

impl SystemState {
    pub fn pack(self) -> u64 {
        ((self.phase as u64) << 56) | (self.version & ((1 << 56) - 1))
    }
    pub fn unpack(w: u64) -> Self {
        let phase = match w >> 56 {
            0 => Phase::Rest,
            1 => Phase::Prepare,
            2 => Phase::InProgress,
            _ => Phase::WaitFlush,
        };
        SystemState {
            phase,
            version: w & ((1 << 56) - 1),
        }
    }
    #[inline]
    pub fn in_new_version(self) -> bool {
        matches!(self.phase, Phase::InProgress | Phase::WaitFlush)
    }
}

#[derive(Clone, Debug)]
pub struct StoreSettings {
    pub index_buckets: usize,
    pub log: LogSettings,
}

impl Default for StoreSettings {
    fn default() -> Self {
        StoreSettings {
            index_buckets: 1 << 16,
            log: LogSettings::default(),
        }
    }
}

pub struct Store {
    pub epoch: Arc<LightEpoch>,
    pub index: HashIndex,
    pub log: Arc<HybridLog>,
    system_state: AtomicU64,
    /// Tail captured at checkpoint PREPARE; records at/after it with
    /// `InNewVersion` belong to version v+1.
    checkpoint_start_address: AtomicU64,
}

impl Store {
    pub fn new(settings: StoreSettings, device: Box<dyn Device>) -> Arc<Self> {
        let epoch = Arc::new(LightEpoch::new());
        let log = HybridLog::new(settings.log, epoch.clone(), device);
        Arc::new(Store {
            epoch,
            index: HashIndex::new(settings.index_buckets),
            log,
            system_state: AtomicU64::new(
                SystemState {
                    phase: Phase::Rest,
                    version: 1,
                }
                .pack(),
            ),
            checkpoint_start_address: AtomicU64::new(u64::MAX),
        })
    }

    #[inline]
    pub fn system_state(&self) -> SystemState {
        SystemState::unpack(self.system_state.load(Ordering::SeqCst))
    }

    pub fn current_version(&self) -> u64 {
        self.system_state().version
    }

    /// CAS the global state (used by the checkpoint state machine).
    pub fn try_set_system_state(&self, expected: SystemState, new: SystemState) -> bool {
        self.system_state
            .compare_exchange(
                expected.pack(),
                new.pack(),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    pub fn set_system_state(&self, s: SystemState) {
        self.system_state.store(s.pack(), Ordering::SeqCst);
    }

    #[inline]
    pub fn checkpoint_start_address(&self) -> u64 {
        self.checkpoint_start_address.load(Ordering::Acquire)
    }
    pub fn set_checkpoint_start_address(&self, a: u64) {
        self.checkpoint_start_address.store(a, Ordering::Release);
    }

    /// Whether `info` at `addr` was written in the new (v+1) version of the
    /// checkpoint in progress.
    #[inline]
    fn record_in_new_version(&self, addr: u64, info: crate::record::RecordInfo) -> bool {
        info.in_new_version() && addr >= self.checkpoint_start_address()
    }

    /// Open a session. Each Garnet connection owns one (FIFO per session).
    pub fn new_session<F: SessionFunctions>(self: &Arc<Self>, functions: F) -> Session<F> {
        Session {
            store: Arc::clone(self),
            functions,
            txn_mode: false,
            txn_locks: Vec::new(),
            state: self.system_state(),
        }
    }

    /// Number of live (valid, non-tombstoned, latest-version) records.
    /// Implemented as a scan with a counting callback (paper: DBSIZE).
    pub fn count_live(&self) -> usize {
        let mut n = 0;
        self.scan_live(|_, _| n += 1);
        n
    }

    /// Visit every live record: the newest record for each key that is not a
    /// tombstone. Liveness is checked against the hash index, exactly like
    /// Tsavorite's scan (`ScanIterator` with the "latest version" check).
    pub fn scan_live(&self, mut f: impl FnMut(u64, RecordPtr)) {
        let g = EpochGuard::new(&self.epoch);
        let begin = self.log.begin_address();
        let tail = self.log.tail_address();
        let mut last_page = u64::MAX;
        let page_bits = self.log.page_bits();
        let res = self.log.scan(begin, tail, |addr, rec| {
            let page = addr >> page_bits; // refresh the epoch once per page
            if page != last_page {
                last_page = page;
                g.refresh();
            }
            let info = rec.info().load();
            if !info.is_valid() || info.is_tombstone() {
                return;
            }
            if self.is_latest(addr, rec.key()) {
                f(addr, rec);
            }
        });
        if let Err(e) = res {
            eprintln!("tsavorite: scan error: {e}");
        }
    }

    /// Is the record at `addr` the newest record for `key`? Walks the chain
    /// from the index entry down to `addr` (addresses above `addr` are newer).
    fn is_latest(&self, addr: u64, key: &[u8]) -> bool {
        let hash = hash64(key);
        let Some(slot) = self.index.find_tag(hash) else {
            return false;
        };
        let mut cur = slot.load().address();
        let head = self.log.head_address();
        let begin = self.log.begin_address();
        while cur > addr && cur >= begin {
            if cur >= head {
                let rec = self.log.record_at(cur);
                if rec.key() == key && rec.info().load().is_valid() {
                    return false;
                }
                cur = rec.info().load().previous_address();
            } else {
                let Ok(buf) = self.log.read_record_from_disk(cur) else {
                    return false;
                };
                let rec = buf.record();
                if rec.key() == key {
                    return false;
                }
                cur = rec.info().load().previous_address();
            }
        }
        cur == addr
    }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

enum Op {
    Done(Status),
    RetryNow,
    RetryLater,
    /// The chain led below HeadAddress; `addr` is the first on-disk address,
    /// `latest` the chain head at the time (for the in-memory re-check).
    RecordOnDisk {
        addr: u64,
        latest: u64,
    },
    CprShift,
}

enum Trace {
    Found(u64),
    Stopped(u64),
}

/// RAII bucket latch. In transaction mode the session already holds the
/// latches, so this is a no-op.
struct Latch<'a> {
    index: &'a HashIndex,
    bucket: usize,
    exclusive: bool,
    held: bool,
}

impl Drop for Latch<'_> {
    fn drop(&mut self) {
        if self.held {
            if self.exclusive {
                self.index.unlock_exclusive(self.bucket);
            } else {
                self.index.unlock_shared(self.bucket);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockType {
    Shared,
    Exclusive,
}

pub struct Session<F: SessionFunctions> {
    store: Arc<Store>,
    pub functions: F,
    txn_mode: bool,
    txn_locks: Vec<(usize, LockType)>,
    state: SystemState,
}

const MAX_RETRIES: usize = 1_000_000;

fn latch(index: &HashIndex, txn_mode: bool, bucket: usize, exclusive: bool) -> Option<Latch<'_>> {
    if txn_mode {
        return Some(Latch {
            index,
            bucket,
            exclusive,
            held: false,
        });
    }
    let ok = if exclusive {
        index.try_lock_exclusive(bucket)
    } else {
        index.try_lock_shared(bucket)
    };
    if ok {
        Some(Latch {
            index,
            bucket,
            exclusive,
            held: true,
        })
    } else {
        None
    }
}

impl<F: SessionFunctions> Session<F> {
    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// Re-read the global phase/version (`InternalRefresh`).
    #[inline]
    fn sync_state(&mut self) {
        self.state = self.store.system_state();
    }

    #[inline]
    pub fn current_version(&self) -> u64 {
        self.state.version
    }

    /// Walk the in-memory chain from `addr` looking for `key`, not going below `min`.
    fn trace_back(&self, mut addr: u64, key: &[u8], min: u64) -> Trace {
        while addr >= min && addr >= FIRST_VALID_ADDRESS {
            let rec = self.store.log.record_at(addr);
            if rec.key() == key {
                return Trace::Found(addr);
            }
            addr = rec.info().load().previous_address();
        }
        Trace::Stopped(addr)
    }

    /// Allocate + init a record at the tail, or RetryLater if the log is full.
    fn allocate_record(
        &self,
        size: &RecordSizeInfo,
        key: &[u8],
        prev: u64,
    ) -> Result<(u64, RecordPtr), ()> {
        let total = size.total_size();
        loop {
            match self.store.log.try_allocate_retry_now(total) {
                Some(addr) => {
                    // The chain must stay address-descending; a slower thread
                    // could have allocated below the current chain head.
                    if addr <= prev {
                        self.store
                            .log
                            .record_at(addr)
                            .info()
                            .0
                            .store(0, Ordering::Release);
                        continue;
                    }
                    let rec = self.store.log.record_at(addr);
                    rec.init(prev, self.state.in_new_version(), size, key);
                    return Ok((addr, rec));
                }
                None => return Err(()),
            }
        }
    }

    /// Publish `new_addr` in the index entry (`CASRecordIntoChain`).
    fn cas_into_chain(
        &self,
        slot: &SlotRef,
        expected: HashEntry,
        new_addr: u64,
        rec: RecordPtr,
    ) -> bool {
        if slot.try_cas(expected, new_addr) {
            rec.info().unseal_and_validate();
            true
        } else {
            // Leave the record sealed+invalid; the space is wasted (Tsavorite
            // would hand it to the revivification free list).
            rec.info().seal_and_invalidate();
            false
        }
    }

    /// Read the chain on disk starting at `addr` until `key` matches or the
    /// chain drops below BeginAddress.
    fn find_on_disk(&self, mut addr: u64, key: &[u8]) -> Option<AlignedBuf> {
        let begin = self.store.log.begin_address();
        while addr >= begin && addr >= FIRST_VALID_ADDRESS {
            let buf = self.store.log.read_record_from_disk(addr).ok()?;
            let rec = buf.record();
            // Sealed bits are meaningless on disk images.
            rec.info().0.fetch_and(!SEALED_BIT, Ordering::AcqRel);
            if rec.key() == key {
                return Some(buf);
            }
            addr = rec.info().load().previous_address();
        }
        None
    }

    /// Run one attempt of an operation while protected by the epoch.
    ///
    /// Protection is per operation, as in Tsavorite's `BasicContext`
    /// (`UnsafeResumeThread` / `UnsafeSuspendThread`): entering publishes the
    /// current epoch into this thread's slot so no page it touches can be freed
    /// underneath it, and leaving lets reclamation proceed.
    fn protected<R>(&mut self, body: impl FnOnce(&mut Self) -> R) -> R {
        struct Unprotect<'a>(&'a LightEpoch);
        impl Drop for Unprotect<'_> {
            fn drop(&mut self) {
                self.0.unprotect();
            }
        }
        // SAFETY: `self.store` is an `Arc` field that `body` never reassigns or
        // drops (it only borrows `self`), so the `LightEpoch` stays alive and at
        // a fixed address for the whole call. Detaching the borrow from `self`
        // is what lets `body` take `&mut self`; the alternative is an
        // `Arc::clone` per operation, i.e. two atomic RMWs on a refcount shared
        // by every connection thread, which is exactly the contention this
        // engine exists to avoid.
        let epoch: &LightEpoch = unsafe { &*Arc::as_ptr(&self.store.epoch) };
        epoch.protect();
        let _guard = Unprotect(epoch);
        self.sync_state();
        body(self)
    }

    /// Back off after a non-terminal outcome. Returns false once the operation
    /// has retried so many times that something is certainly wrong.
    ///
    /// The three outcomes need different treatment, and `RetryLater` is the
    /// interesting one: it means some boundary move is pending, so this thread
    /// bumps the epoch to give the queued drain action (page flush, page close)
    /// a chance to run. Without that, a thread whose allocation is blocked on a
    /// page close could sleep forever waiting for a drain nobody triggers.
    /// Concurrent Prefix Recovery gate, applied to every record an operation
    /// finds before it decides what to do with it (paper §5.1).
    ///
    /// Two rules, and they are the only places version numbers matter:
    /// * A **v thread** that finds a **v+1** record has been overtaken by the
    ///   version shift and must restart so it re-reads the phase.
    /// * A record that is sealed or invalid is mid read-copy-update, so retry
    ///   rather than trust it.
    ///
    /// Returns the outcome to propagate, or `None` to continue normally.
    #[inline]
    fn cpr_gate(&self, addr: u64, info: RecordInfo) -> Option<Op> {
        if self.state.phase == Phase::Prepare && self.store.record_in_new_version(addr, info) {
            return Some(Op::CprShift);
        }
        if info.is_closed() {
            return Some(Op::RetryLater);
        }
        None
    }

    /// Whether a **v+1 thread** must copy instead of updating in place, because
    /// the record it found still belongs to version v and the checkpoint needs
    /// that value intact. This is what makes the snapshot consistent without
    /// blocking anyone.
    #[inline]
    fn cpr_blocks_in_place(&self, addr: u64, info: RecordInfo) -> bool {
        self.state.in_new_version() && !self.store.record_in_new_version(addr, info)
    }

    fn handle_retry(&mut self, op: &str, outcome: Op, attempt: &mut usize) -> bool {
        *attempt += 1;
        if *attempt > MAX_RETRIES {
            eprintln!("tsavorite: giving up after {MAX_RETRIES} retries ({op})");
            return false;
        }
        match outcome {
            // Lost a CAS or another thread owns the page turn: pure contention.
            Op::RetryNow => std::hint::spin_loop(),
            // A pending epoch action must complete first. Drain it, then yield.
            Op::RetryLater => {
                self.store.epoch.bump_current_epoch();
                std::thread::yield_now();
            }
            // The checkpoint state machine moved; `protected` re-reads it.
            Op::CprShift => std::thread::yield_now(),
            Op::Done(_) | Op::RecordOnDisk { .. } => unreachable!("terminal outcome"),
        }
        true
    }

    // ---------------------------------------------------------------------
    // Read
    // ---------------------------------------------------------------------

    pub fn read(&mut self, key: &[u8], input: &F::Input, out: &mut F::Output) -> Status {
        let hash = hash64(key);
        let mut attempt = 0;
        loop {
            let r = self.protected(|s| s.internal_read(key, hash, input, out));
            match r {
                Op::Done(s) => return s,
                Op::RecordOnDisk { addr, .. } => {
                    return match self.find_on_disk(addr, key) {
                        Some(buf) => {
                            let rec = buf.record();
                            let s = if !rec.info().load().is_tombstone()
                                && self.functions.reader(key, input, rec, out)
                            {
                                Status::Found
                            } else {
                                Status::NotFound
                            };
                            s
                        }
                        None => Status::NotFound,
                    };
                }
                other => {
                    if !self.handle_retry("read", other, &mut attempt) {
                        return Status::Error;
                    }
                }
            }
        }
    }

    fn internal_read(
        &mut self,
        key: &[u8],
        hash: u64,
        input: &F::Input,
        out: &mut F::Output,
    ) -> Op {
        let store = Arc::clone(&self.store);
        let Some(slot) = store.index.find_tag(hash) else {
            return Op::Done(Status::NotFound);
        };
        let Some(_latch) = latch(&store.index, self.txn_mode, slot.bucket, false) else {
            return Op::RetryLater;
        };
        let entry = slot.load();
        let head = store.log.head_address();
        match self.trace_back(entry.address(), key, head) {
            Trace::Found(addr) => {
                let rec = store.log.record_at(addr);
                let info = rec.info().load();
                if let Some(op) = self.cpr_gate(addr, info) {
                    return op;
                }
                if info.is_tombstone() {
                    return Op::Done(Status::NotFound);
                }
                // Reader runs under the shared latch (paper §4.5).
                if self.functions.reader(key, input, rec, out) {
                    Op::Done(Status::Found)
                } else {
                    Op::Done(Status::NotFound)
                }
            }
            Trace::Stopped(addr) => {
                if addr >= store.log.begin_address() && addr >= FIRST_VALID_ADDRESS {
                    Op::RecordOnDisk {
                        addr,
                        latest: entry.address(),
                    }
                } else {
                    Op::Done(Status::NotFound)
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // Upsert
    // ---------------------------------------------------------------------

    pub fn upsert(&mut self, key: &[u8], input: &F::Input, out: &mut F::Output) -> Status {
        let hash = hash64(key);
        let mut attempt = 0;
        loop {
            let r = self.protected(|s| s.internal_upsert(key, hash, input, out));
            match r {
                Op::Done(s) => return s,
                Op::RecordOnDisk { .. } => unreachable!("upsert never reads disk"),
                other => {
                    if !self.handle_retry("upsert", other, &mut attempt) {
                        return Status::Error;
                    }
                }
            }
        }
    }

    fn internal_upsert(
        &mut self,
        key: &[u8],
        hash: u64,
        input: &F::Input,
        out: &mut F::Output,
    ) -> Op {
        let store = Arc::clone(&self.store);
        let slot = store.index.find_or_create_tag(hash);
        let Some(_latch) = latch(&store.index, self.txn_mode, slot.bucket, true) else {
            return Op::RetryLater;
        };
        let entry = slot.load();
        let latest = entry.address();
        // Upsert is blind: it only looks in the mutable region.
        let ro = store.log.read_only_address();
        let mut old_to_seal = None;
        if let Trace::Found(addr) = self.trace_back(latest, key, ro) {
            let rec = store.log.record_at(addr);
            let info = rec.info().load();
            if let Some(op) = self.cpr_gate(addr, info) {
                return op;
            }
            // A v+1 thread must not overwrite a v record in place.
            if !info.is_tombstone()
                && !self.cpr_blocks_in_place(addr, info)
                && self.functions.in_place_writer(key, input, rec, out)
            {
                rec.info().set_modified();
                return Op::Done(Status::InPlaceUpdated);
            }
            if !info.is_tombstone() {
                old_to_seal = Some(rec);
            }
        }
        // Missing keys and values that cannot be updated in place share this path.
        let size = self.functions.upsert_size(key, input);
        let Ok((new_addr, new_rec)) = self.allocate_record(&size, key, latest) else {
            return Op::RetryLater;
        };
        self.functions.initial_writer(key, input, new_rec, out);
        if !self.cas_into_chain(&slot, entry, new_addr, new_rec) {
            return Op::RetryNow;
        }
        if let Some(old) = old_to_seal {
            old.info().try_seal();
        }
        self.functions.post_initial_writer(key, input, new_rec, out);
        Op::Done(Status::Created)
    }

    // ---------------------------------------------------------------------
    // RMW (Modify)
    // ---------------------------------------------------------------------

    pub fn rmw(&mut self, key: &[u8], input: &F::Input, out: &mut F::Output) -> Status {
        let hash = hash64(key);
        let mut attempt = 0;
        // Source record fetched from disk for the copy-update path:
        // (image or None if confirmed absent, chain head observed before the IO).
        let mut disk_src: Option<(Option<AlignedBuf>, u64)> = None;
        loop {
            let src = disk_src
                .as_ref()
                .map(|(b, l)| (b.as_ref().map(|b| b.record()), *l));
            let r = self.protected(|s| s.internal_rmw(key, hash, input, out, src));
            match r {
                Op::Done(s) => return s,
                Op::RecordOnDisk { addr, latest } => {
                    // Pending IO (synchronous here): fetch the old value, then
                    // re-enter and copy-update. `latest` lets the re-entry
                    // detect a concurrent in-memory insert of the same key.
                    disk_src = Some((self.find_on_disk(addr, key), latest));
                }
                other => {
                    if !self.handle_retry("rmw", other, &mut attempt) {
                        return Status::Error;
                    }
                }
            }
        }
    }

    /// `disk_src`: (record image fetched from disk, or None if confirmed
    /// absent; chain head observed before the IO).
    fn internal_rmw(
        &mut self,
        key: &[u8],
        hash: u64,
        input: &F::Input,
        out: &mut F::Output,
        disk_src: Option<(Option<RecordPtr>, u64)>,
    ) -> Op {
        let store = Arc::clone(&self.store);
        let slot = store.index.find_or_create_tag(hash);
        let Some(_latch) = latch(&store.index, self.txn_mode, slot.bucket, true) else {
            return Op::RetryLater;
        };
        let entry = slot.load();
        let latest = entry.address();
        let head = store.log.head_address();
        let ro = store.log.read_only_address();
        let safe_ro = store.log.safe_read_only_address();

        match self.trace_back(latest, key, head) {
            Trace::Found(addr) => {
                let rec = store.log.record_at(addr);
                let info = rec.info().load();
                if let Some(op) = self.cpr_gate(addr, info) {
                    return op;
                }
                if info.is_tombstone() {
                    return self.rmw_initial(&slot, entry, key, input, out);
                }
                if addr >= ro {
                    if !self.cpr_blocks_in_place(addr, info) {
                        match self.functions.in_place_updater(key, input, rec, out) {
                            InPlaceResult::Updated => {
                                rec.info().set_modified();
                                return Op::Done(Status::InPlaceUpdated);
                            }
                            InPlaceResult::Cancel => return Op::Done(Status::Found),
                            InPlaceResult::Expired => {
                                rec.info().set_tombstone();
                                return self.rmw_initial(&slot, entry, key, input, out);
                            }
                            InPlaceResult::Delete => {
                                rec.info().set_tombstone();
                                self.functions.post_deleter(key);
                                return Op::Done(Status::Deleted);
                            }
                            InPlaceResult::NeedCopy => {}
                        }
                    }
                    return self.rmw_copy(&slot, entry, key, input, out, addr, rec, true);
                }
                if addr >= safe_ro {
                    // Fuzzy region: someone may still be updating it in place.
                    return Op::RetryLater;
                }
                // Immutable in memory.
                self.rmw_copy(&slot, entry, key, input, out, addr, rec, false)
            }
            Trace::Stopped(addr) => {
                let begin = store.log.begin_address();
                if addr >= begin && addr >= FIRST_VALID_ADDRESS {
                    // Chain continues on disk.
                    match disk_src {
                        Some((src, seen_latest))
                            if seen_latest == latest
                                || self
                                    .trace_back(latest, key, (seen_latest + 1).max(head))
                                    .is_stopped() =>
                        {
                            // No newer in-memory record for this key appeared since the IO.
                            match src {
                                Some(old) if !old.info().load().is_tombstone() => {
                                    self.rmw_copy(&slot, entry, key, input, out, 0, old, false)
                                }
                                _ => self.rmw_initial(&slot, entry, key, input, out),
                            }
                        }
                        Some(_) => Op::RetryNow, // a newer record appeared in memory: restart
                        None => Op::RecordOnDisk { addr, latest },
                    }
                } else {
                    self.rmw_initial(&slot, entry, key, input, out)
                }
            }
        }
    }

    fn rmw_initial(
        &mut self,
        slot: &SlotRef,
        entry: HashEntry,
        key: &[u8],
        input: &F::Input,
        out: &mut F::Output,
    ) -> Op {
        if !self.functions.need_initial_update(key, input, out) {
            return Op::Done(Status::NotFound);
        }
        let size = self.functions.initial_size(key, input);
        let Ok((new_addr, new_rec)) = self.allocate_record(&size, key, entry.address()) else {
            return Op::RetryLater;
        };
        self.functions.initial_updater(key, input, new_rec, out);
        if !self.cas_into_chain(slot, entry, new_addr, new_rec) {
            return Op::RetryNow;
        }
        self.functions
            .post_initial_updater(key, input, new_rec, out);
        Op::Done(Status::Created)
    }

    #[allow(clippy::too_many_arguments)]
    fn rmw_copy(
        &mut self,
        slot: &SlotRef,
        entry: HashEntry,
        key: &[u8],
        input: &F::Input,
        out: &mut F::Output,
        old_addr: u64,
        old: RecordPtr,
        seal_old: bool,
    ) -> Op {
        match self.functions.need_copy_update(key, input, old, out) {
            CopyDecision::Copy => {}
            CopyDecision::Cancel => return Op::Done(Status::Canceled),
            CopyDecision::Delete => {
                let seal = if seal_old && old_addr != 0 {
                    Some(old)
                } else {
                    None
                };
                return self.delete_rcu(slot, entry, key, seal);
            }
        }
        let size = self.functions.copy_size(key, input, old);
        let Ok((new_addr, new_rec)) = self.allocate_record(&size, key, entry.address()) else {
            return Op::RetryLater;
        };
        self.functions.copy_updater(key, input, old, new_rec, out);
        if !self.cas_into_chain(slot, entry, new_addr, new_rec) {
            return Op::RetryNow;
        }
        // Seal the mutable source so a racing in-place updater retries
        // rather than writing to the stale copy.
        if seal_old && old_addr != 0 {
            old.info().try_seal();
        }
        self.functions.post_copy_updater(key, input, new_rec, out);
        Op::Done(Status::CopyUpdated)
    }

    // ---------------------------------------------------------------------
    // Delete
    // ---------------------------------------------------------------------

    pub fn delete(&mut self, key: &[u8]) -> Status {
        let hash = hash64(key);
        let mut attempt = 0;
        let mut disk_checked: Option<(bool, u64)> = None; // (exists on disk, latest seen)
        loop {
            let r = self.protected(|s| s.internal_delete(key, hash, disk_checked));
            match r {
                Op::Done(s) => return s,
                Op::RecordOnDisk { addr, latest } => {
                    let exists = match self.find_on_disk(addr, key) {
                        Some(buf) => !buf.record().info().load().is_tombstone(),
                        None => false,
                    };
                    disk_checked = Some((exists, latest));
                }
                other => {
                    if !self.handle_retry("delete", other, &mut attempt) {
                        return Status::Error;
                    }
                }
            }
        }
    }

    fn internal_delete(&mut self, key: &[u8], hash: u64, disk_checked: Option<(bool, u64)>) -> Op {
        let store = Arc::clone(&self.store);
        let Some(slot) = store.index.find_tag(hash) else {
            return Op::Done(Status::NotFound);
        };
        let Some(_latch) = latch(&store.index, self.txn_mode, slot.bucket, true) else {
            return Op::RetryLater;
        };
        let entry = slot.load();
        let latest = entry.address();
        let head = store.log.head_address();
        let ro = store.log.read_only_address();
        match self.trace_back(latest, key, head) {
            Trace::Found(addr) => {
                let rec = store.log.record_at(addr);
                let info = rec.info().load();
                if let Some(op) = self.cpr_gate(addr, info) {
                    return op;
                }
                if info.is_tombstone() {
                    return Op::Done(Status::NotFound);
                }
                if addr >= ro && !self.cpr_blocks_in_place(addr, info) {
                    rec.info().set_tombstone();
                    rec.info().set_modified();
                    self.functions.post_deleter(key);
                    return Op::Done(Status::Deleted);
                }
                self.delete_rcu(&slot, entry, key, Some(rec))
            }
            Trace::Stopped(addr) => {
                let begin = store.log.begin_address();
                if addr >= begin && addr >= FIRST_VALID_ADDRESS {
                    match disk_checked {
                        Some((exists, seen_latest))
                            if seen_latest == latest
                                || self
                                    .trace_back(latest, key, (seen_latest + 1).max(head))
                                    .is_stopped() =>
                        {
                            if exists {
                                self.delete_rcu(&slot, entry, key, None)
                            } else {
                                Op::Done(Status::NotFound)
                            }
                        }
                        Some(_) => Op::RetryNow,
                        None => Op::RecordOnDisk { addr, latest },
                    }
                } else {
                    Op::Done(Status::NotFound)
                }
            }
        }
    }

    /// Append a tombstone record at the tail.
    fn delete_rcu(
        &mut self,
        slot: &SlotRef,
        entry: HashEntry,
        key: &[u8],
        old: Option<RecordPtr>,
    ) -> Op {
        let size = RecordSizeInfo {
            key_len: key.len(),
            value_cap: 0,
            has_expiration: false,
        };
        let Ok((new_addr, new_rec)) = self.allocate_record(&size, key, entry.address()) else {
            return Op::RetryLater;
        };
        new_rec.info().set_tombstone();
        if !self.cas_into_chain(slot, entry, new_addr, new_rec) {
            return Op::RetryNow;
        }
        if let Some(old) = old {
            old.info().try_seal();
        }
        self.functions.post_deleter(key);
        Op::Done(Status::Deleted)
    }

    // ---------------------------------------------------------------------
    // Transactions: two-phase locking on bucket latches (paper §4.6)
    // ---------------------------------------------------------------------

    /// Acquire bucket latches for all keys **in sorted bucket order** (the
    /// deadlock-freedom rule), then enter transaction mode so subsequent
    /// ops skip ephemeral latching. Blocks (with epoch refreshes) until all
    /// latches are held.
    pub fn txn_lock(&mut self, keys: &[(&[u8], LockType)]) {
        assert!(!self.txn_mode, "already in a transaction");
        let mut wanted: Vec<(usize, LockType)> = keys
            .iter()
            .map(|(k, t)| (self.store.index.bucket_index(hash64(k)), *t))
            .collect();
        wanted.sort_by_key(|(b, t)| (*b, if *t == LockType::Exclusive { 0 } else { 1 }));
        wanted.dedup_by_key(|(b, _)| *b); // exclusive wins (sorted first)
        for (bucket, ty) in wanted {
            loop {
                let ok = match ty {
                    LockType::Exclusive => self.store.index.try_lock_exclusive(bucket),
                    LockType::Shared => self.store.index.try_lock_shared(bucket),
                };
                if ok {
                    break;
                }
                std::thread::yield_now();
            }
            self.txn_locks.push((bucket, ty));
        }
        self.txn_mode = true;
    }

    pub fn txn_unlock(&mut self) {
        for (bucket, ty) in self.txn_locks.drain(..) {
            match ty {
                LockType::Exclusive => self.store.index.unlock_exclusive(bucket),
                LockType::Shared => self.store.index.unlock_shared(bucket),
            }
        }
        self.txn_mode = false;
    }

    pub fn in_transaction(&self) -> bool {
        self.txn_mode
    }
}

/// Releasing the epoch-table slot is engine bookkeeping, so it belongs to the
/// session's lifetime rather than to whoever remembers to call a cleanup
/// method. The table has a fixed number of slots; a leaked one is a slot no
/// future thread can claim.
impl<F: SessionFunctions> Drop for Session<F> {
    fn drop(&mut self) {
        if self.txn_mode {
            self.txn_unlock();
        }
        self.store.epoch.release_thread();
    }
}

impl Trace {
    fn is_stopped(&self) -> bool {
        matches!(self, Trace::Stopped(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemDevice;
    use std::sync::atomic::AtomicUsize;

    /// Minimal "string store" functions: values are byte strings; input is a
    /// command. Mirrors the shape of Garnet's MainSessionFunctions.
    #[derive(Clone, Copy)]
    enum Cmd<'a> {
        Set(&'a [u8]),
        Incr(i64),
        Append(&'a [u8]),
        SetNx(&'a [u8]),
        SetXx(&'a [u8]),
    }

    struct TestFns {
        posts: AtomicUsize,
    }

    fn parse_i64(b: &[u8]) -> i64 {
        std::str::from_utf8(b).unwrap().parse().unwrap()
    }

    impl SessionFunctions for TestFns {
        type Input = Cmd<'static>;
        type Output = Vec<u8>;

        fn reader(&mut self, _key: &[u8], _input: &Cmd, rec: RecordPtr, out: &mut Vec<u8>) -> bool {
            out.clear();
            out.extend_from_slice(rec.value());
            true
        }
        fn upsert_size(&mut self, key: &[u8], input: &Cmd) -> RecordSizeInfo {
            let v = match input {
                Cmd::Set(v) => v.len(),
                _ => unreachable!(),
            };
            RecordSizeInfo {
                key_len: key.len(),
                value_cap: v,
                has_expiration: false,
            }
        }
        fn initial_writer(&mut self, _key: &[u8], input: &Cmd, rec: RecordPtr, _out: &mut Vec<u8>) {
            let Cmd::Set(v) = input else { unreachable!() };
            rec.value_buf_mut()[..v.len()].copy_from_slice(v);
            rec.set_value_len(v.len());
        }
        fn post_initial_writer(&mut self, _k: &[u8], _i: &Cmd, _r: RecordPtr, _o: &mut Vec<u8>) {
            self.posts.fetch_add(1, Ordering::Relaxed);
        }
        fn in_place_writer(
            &mut self,
            _key: &[u8],
            input: &Cmd,
            rec: RecordPtr,
            _out: &mut Vec<u8>,
        ) -> bool {
            let Cmd::Set(v) = input else { unreachable!() };
            if v.len() > rec.value_cap() {
                return false;
            }
            rec.value_buf_mut()[..v.len()].copy_from_slice(v);
            rec.set_value_len(v.len());
            self.posts.fetch_add(1, Ordering::Relaxed);
            true
        }
        fn need_initial_update(&mut self, _key: &[u8], input: &Cmd, _out: &mut Vec<u8>) -> bool {
            !matches!(input, Cmd::SetXx(_))
        }
        fn initial_size(&mut self, key: &[u8], input: &Cmd) -> RecordSizeInfo {
            let cap = match input {
                Cmd::Incr(d) => d.to_string().len(),
                Cmd::Append(v) | Cmd::SetNx(v) | Cmd::SetXx(v) | Cmd::Set(v) => v.len(),
            };
            RecordSizeInfo {
                key_len: key.len(),
                value_cap: cap,
                has_expiration: false,
            }
        }
        fn initial_updater(&mut self, _key: &[u8], input: &Cmd, rec: RecordPtr, out: &mut Vec<u8>) {
            let v: Vec<u8> = match input {
                Cmd::Incr(d) => d.to_string().into_bytes(),
                Cmd::Append(v) | Cmd::SetNx(v) | Cmd::SetXx(v) | Cmd::Set(v) => v.to_vec(),
            };
            rec.value_buf_mut()[..v.len()].copy_from_slice(&v);
            rec.set_value_len(v.len());
            out.clear();
            out.extend_from_slice(&v);
        }
        fn post_initial_updater(&mut self, _k: &[u8], _i: &Cmd, _r: RecordPtr, _o: &mut Vec<u8>) {
            self.posts.fetch_add(1, Ordering::Relaxed);
        }
        fn in_place_updater(
            &mut self,
            _key: &[u8],
            input: &Cmd,
            rec: RecordPtr,
            out: &mut Vec<u8>,
        ) -> InPlaceResult {
            match input {
                Cmd::SetNx(_) => InPlaceResult::Cancel,
                Cmd::Incr(d) => {
                    let n = parse_i64(rec.value()) + d;
                    let s = n.to_string();
                    if s.len() > rec.value_cap() {
                        return InPlaceResult::NeedCopy;
                    }
                    rec.value_buf_mut()[..s.len()].copy_from_slice(s.as_bytes());
                    rec.set_value_len(s.len());
                    out.clear();
                    out.extend_from_slice(s.as_bytes());
                    self.posts.fetch_add(1, Ordering::Relaxed);
                    InPlaceResult::Updated
                }
                Cmd::Append(v) => {
                    let n = rec.value_len() + v.len();
                    if n > rec.value_cap() {
                        return InPlaceResult::NeedCopy;
                    }
                    let start = rec.value_len();
                    rec.value_buf_mut()[start..n].copy_from_slice(v);
                    rec.set_value_len(n);
                    self.posts.fetch_add(1, Ordering::Relaxed);
                    InPlaceResult::Updated
                }
                Cmd::SetXx(v) | Cmd::Set(v) => {
                    if v.len() > rec.value_cap() {
                        return InPlaceResult::NeedCopy;
                    }
                    rec.value_buf_mut()[..v.len()].copy_from_slice(v);
                    rec.set_value_len(v.len());
                    self.posts.fetch_add(1, Ordering::Relaxed);
                    InPlaceResult::Updated
                }
            }
        }
        fn need_copy_update(
            &mut self,
            _key: &[u8],
            input: &Cmd,
            _old: RecordPtr,
            _out: &mut Vec<u8>,
        ) -> CopyDecision {
            if matches!(input, Cmd::SetNx(_)) {
                CopyDecision::Cancel
            } else {
                CopyDecision::Copy
            }
        }
        fn copy_size(&mut self, key: &[u8], input: &Cmd, old: RecordPtr) -> RecordSizeInfo {
            let cap = match input {
                Cmd::Incr(d) => (parse_i64(old.value()) + d).to_string().len(),
                Cmd::Append(v) => old.value_len() + v.len(),
                Cmd::SetNx(v) | Cmd::SetXx(v) | Cmd::Set(v) => v.len(),
            };
            RecordSizeInfo {
                key_len: key.len(),
                value_cap: cap,
                has_expiration: false,
            }
        }
        fn copy_updater(
            &mut self,
            _key: &[u8],
            input: &Cmd,
            old: RecordPtr,
            new: RecordPtr,
            out: &mut Vec<u8>,
        ) {
            let v: Vec<u8> = match input {
                Cmd::Incr(d) => (parse_i64(old.value()) + d).to_string().into_bytes(),
                Cmd::Append(a) => {
                    let mut v = old.value().to_vec();
                    v.extend_from_slice(a);
                    v
                }
                Cmd::SetNx(v) | Cmd::SetXx(v) | Cmd::Set(v) => v.to_vec(),
            };
            new.value_buf_mut()[..v.len()].copy_from_slice(&v);
            new.set_value_len(v.len());
            out.clear();
            out.extend_from_slice(&v);
        }
        fn post_copy_updater(&mut self, _k: &[u8], _i: &Cmd, _r: RecordPtr, _o: &mut Vec<u8>) {
            self.posts.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn small_store() -> Arc<Store> {
        Store::new(
            StoreSettings {
                index_buckets: 64,
                log: LogSettings {
                    page_bits: 10,
                    memory_pages: 4,
                    mutable_fraction: 0.5,
                },
            },
            Box::new(MemDevice::new()),
        )
    }

    fn fns() -> TestFns {
        TestFns {
            posts: AtomicUsize::new(0),
        }
    }

    #[test]
    fn upsert_read_rmw_delete_in_memory() {
        let store = small_store();
        let mut s = store.new_session(fns());
        let mut out = Vec::new();
        assert_eq!(s.read(b"a", &Cmd::Set(b""), &mut out), Status::NotFound);
        assert_eq!(
            s.upsert(b"a", &Cmd::Set(b"hello"), &mut out),
            Status::Created
        );
        assert_eq!(s.read(b"a", &Cmd::Set(b""), &mut out), Status::Found);
        assert_eq!(out, b"hello");
        // In-place overwrite (fits).
        assert_eq!(
            s.upsert(b"a", &Cmd::Set(b"hi"), &mut out),
            Status::InPlaceUpdated
        );
        // Does not fit -> new record at tail.
        assert_eq!(
            s.upsert(b"a", &Cmd::Set(b"a much longer value"), &mut out),
            Status::Created
        );
        assert_eq!(s.read(b"a", &Cmd::Set(b""), &mut out), Status::Found);
        assert_eq!(out, b"a much longer value");

        // RMW: INCR creates, then in-place, then copy when digits grow.
        assert_eq!(s.rmw(b"n", &Cmd::Incr(5), &mut out), Status::Created);
        assert_eq!(out, b"5");
        assert_eq!(s.rmw(b"n", &Cmd::Incr(4), &mut out), Status::InPlaceUpdated);
        assert_eq!(out, b"9");
        assert_eq!(s.rmw(b"n", &Cmd::Incr(1), &mut out), Status::CopyUpdated);
        assert_eq!(out, b"10");
        assert_eq!(s.read(b"n", &Cmd::Set(b""), &mut out), Status::Found);
        assert_eq!(out, b"10");

        // SETNX / SETXX semantics via need_* callbacks.
        assert_eq!(s.rmw(b"n", &Cmd::SetNx(b"x"), &mut out), Status::Found);
        assert_eq!(s.rmw(b"zz", &Cmd::SetXx(b"x"), &mut out), Status::NotFound);
        assert_eq!(s.rmw(b"zz", &Cmd::SetNx(b"x"), &mut out), Status::Created);

        // Delete.
        assert_eq!(s.delete(b"n"), Status::Deleted);
        assert_eq!(s.delete(b"n"), Status::NotFound);
        assert_eq!(s.read(b"n", &Cmd::Set(b""), &mut out), Status::NotFound);
        // Re-create after delete.
        assert_eq!(s.rmw(b"n", &Cmd::Incr(1), &mut out), Status::Created);
        assert_eq!(out, b"1");
        assert_eq!(store.count_live(), 3); // a, zz, n
        assert_eq!(
            s.functions.posts.load(Ordering::Relaxed),
            8,
            "one post-mutation hook per successful write"
        );
    }

    #[test]
    fn records_evicted_to_disk_are_still_readable_and_updatable() {
        let store = small_store();
        let mut s = store.new_session(fns());
        let mut out = Vec::new();
        // 4 pages x 1KB in memory; write far more than that.
        let n = 2000u32;
        for i in 0..n {
            let k = format!("key{i}");
            assert_eq!(
                s.upsert(
                    k.as_bytes(),
                    &Cmd::Set(format!("val{i}").leak().as_bytes()),
                    &mut out
                ),
                Status::Created
            );
        }
        store.epoch.drain_all_blocking();
        assert!(store.log.head_address() > FIRST_VALID_ADDRESS);
        // Early keys are on disk now.
        for i in (0..n).step_by(97) {
            let k = format!("key{i}");
            assert_eq!(
                s.read(k.as_bytes(), &Cmd::Set(b""), &mut out),
                Status::Found,
                "key{i}"
            );
            assert_eq!(out, format!("val{i}").as_bytes());
        }
        // RMW on a key that lives on disk: read-copy-update to the tail.
        assert_eq!(
            s.rmw(b"key0", &Cmd::Append(b"+more"), &mut out),
            Status::CopyUpdated
        );
        assert_eq!(s.read(b"key0", &Cmd::Set(b""), &mut out), Status::Found);
        assert_eq!(out, b"val0+more");
        // Delete a key on disk.
        assert_eq!(s.delete(b"key1"), Status::Deleted);
        assert_eq!(s.read(b"key1", &Cmd::Set(b""), &mut out), Status::NotFound);
        assert_eq!(s.delete(b"key1"), Status::NotFound);
        assert_eq!(
            s.rmw(b"nope-on-disk", &Cmd::Incr(1), &mut out),
            Status::Created
        );
        assert_eq!(store.count_live(), n as usize);
    }

    #[test]
    fn concurrent_incr_is_atomic_and_never_loses_updates() {
        let store = Store::new(
            StoreSettings {
                index_buckets: 16,
                log: LogSettings {
                    page_bits: 12,
                    memory_pages: 8,
                    mutable_fraction: 0.5,
                },
            },
            Box::new(MemDevice::new()),
        );
        let threads = 8;
        let per = 2000;
        let keys = 5;
        let mut hs = Vec::new();
        for t in 0..threads {
            let store = store.clone();
            hs.push(std::thread::spawn(move || {
                let mut s = store.new_session(fns());
                let mut out = Vec::new();
                for i in 0..per {
                    let k = format!("counter{}", (i + t) % keys);
                    let st = s.rmw(k.as_bytes(), &Cmd::Incr(1), &mut out);
                    assert!(st.is_updated(), "{st:?}");
                }
                store.epoch.release_thread();
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let mut s = store.new_session(fns());
        let mut out = Vec::new();
        let mut total = 0;
        for k in 0..keys {
            assert_eq!(
                s.read(format!("counter{k}").as_bytes(), &Cmd::Set(b""), &mut out),
                Status::Found
            );
            total += parse_i64(&out);
        }
        assert_eq!(total, (threads * per) as i64);
    }

    #[test]
    fn transaction_locks_are_sorted_and_exclusive() {
        let store = small_store();
        let mut s = store.new_session(fns());
        let mut out = Vec::new();
        s.upsert(b"src", &Cmd::Set(b"10"), &mut out);
        s.upsert(b"dst", &Cmd::Set(b"0"), &mut out);
        s.txn_lock(&[(b"dst", LockType::Exclusive), (b"src", LockType::Exclusive)]);
        assert!(s.in_transaction());
        // Another session cannot get the latch while the txn holds it: its
        // RMW keeps retrying until we unlock.
        let b = store.index.bucket_index(hash64(b"src"));
        assert!(store.index.is_locked_exclusive(b));
        let done = Arc::new(AtomicUsize::new(0));
        let waiter = {
            let store = store.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let mut other = store.new_session(fns());
                let mut out = Vec::new();
                let st = other.rmw(b"src", &Cmd::Incr(1), &mut out);
                done.store(1, Ordering::SeqCst);
                store.epoch.release_thread();
                st
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(
            done.load(Ordering::SeqCst),
            0,
            "must be blocked by the txn latch"
        );
        s.rmw(b"src", &Cmd::Incr(-3), &mut out);
        s.rmw(b"dst", &Cmd::Incr(3), &mut out);
        s.txn_unlock();
        assert!(!s.in_transaction());
        assert!(waiter.join().unwrap().is_updated());
        assert_eq!(s.read(b"src", &Cmd::Set(b""), &mut out), Status::Found);
        assert_eq!(out, b"8");
        assert_eq!(s.read(b"dst", &Cmd::Set(b""), &mut out), Status::Found);
        assert_eq!(out, b"3");
    }
}
