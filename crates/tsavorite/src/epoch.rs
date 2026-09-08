//! Epoch protection (Tsavorite's `LightEpoch`).
//!
//! The store is "shared-everything": many session threads read and write the
//! same index and log pages. Rather than locking, structural changes that would
//! invalidate memory another thread may still be looking at (evicting a page,
//! freeing an old hash table after a resize, reusing a record slot) are
//! **deferred** until every thread that could have observed the old state has
//! moved on. That is what epochs give us:
//!
//! * A global `current_epoch` counter `E`.
//! * Every thread that touches the store first *protects* itself by publishing
//!   `E` into its own cache-line-padded slot of the epoch table, and *unprotects*
//!   (publishes 0) when its operation finishes. Garnet does this once per
//!   network batch, not per command.
//! * `safe_to_reclaim_epoch` = (min over all protected threads' published
//!   epochs) - 1. Anything retired in epoch `e <= safe` cannot be referenced by
//!   any in-flight operation.
//! * `bump_current_epoch(action)` increments `E` and enqueues `action` on the
//!   drain list tagged with the *previous* `E`. Actions run (from whichever
//!   thread next calls `refresh`/`bump`) once `E_prev <= safe_to_reclaim`.
//!
//! This is also the substrate for the paper's *epoch-protected state machine*
//! (EPSM): a phase change = bump epoch; the next phase's actions run only after
//! all threads have observed the new phase.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Maximum number of concurrently protected threads.
pub const MAX_THREADS: usize = 128;

const UNPROTECTED: u64 = 0;

#[repr(align(64))]
struct Slot {
    /// 0 = unprotected; otherwise the epoch this thread is operating in.
    epoch: AtomicU64,
    /// Thread ownership marker (0 = free).
    owner: AtomicU64,
}

struct DrainAction {
    epoch: u64,
    action: Box<dyn FnOnce() + Send>,
}

pub struct LightEpoch {
    current: AtomicU64,
    table: Box<[Slot]>,
    drain: Mutex<Vec<DrainAction>>,
    drain_count: AtomicUsize,
}

thread_local! {
    /// Index into the epoch table for this thread (usize::MAX = none yet).
    static MY_SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
    static MY_ID: Cell<u64> = const { Cell::new(0) };
}

static NEXT_THREAD_ID: AtomicU64 = AtomicU64::new(1);

impl Default for LightEpoch {
    fn default() -> Self {
        Self::new()
    }
}

impl LightEpoch {
    pub fn new() -> Self {
        let table: Vec<Slot> = (0..MAX_THREADS)
            .map(|_| Slot {
                epoch: AtomicU64::new(UNPROTECTED),
                owner: AtomicU64::new(0),
            })
            .collect();
        LightEpoch {
            current: AtomicU64::new(1),
            table: table.into_boxed_slice(),
            drain: Mutex::new(Vec::new()),
            drain_count: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn current_epoch(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    #[inline]
    fn thread_id() -> u64 {
        MY_ID.with(|id| {
            if id.get() == 0 {
                id.set(NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed));
            }
            id.get()
        })
    }

    /// Find (or lazily claim) this thread's slot in the table.
    fn my_slot(&self) -> usize {
        let cached = MY_SLOT.with(|s| s.get());
        let tid = Self::thread_id();
        if cached != usize::MAX && self.table[cached].owner.load(Ordering::Relaxed) == tid {
            return cached;
        }
        // Claim a free slot, starting at a hash of the thread id to spread load.
        let start = (tid as usize).wrapping_mul(0x9E37_79B9) % MAX_THREADS;
        for i in 0..MAX_THREADS {
            let idx = (start + i) % MAX_THREADS;
            if self.table[idx]
                .owner
                .compare_exchange(0, tid, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                MY_SLOT.with(|s| s.set(idx));
                return idx;
            }
        }
        panic!("epoch table full: more than {MAX_THREADS} concurrent threads");
    }

    #[inline]
    pub fn is_protected(&self) -> bool {
        let idx = MY_SLOT.with(|s| s.get());
        idx != usize::MAX
            && self.table[idx].owner.load(Ordering::Relaxed) == Self::thread_id()
            && self.table[idx].epoch.load(Ordering::Relaxed) != UNPROTECTED
    }

    /// Enter the current epoch. Returns the epoch entered.
    #[inline]
    pub fn protect(&self) -> u64 {
        let idx = self.my_slot();
        let e = self.current.load(Ordering::Acquire);
        // SeqCst store so that the reader of the table (compute_safe) cannot
        // miss our publication relative to our subsequent loads of shared state.
        self.table[idx].epoch.store(e, Ordering::SeqCst);
        e
    }

    /// Re-read the current epoch (call periodically inside long operations so
    /// that the thread does not hold back reclamation), and drain any actions
    /// that have become safe.
    #[inline]
    pub fn refresh(&self) -> u64 {
        let idx = self.my_slot();
        let e = self.current.load(Ordering::Acquire);
        self.table[idx].epoch.store(e, Ordering::SeqCst);
        if self.drain_count.load(Ordering::Acquire) > 0 {
            self.drain();
        }
        e
    }

    /// Leave the epoch.
    #[inline]
    pub fn unprotect(&self) {
        let idx = self.my_slot();
        self.table[idx].epoch.store(UNPROTECTED, Ordering::Release);
    }

    /// Release this thread's slot entirely (e.g. when a session thread exits).
    pub fn release_thread(&self) {
        let idx = MY_SLOT.with(|s| s.get());
        if idx != usize::MAX && self.table[idx].owner.load(Ordering::Relaxed) == Self::thread_id() {
            self.table[idx].epoch.store(UNPROTECTED, Ordering::Release);
            self.table[idx].owner.store(0, Ordering::Release);
            MY_SLOT.with(|s| s.set(usize::MAX));
        }
    }

    /// (min protected epoch) - 1, or current - 1 if no thread is protected.
    pub fn compute_safe_epoch(&self) -> u64 {
        let current = self.current.load(Ordering::SeqCst);
        let mut min = current;
        for slot in self.table.iter() {
            let e = slot.epoch.load(Ordering::SeqCst);
            if e != UNPROTECTED && e < min {
                min = e;
            }
        }
        min - 1
    }

    /// Increment the global epoch. Returns the new epoch.
    pub fn bump_current_epoch(&self) -> u64 {
        let new = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        if self.drain_count.load(Ordering::Acquire) > 0 {
            self.drain();
        }
        new
    }

    /// Increment the global epoch and register `action` to run once every
    /// thread has left the *previous* epoch (i.e. no thread can still observe
    /// the state being retired).
    pub fn bump_with_action<F: FnOnce() + Send + 'static>(&self, action: F) -> u64 {
        let prior = self.current.load(Ordering::SeqCst);
        {
            let mut d = self.drain.lock().unwrap();
            d.push(DrainAction {
                epoch: prior,
                action: Box::new(action),
            });
            self.drain_count.store(d.len(), Ordering::Release);
        }
        let new = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.drain();
        new
    }

    /// Run all drain actions whose epoch <= safe-to-reclaim.
    fn drain(&self) {
        let safe = self.compute_safe_epoch();
        let ready: Vec<DrainAction> = {
            let Ok(mut d) = self.drain.try_lock() else {
                return;
            };
            if d.is_empty() {
                return;
            }
            let (ready, pending): (Vec<_>, Vec<_>) = d.drain(..).partition(|a| a.epoch <= safe);
            *d = pending;
            self.drain_count.store(d.len(), Ordering::Release);
            ready
        };
        for a in ready {
            (a.action)();
        }
    }

    /// Block until every action registered so far has run. Callers must not be
    /// protected (otherwise they would hold back their own reclamation).
    pub fn drain_all_blocking(&self) {
        debug_assert!(!self.is_protected());
        while self.drain_count.load(Ordering::Acquire) > 0 {
            self.bump_current_epoch();
            std::thread::yield_now();
        }
    }
}

/// RAII guard: protect on construction, unprotect on drop.
pub struct EpochGuard<'a> {
    epoch: &'a LightEpoch,
}

impl<'a> EpochGuard<'a> {
    #[inline]
    pub fn new(epoch: &'a LightEpoch) -> Self {
        epoch.protect();
        EpochGuard { epoch }
    }
    #[inline]
    pub fn refresh(&self) {
        self.epoch.refresh();
    }
}

impl Drop for EpochGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.epoch.unprotect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn action_runs_only_after_protected_thread_leaves() {
        let epoch = Arc::new(LightEpoch::new());
        let ran = Arc::new(AtomicBool::new(false));

        // Thread A enters an epoch and holds it.
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let (ack_tx, ack_rx) = std::sync::mpsc::channel::<()>();
        let e2 = epoch.clone();
        let a = std::thread::spawn(move || {
            e2.protect();
            ack_tx.send(()).unwrap();
            rx.recv().unwrap(); // wait until told to leave
            e2.unprotect();
            e2.release_thread();
        });
        ack_rx.recv().unwrap();

        // Main thread retires something.
        let r = ran.clone();
        epoch.bump_with_action(move || r.store(true, Ordering::SeqCst));
        // A still protected in the old epoch => must not have run.
        for _ in 0..10 {
            epoch.bump_current_epoch();
        }
        assert!(!ran.load(Ordering::SeqCst));

        tx.send(()).unwrap();
        a.join().unwrap();
        // Now safe: the next bump/refresh drains it.
        epoch.drain_all_blocking();
        assert!(ran.load(Ordering::SeqCst));
    }

    #[test]
    fn guard_protects_and_unprotects() {
        let epoch = LightEpoch::new();
        assert!(!epoch.is_protected());
        {
            let _g = EpochGuard::new(&epoch);
            assert!(epoch.is_protected());
            let safe = epoch.compute_safe_epoch();
            assert_eq!(safe, epoch.current_epoch() - 1);
        }
        assert!(!epoch.is_protected());
    }

    #[test]
    fn many_threads_protect_concurrently() {
        let epoch = Arc::new(LightEpoch::new());
        let counter = Arc::new(AtomicU64::new(0));
        let mut hs = Vec::new();
        for _ in 0..16 {
            let e = epoch.clone();
            let c = counter.clone();
            hs.push(std::thread::spawn(move || {
                for _ in 0..200 {
                    let _g = EpochGuard::new(&e);
                    e.refresh();
                    let c2 = c.clone();
                    e.bump_with_action(move || {
                        c2.fetch_add(1, Ordering::SeqCst);
                    });
                    std::thread::sleep(Duration::from_micros(1));
                }
                e.release_thread();
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        epoch.drain_all_blocking();
        assert_eq!(counter.load(Ordering::SeqCst), 16 * 200);
    }
}
