//! Latch-free hash index (`HashBucket.cs`, `HashBucketEntry.cs`,
//! `TsavoriteBase.cs::FindTag/FindOrCreateTag`).
//!
//! * A bucket is one 64-byte cache line = 8 × 64-bit entries.
//! * Entries 0..6 hold `[tentative:1][tag:15][address:48]`.
//!   The **bucket index** comes from the *low* bits of the 64-bit key hash
//!   and the **tag** from the *high* 15 bits, so the tag is an independent
//!   discriminator within the bucket. Every record whose hash maps to the
//!   same (bucket, tag) lives on the reverse-linked chain rooted at `address`.
//! * Entry 7 is special: low 48 bits = address of an overflow bucket (0 = none),
//!   high 16 bits = the **bucket latch** (15-bit shared count + exclusive bit).
//!   Putting the lock next to the entries means an operation that just did the
//!   hash lookup takes the lock without another cache miss. The latch lives in
//!   the *first* bucket of a chain even when the entry is in an overflow bucket.
//!
//! Invariant: at most one non-tentative entry per (chain, tag). Established
//! by the tentative-bit protocol in [`HashIndex::find_or_create_tag`].
//!
//! Latches use *bounded spinning*: `try_lock_*` returns `false` instead of
//! blocking, and the store turns that into `RETRY_LATER`, which refreshes the
//! thread's epoch. That rule is what keeps a spinning thread from stalling
//! epoch-based reclamation (and thus page flush) for everyone else.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::record::{ADDRESS_MASK, TEMP_INVALID_ADDRESS};

pub const ENTRIES_PER_BUCKET: usize = 7;
pub const OVERFLOW_ENTRY: usize = 7;

pub const TAG_BITS: u32 = 15;
pub const TAG_SHIFT: u32 = 48;
pub const TAG_MASK: u64 = ((1u64 << TAG_BITS) - 1) << TAG_SHIFT;
pub const TENTATIVE_BIT: u64 = 1 << 63;

// Latch bits in entry 7.
const SHARED_ONE: u64 = 1 << 48;
const SHARED_MASK: u64 = ((1u64 << 15) - 1) << 48;
const EXCLUSIVE_BIT: u64 = 1 << 63;

const MAX_LOCK_SPINS: usize = 10;
const MAX_READER_DRAIN_SPINS: usize = 100;

#[repr(C, align(64))]
pub struct Bucket {
    pub entries: [AtomicU64; 8],
}

impl Bucket {
    fn new() -> Self {
        Bucket {
            entries: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Decoded entry word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HashEntry(pub u64);

impl HashEntry {
    #[inline]
    pub fn make(tag: u64, address: u64, tentative: bool) -> Self {
        let mut w = (tag << TAG_SHIFT) & TAG_MASK | (address & ADDRESS_MASK);
        if tentative {
            w |= TENTATIVE_BIT;
        }
        HashEntry(w)
    }
    #[inline]
    pub fn address(self) -> u64 {
        self.0 & ADDRESS_MASK
    }
    #[inline]
    pub fn tag(self) -> u64 {
        (self.0 & TAG_MASK) >> TAG_SHIFT
    }
    #[inline]
    pub fn is_tentative(self) -> bool {
        self.0 & TENTATIVE_BIT != 0
    }
    #[inline]
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// A located slot: raw pointer to the entry word plus the main-table bucket
/// index (for latching). Only valid while the index is alive.
#[derive(Clone, Copy, Debug)]
pub struct SlotRef {
    entry: *const AtomicU64,
    pub bucket: usize,
    pub tag: u64,
}

unsafe impl Send for SlotRef {}
unsafe impl Sync for SlotRef {}

impl SlotRef {
    #[inline]
    fn slot(&self) -> &AtomicU64 {
        unsafe { &*self.entry }
    }
    #[inline]
    pub fn load(&self) -> HashEntry {
        HashEntry(self.slot().load(Ordering::Acquire))
    }
    /// Install `new_address` for this tag if the entry still equals `expected`.
    #[inline]
    pub fn try_cas(&self, expected: HashEntry, new_address: u64) -> bool {
        let new = HashEntry::make(self.tag, new_address, false);
        self.slot()
            .compare_exchange(expected.0, new.0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    /// Remove the entry entirely (elision) if it still equals `expected`.
    #[inline]
    pub fn try_elide(&self, expected: HashEntry) -> bool {
        self.slot()
            .compare_exchange(expected.0, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

pub struct HashIndex {
    buckets: Box<[Bucket]>,
    mask: u64,
    overflow: Box<[Bucket]>,
    overflow_next: AtomicU64,
}

impl HashIndex {
    /// `size` buckets (rounded up to a power of two). Capacity ≈ 7 × size
    /// entries in the main table plus an equal-sized overflow arena.
    pub fn new(size: usize) -> Self {
        let size = size.max(2).next_power_of_two();
        let buckets: Vec<Bucket> = (0..size).map(|_| Bucket::new()).collect();
        let overflow_len = (size * 2).max(1024);
        let overflow: Vec<Bucket> = (0..overflow_len).map(|_| Bucket::new()).collect();
        HashIndex {
            buckets: buckets.into_boxed_slice(),
            mask: (size - 1) as u64,
            overflow: overflow.into_boxed_slice(),
            overflow_next: AtomicU64::new(1), // 0 = no overflow bucket
        }
    }

    #[inline]
    pub fn bucket_index(&self, hash: u64) -> usize {
        (hash & self.mask) as usize
    }
    #[inline]
    pub fn tag_of(hash: u64) -> u64 {
        hash >> (64 - TAG_BITS)
    }

    #[inline]
    fn overflow_bucket(&self, addr: u64) -> &Bucket {
        &self.overflow[(addr - 1) as usize]
    }

    fn alloc_overflow_bucket(&self) -> u64 {
        let a = self.overflow_next.fetch_add(1, Ordering::AcqRel);
        assert!(
            (a as usize) <= self.overflow.len(),
            "hash index overflow arena exhausted"
        );
        a
    }

    /// Walk the bucket chain for `bi`, following overflow pointers. The
    /// overflow pointer shares its word with the bucket latch, so the address
    /// mask lives here and nowhere else.
    #[inline]
    fn walk_chain(&self, bi: usize, mut visit: impl FnMut(&Bucket) -> bool) {
        let mut b: &Bucket = &self.buckets[bi];
        loop {
            if visit(b) {
                return;
            }
            let next = b.entries[OVERFLOW_ENTRY].load(Ordering::Acquire) & ADDRESS_MASK;
            if next == 0 {
                return;
            }
            b = self.overflow_bucket(next);
        }
    }

    /// Find the non-tentative entry for `hash`'s tag, following overflow buckets.
    pub fn find_tag(&self, hash: u64) -> Option<SlotRef> {
        let bi = self.bucket_index(hash);
        let tag = Self::tag_of(hash);
        let mut found = None;
        self.walk_chain(bi, |b| {
            for i in 0..ENTRIES_PER_BUCKET {
                let e = HashEntry(b.entries[i].load(Ordering::Acquire));
                if !e.is_empty() && e.tag() == tag && !e.is_tentative() {
                    found = Some(SlotRef {
                        entry: &b.entries[i],
                        bucket: bi,
                        tag,
                    });
                    return true;
                }
            }
            false
        });
        found
    }

    /// Scan the chain for a matching tag or the first free slot; allocates an
    /// overflow bucket if the chain is full.
    fn find_tag_or_free(&self, bi: usize, tag: u64) -> Result<SlotRef, SlotRef> {
        let mut b: &Bucket = &self.buckets[bi];
        let mut free: Option<SlotRef> = None;
        loop {
            for i in 0..ENTRIES_PER_BUCKET {
                let e = HashEntry(b.entries[i].load(Ordering::Acquire));
                if e.is_empty() {
                    if free.is_none() {
                        free = Some(SlotRef {
                            entry: &b.entries[i],
                            bucket: bi,
                            tag,
                        });
                    }
                } else if e.tag() == tag && !e.is_tentative() {
                    return Ok(SlotRef {
                        entry: &b.entries[i],
                        bucket: bi,
                        tag,
                    });
                }
            }
            let w = b.entries[OVERFLOW_ENTRY].load(Ordering::Acquire);
            let next = w & ADDRESS_MASK;
            if next != 0 {
                b = self.overflow_bucket(next);
                continue;
            }
            if let Some(f) = free {
                return Err(f);
            }
            // Chain full: link a new overflow bucket (preserve latch bits in
            // the high 16 bits of entry 7).
            let new_addr = self.alloc_overflow_bucket();
            let mut cur = w;
            loop {
                if cur & ADDRESS_MASK != 0 {
                    // Someone else linked one; use theirs (ours is leaked; fine for a mini).
                    break;
                }
                let n = (cur & !ADDRESS_MASK) | new_addr;
                match b.entries[OVERFLOW_ENTRY].compare_exchange(
                    cur,
                    n,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
            // loop continues into whichever overflow bucket is now linked
        }
    }

    /// Is there an entry with this tag (tentative or not) other than `mine`?
    fn other_slot_with_tag(&self, bi: usize, tag: u64, mine: &SlotRef) -> bool {
        let mut other = false;
        self.walk_chain(bi, |b| {
            for i in 0..ENTRIES_PER_BUCKET {
                if std::ptr::eq(&b.entries[i] as *const AtomicU64, mine.entry) {
                    continue;
                }
                let e = HashEntry(b.entries[i].load(Ordering::Acquire));
                if !e.is_empty() && e.tag() == tag {
                    other = true;
                    return true;
                }
            }
            false
        });
        other
    }

    /// Find the entry for this tag, or claim a new one using the two-phase
    /// tentative protocol:
    ///
    /// 1. CAS `{tentative, tag, TEMP_INVALID}` into a free slot.
    /// 2. Re-scan the whole chain for *any other* entry with the same tag.
    ///    If one exists, we lost the race: clear our slot and restart.
    /// 3. Otherwise clear the tentative bit (plain store; we own the slot).
    ///
    /// Readers ignore tentative entries, so a loser is never observed.
    pub fn find_or_create_tag(&self, hash: u64) -> SlotRef {
        let bi = self.bucket_index(hash);
        let tag = Self::tag_of(hash);
        loop {
            let free = match self.find_tag_or_free(bi, tag) {
                Ok(found) => return found,
                Err(free) => free,
            };
            let tentative = HashEntry::make(tag, TEMP_INVALID_ADDRESS, true);
            if free
                .slot()
                .compare_exchange(0, tentative.0, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            if self.other_slot_with_tag(bi, tag, &free) {
                free.slot().store(0, Ordering::Release);
                std::hint::spin_loop();
                continue;
            }
            free.slot().store(
                HashEntry::make(tag, TEMP_INVALID_ADDRESS, false).0,
                Ordering::Release,
            );
            return free;
        }
    }

    // ----------------------------------------------------------------------
    // Bucket latches (entry 7 of the main-table bucket)
    // ----------------------------------------------------------------------

    #[inline]
    fn latch_word(&self, bucket: usize) -> &AtomicU64 {
        &self.buckets[bucket].entries[OVERFLOW_ENTRY]
    }

    pub fn try_lock_shared(&self, bucket: usize) -> bool {
        let w = self.latch_word(bucket);
        for _ in 0..MAX_LOCK_SPINS {
            let cur = w.load(Ordering::Acquire);
            if cur & EXCLUSIVE_BIT != 0 || cur & SHARED_MASK == SHARED_MASK {
                std::hint::spin_loop();
                continue;
            }
            if w.compare_exchange_weak(cur, cur + SHARED_ONE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
        false
    }

    #[inline]
    pub fn unlock_shared(&self, bucket: usize) {
        let prev = self
            .latch_word(bucket)
            .fetch_sub(SHARED_ONE, Ordering::AcqRel);
        debug_assert!(prev & SHARED_MASK != 0);
    }

    /// Set the exclusive bit (blocking new readers), then wait a bounded time
    /// for existing readers to drain; on timeout release and return false.
    pub fn try_lock_exclusive(&self, bucket: usize) -> bool {
        let w = self.latch_word(bucket);
        let mut acquired_bit = false;
        for _ in 0..MAX_LOCK_SPINS {
            let cur = w.load(Ordering::Acquire);
            if cur & EXCLUSIVE_BIT != 0 {
                std::hint::spin_loop();
                continue;
            }
            if w.compare_exchange_weak(
                cur,
                cur | EXCLUSIVE_BIT,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
            {
                acquired_bit = true;
                break;
            }
        }
        if !acquired_bit {
            return false;
        }
        for _ in 0..MAX_READER_DRAIN_SPINS {
            if w.load(Ordering::Acquire) & SHARED_MASK == 0 {
                return true;
            }
            std::hint::spin_loop();
        }
        w.fetch_and(!EXCLUSIVE_BIT, Ordering::AcqRel);
        false
    }

    #[inline]
    pub fn unlock_exclusive(&self, bucket: usize) {
        let prev = self
            .latch_word(bucket)
            .fetch_and(!EXCLUSIVE_BIT, Ordering::AcqRel);
        debug_assert!(prev & EXCLUSIVE_BIT != 0);
    }

    #[inline]
    pub fn is_locked_exclusive(&self, bucket: usize) -> bool {
        self.latch_word(bucket).load(Ordering::Acquire) & EXCLUSIVE_BIT != 0
    }

    /// Visit every non-tentative entry: (bucket, tag, address).
    pub fn for_each_entry(&self, mut f: impl FnMut(usize, u64, u64)) {
        for bi in 0..self.buckets.len() {
            self.walk_chain(bi, |b| {
                for i in 0..ENTRIES_PER_BUCKET {
                    let e = HashEntry(b.entries[i].load(Ordering::Acquire));
                    if !e.is_empty() && !e.is_tentative() {
                        f(bi, e.tag(), e.address());
                    }
                }
                false
            });
        }
    }

    /// Reset every entry (used by recovery / FLUSHALL).
    pub fn clear(&self) {
        for b in self.buckets.iter().chain(self.overflow.iter()) {
            for e in &b.entries {
                e.store(0, Ordering::Release);
            }
        }
        self.overflow_next.store(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash64;
    use std::sync::Arc;

    #[test]
    fn entry_encoding() {
        let e = HashEntry::make(0x7fff, 0xffff_ffff_ffff, true);
        assert_eq!(e.tag(), 0x7fff);
        assert_eq!(e.address(), 0xffff_ffff_ffff);
        assert!(e.is_tentative());
        let e2 = HashEntry::make(5, 64, false);
        assert!(!e2.is_tentative());
        assert_eq!(e2.tag(), 5);
    }

    #[test]
    fn find_or_create_then_find() {
        let idx = HashIndex::new(16);
        let h = hash64(b"k");
        assert!(idx.find_tag(h).is_none());
        let s = idx.find_or_create_tag(h);
        assert_eq!(s.load().address(), TEMP_INVALID_ADDRESS);
        assert!(s.try_cas(s.load(), 1000));
        let f = idx.find_tag(h).unwrap();
        assert_eq!(f.load().address(), 1000);
        // Same tag again returns the same slot.
        let s2 = idx.find_or_create_tag(h);
        assert_eq!(s2.load().address(), 1000);
    }

    #[test]
    fn overflow_buckets_are_used() {
        // 2 buckets: force many tags into the same bucket via crafted hashes.
        let idx = HashIndex::new(2);
        let mut slots = Vec::new();
        for t in 1..40u64 {
            let h = t << (64 - TAG_BITS); // bucket 0, distinct tags
            let s = idx.find_or_create_tag(h);
            assert!(s.try_cas(s.load(), 64 * t));
            slots.push((h, 64 * t));
        }
        for (h, a) in slots {
            assert_eq!(idx.find_tag(h).unwrap().load().address(), a);
        }
        let mut n = 0;
        idx.for_each_entry(|_, _, _| n += 1);
        assert_eq!(n, 39);
    }

    #[test]
    fn concurrent_create_yields_single_entry_per_tag() {
        let idx = Arc::new(HashIndex::new(4));
        let mut hs = Vec::new();
        for _ in 0..8 {
            let idx = idx.clone();
            hs.push(std::thread::spawn(move || {
                for t in 1..200u64 {
                    let h = (t << (64 - TAG_BITS)) | (t & 3);
                    let s = idx.find_or_create_tag(h);
                    let _ = s.try_cas(
                        HashEntry::make(HashIndex::tag_of(h), TEMP_INVALID_ADDRESS, false),
                        64 * t,
                    );
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let mut seen = std::collections::HashSet::new();
        idx.for_each_entry(|b, tag, _| {
            assert!(seen.insert((b, tag)), "duplicate tag {tag} in bucket {b}");
        });
        assert_eq!(seen.len(), 199);
    }

    #[test]
    fn latches() {
        let idx = HashIndex::new(4);
        assert!(idx.try_lock_shared(1));
        assert!(idx.try_lock_shared(1));
        assert!(!idx.try_lock_exclusive(1), "readers present");
        idx.unlock_shared(1);
        idx.unlock_shared(1);
        assert!(idx.try_lock_exclusive(1));
        assert!(!idx.try_lock_shared(1));
        assert!(!idx.try_lock_exclusive(1));
        idx.unlock_exclusive(1);
        assert!(idx.try_lock_shared(1));
        idx.unlock_shared(1);
        // Latch bits must not disturb the overflow pointer and vice versa.
        let h = 1u64 << (64 - TAG_BITS) | 1;
        for t in 1..20u64 {
            let s = idx.find_or_create_tag((t << (64 - TAG_BITS)) | 1);
            s.try_cas(s.load(), 64 * t);
        }
        assert!(idx.try_lock_exclusive(1));
        assert!(idx.find_tag(h).is_some());
        idx.unlock_exclusive(1);
    }
}
