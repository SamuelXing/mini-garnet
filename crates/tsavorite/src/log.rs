//! The hybrid log allocator (`AllocatorBase.cs`).
//!
//! The record log is one 48-bit logical address space. Its tail lives in a
//! **circular buffer of fixed-size pages** in memory; older pages are flushed
//! to the [`Device`] and then evicted, so the same address space spans memory
//! and disk (paper §4.4, "larger-than-memory but memory-optimized").
//!
//! ```text
//!   disk ............ | in-memory circular buffer ............................ |
//!   Begin   ClosedUntil SafeHead Head   FlushedUntil SafeReadOnly ReadOnly   Tail
//!     |---------|---------|------|------------|------------|---------|--------|
//!                         <-- immutable (read-only) -->   fuzzy   <- mutable ->
//! ```
//!
//! Invariant (`AllocatorBase.cs:152-186`):
//! `Begin ≤ ClosedUntil ≤ SafeHead ≤ Head ≤ FlushedUntil ≤ SafeReadOnly ≤ ReadOnly ≤ Tail`.
//!
//! * `[ReadOnly, Tail)` **mutable**: records are updated in place.
//! * `[SafeReadOnly, ReadOnly)` **fuzzy**: `ReadOnly` was just advanced but some
//!   thread may still hold the *old* value and be mid in-place-update. Readers
//!   may read here; updaters must `RETRY_LATER` (refresh epoch) — otherwise a
//!   copy-to-tail could race an in-place write and lose it.
//! * `[Head, SafeReadOnly)` **immutable in memory**: updates copy the record to
//!   the tail (read-copy-update) and *seal* the old one.
//! * `[Begin, Head)` **on disk**: reads go to the device.
//!
//! Every boundary move is a two-step *epoch* protocol (`AllocatorBase.cs:336`):
//! publish the new value optimistically, then `bump_with_action(...)` so the
//! consequence (flushing, or freeing a page) happens only once every thread has
//! observed the new boundary. That is the whole trick that lets flush and
//! eviction run without ever blocking the request path.
//!
//! The tail is a single packed `(page, offset)` word advanced with
//! fetch-and-add (`TryAllocate`); the *first* thread whose allocation crosses
//! the page boundary "owns" the page turn (`HandlePageOverflow`).

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::device::Device;
use crate::epoch::LightEpoch;
use crate::record::{total_size_from_header, RecordPtr, MIN_DISK_READ};

/// Bytes reserved at the start of each page (`PageHeader.Size`). Address 0..63
/// are therefore never records, so 0 (invalid) and 1 (temp-invalid) are free.
pub const PAGE_HEADER_SIZE: usize = 64;
pub const FIRST_VALID_ADDRESS: u64 = PAGE_HEADER_SIZE as u64;

#[derive(Clone, Debug)]
pub struct LogSettings {
    /// Page size = 2^page_bits bytes.
    pub page_bits: u32,
    /// Number of pages held in memory (rounded up to a power of two).
    pub memory_pages: usize,
    /// Fraction of the in-memory pages kept mutable (`MutableFraction`, default 0.9).
    pub mutable_fraction: f64,
}

impl Default for LogSettings {
    fn default() -> Self {
        LogSettings {
            page_bits: 20,
            memory_pages: 64,
            mutable_fraction: 0.9,
        }
    }
}

pub enum AllocResult {
    Ok(u64),
    /// Another thread is turning the page; retry immediately.
    RetryNow,
    /// The next page is not free yet (flush/close pending); refresh epoch and retry.
    RetryLater,
}

struct Page {
    data: Box<[u64]>,
}

/// 8-byte aligned buffer holding a record image read from disk.
pub struct AlignedBuf(pub Vec<u64>);

impl AlignedBuf {
    pub fn with_len(bytes: usize) -> Self {
        AlignedBuf(vec![0u64; bytes.div_ceil(8)])
    }
    pub fn as_ptr(&self) -> *mut u8 {
        self.0.as_ptr() as *mut u8
    }
    pub fn as_bytes_mut(&mut self, len: usize) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.0.as_mut_ptr() as *mut u8, len) }
    }
    pub fn record(&self) -> RecordPtr {
        RecordPtr::new(self.as_ptr())
    }
}

pub struct HybridLog {
    page_bits: u32,
    page_size: usize,
    page_mask: u64,
    buffer_size: usize,
    mutable_pages: usize,
    pages: Box<[Page]>,

    /// Packed tail: (page << 32) | offset. Offset may transiently exceed page_size.
    tail: AtomicU64,
    begin_address: AtomicU64,
    closed_until_address: AtomicU64,
    safe_head_address: AtomicU64,
    head_address: AtomicU64,
    flushed_until_address: AtomicU64,
    safe_read_only_address: AtomicU64,
    read_only_address: AtomicU64,

    pub epoch: Arc<LightEpoch>,
    device: Box<dyn Device>,
    flush_lock: Mutex<()>,
    close_lock: Mutex<()>,
}

// Pages are only accessed through raw pointers guarded by epochs/latches.
unsafe impl Send for HybridLog {}
unsafe impl Sync for HybridLog {}

#[inline]
fn pack(page: u64, offset: u64) -> u64 {
    (page << 32) | offset
}
#[inline]
fn unpack(w: u64) -> (u64, u64) {
    (w >> 32, w & 0xffff_ffff)
}

impl HybridLog {
    pub fn new(
        settings: LogSettings,
        epoch: Arc<LightEpoch>,
        device: Box<dyn Device>,
    ) -> Arc<Self> {
        assert!(settings.page_bits >= 9 && settings.page_bits <= 30);
        let page_size = 1usize << settings.page_bits;
        let buffer_size = settings.memory_pages.max(2).next_power_of_two();
        let mutable_pages = ((buffer_size as f64) * settings.mutable_fraction)
            .ceil()
            .max(1.0) as usize;
        let mutable_pages = mutable_pages.min(buffer_size - 1);
        let pages: Vec<Page> = (0..buffer_size)
            .map(|_| Page {
                data: vec![0u64; page_size / 8].into_boxed_slice(),
            })
            .collect();
        Arc::new(HybridLog {
            page_bits: settings.page_bits,
            page_size,
            page_mask: (page_size - 1) as u64,
            buffer_size,
            mutable_pages,
            pages: pages.into_boxed_slice(),
            tail: AtomicU64::new(pack(0, FIRST_VALID_ADDRESS)),
            begin_address: AtomicU64::new(FIRST_VALID_ADDRESS),
            closed_until_address: AtomicU64::new(0),
            safe_head_address: AtomicU64::new(FIRST_VALID_ADDRESS),
            head_address: AtomicU64::new(FIRST_VALID_ADDRESS),
            flushed_until_address: AtomicU64::new(FIRST_VALID_ADDRESS),
            safe_read_only_address: AtomicU64::new(FIRST_VALID_ADDRESS),
            read_only_address: AtomicU64::new(FIRST_VALID_ADDRESS),
            epoch,
            device,
            flush_lock: Mutex::new(()),
            close_lock: Mutex::new(()),
        })
    }

    // -- address accessors ---------------------------------------------------

    #[inline]
    pub fn page_size(&self) -> usize {
        self.page_size
    }
    #[inline]
    pub fn begin_address(&self) -> u64 {
        self.begin_address.load(Ordering::Acquire)
    }
    #[inline]
    pub fn head_address(&self) -> u64 {
        self.head_address.load(Ordering::Acquire)
    }
    #[inline]
    pub fn safe_head_address(&self) -> u64 {
        self.safe_head_address.load(Ordering::Acquire)
    }
    #[inline]
    pub fn closed_until_address(&self) -> u64 {
        self.closed_until_address.load(Ordering::Acquire)
    }
    #[inline]
    pub fn flushed_until_address(&self) -> u64 {
        self.flushed_until_address.load(Ordering::Acquire)
    }
    #[inline]
    pub fn read_only_address(&self) -> u64 {
        self.read_only_address.load(Ordering::Acquire)
    }
    #[inline]
    pub fn safe_read_only_address(&self) -> u64 {
        self.safe_read_only_address.load(Ordering::Acquire)
    }

    /// Current tail address; spins while a page turn is in progress
    /// (`GetTailAddress` in Tsavorite).
    pub fn tail_address(&self) -> u64 {
        loop {
            let (page, off) = unpack(self.tail.load(Ordering::Acquire));
            if off <= self.page_size as u64 {
                return (page << self.page_bits) | off;
            }
            std::hint::spin_loop();
        }
    }

    #[inline]
    fn page_of(&self, addr: u64) -> u64 {
        addr >> self.page_bits
    }

    #[inline]
    fn page_base_ptr(&self, page: u64) -> *mut u8 {
        self.pages[(page as usize) & (self.buffer_size - 1)]
            .data
            .as_ptr() as *mut u8
    }

    /// Pointer to the in-memory record at `addr`. Caller guarantees
    /// `addr >= head_address` and that it holds the epoch.
    #[inline]
    pub fn record_at(&self, addr: u64) -> RecordPtr {
        debug_assert!(addr >= FIRST_VALID_ADDRESS);
        let page = self.page_of(addr);
        let off = (addr & self.page_mask) as usize;
        RecordPtr::new(unsafe { self.page_base_ptr(page).add(off) })
    }

    // -- allocation ------------------------------------------------------------

    #[inline]
    fn page_is_free(&self, page: u64) -> bool {
        if (page as usize) < self.buffer_size {
            return true;
        }
        // The slot's previous occupant is page - buffer_size; it must be fully closed.
        let prev_end = (page - self.buffer_size as u64 + 1) << self.page_bits;
        self.closed_until_address.load(Ordering::Acquire) >= prev_end
    }

    /// Reserve `size` bytes (8-byte multiple) at the tail (`TryAllocate`).
    pub fn try_allocate(self: &Arc<Self>, size: usize) -> AllocResult {
        debug_assert_eq!(size % 8, 0);
        assert!(
            size + PAGE_HEADER_SIZE <= self.page_size,
            "record larger than a page"
        );
        let old = self.tail.fetch_add(size as u64, Ordering::AcqRel);
        let (page, off) = unpack(old);
        let new_off = off + size as u64;
        if new_off <= self.page_size as u64 {
            return AllocResult::Ok((page << self.page_bits) | off);
        }
        if off > self.page_size as u64 {
            // Someone else crossed the boundary first and owns the page turn.
            return AllocResult::RetryNow;
        }
        // We own the page turn.
        let next = page + 1;
        if !self.page_is_free(next) {
            // Put the tail back at exactly page_size so the next allocator
            // re-runs this check; meanwhile push the flush/close machinery.
            self.tail
                .store(pack(page, self.page_size as u64), Ordering::Release);
            self.issue_shift_addresses(next);
            return AllocResult::RetryLater;
        }
        // Zero the page slot before publishing so scans can stop at the first
        // all-zero header.
        unsafe { std::ptr::write_bytes(self.page_base_ptr(next), 0, self.page_size) };
        self.tail.store(
            pack(next, FIRST_VALID_ADDRESS + size as u64),
            Ordering::Release,
        );
        self.issue_shift_addresses(next);
        AllocResult::Ok((next << self.page_bits) | FIRST_VALID_ADDRESS)
    }

    /// Spin the `RetryNow` case; the caller still handles `RetryLater`.
    pub fn try_allocate_retry_now(self: &Arc<Self>, size: usize) -> Option<u64> {
        loop {
            match self.try_allocate(size) {
                AllocResult::Ok(a) => return Some(a),
                AllocResult::RetryNow => std::hint::spin_loop(),
                AllocResult::RetryLater => return None,
            }
        }
    }

    /// On a page turn to `next`, decide how far ReadOnly and Head should move
    /// (`IssueShiftAddress` / `CalculateReadOnlyAddress`).
    fn issue_shift_addresses(self: &Arc<Self>, next: u64) {
        let ps = self.page_size as i64;
        let desired_head = ((next as i64 + 2 - self.buffer_size as i64).max(0)) * ps;
        let desired_ro = ((next as i64 + 1 - self.mutable_pages as i64).max(0)) * ps;
        let desired_ro = desired_ro.max(desired_head) as u64;
        let desired_head = desired_head as u64;
        if desired_ro > self.read_only_address() {
            self.shift_read_only_address(desired_ro);
        }
        if desired_head > self.head_address() {
            self.shift_head_address(desired_head);
        }
    }

    // -- boundary shifts (the epoch two-step) -------------------------------

    /// Publish a new ReadOnly address; after the epoch drains, mark the range
    /// safe and flush it. Returns false if nothing to do.
    pub fn shift_read_only_address(self: &Arc<Self>, new_ro: u64) -> bool {
        let mut cur = self.read_only_address.load(Ordering::Acquire);
        loop {
            if new_ro <= cur {
                return false;
            }
            match self.read_only_address.compare_exchange(
                cur,
                new_ro,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
        let me = Arc::clone(self);
        self.epoch
            .bump_with_action(move || me.on_pages_marked_read_only(new_ro));
        true
    }

    /// Runs once no thread can be updating below `new_ro` in place.
    fn on_pages_marked_read_only(&self, new_ro: u64) {
        self.safe_read_only_address
            .fetch_max(new_ro, Ordering::AcqRel);
        if let Err(e) = self.flush_until(new_ro, false) {
            eprintln!("tsavorite: flush failed: {e}");
        }
    }

    /// Flush `[flushed_until, until)` to the device. Serialized and monotonic,
    /// so out-of-order drain actions are harmless.
    pub fn flush_until(&self, until: u64, sync: bool) -> io::Result<()> {
        let _g = self.flush_lock.lock().unwrap();
        let from = self.flushed_until_address.load(Ordering::Acquire);
        if until <= from {
            if sync {
                self.device.sync()?;
            }
            return Ok(());
        }
        let mut addr = from;
        while addr < until {
            let page = self.page_of(addr);
            let page_start = page << self.page_bits;
            let start = (addr - page_start) as usize;
            let end = (until.min(page_start + self.page_size as u64) - page_start) as usize;
            let bytes = unsafe {
                std::slice::from_raw_parts(self.page_base_ptr(page).add(start), end - start)
            };
            // A store with heap-object values would serialize them here
            // instead of writing the in-memory bytes verbatim.
            self.device.write_at(addr, bytes)?;
            addr = page_start + end as u64;
        }
        if sync {
            self.device.sync()?;
        }
        self.flushed_until_address
            .fetch_max(until, Ordering::AcqRel);
        Ok(())
    }

    /// Publish a new Head (clamped to FlushedUntil: never evict unflushed
    /// data); after the epoch drains, close (free) the pages below it.
    pub fn shift_head_address(self: &Arc<Self>, desired: u64) -> bool {
        let new_head = desired.min(self.flushed_until_address());
        let mut cur = self.head_address.load(Ordering::Acquire);
        loop {
            if new_head <= cur {
                return false;
            }
            match self.head_address.compare_exchange(
                cur,
                new_head,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
        let me = Arc::clone(self);
        self.epoch
            .bump_with_action(move || me.on_pages_closed(new_head));
        true
    }

    /// Runs once no thread can hold a pointer below `new_head`.
    fn on_pages_closed(&self, new_head: u64) {
        self.safe_head_address.fetch_max(new_head, Ordering::AcqRel);
        let _g = self.close_lock.lock().unwrap();
        loop {
            let closed = self.closed_until_address.load(Ordering::Acquire);
            let page_end = closed + self.page_size as u64;
            if page_end > new_head {
                return;
            }
            // Nothing to do per record: the page's bytes are already on the
            // device and the hash chains point at logical addresses, which stay
            // valid once the records live on disk. That is what makes eviction
            // free in this design. A store with heap-object values would have
            // to serialize and release them here.
            self.closed_until_address.store(page_end, Ordering::Release);
        }
    }

    /// Advance ReadOnly to the tail and flush everything (fold-over checkpoint
    /// core, and what `Commit` on the append log does).
    pub fn shift_read_only_to_tail(self: &Arc<Self>) -> u64 {
        let tail = self.tail_address();
        self.shift_read_only_address(tail);
        tail
    }

    /// Block until `[.., until)` is flushed. The caller must not be protected.
    pub fn wait_flushed(&self, until: u64) {
        while self.flushed_until_address() < until {
            self.epoch.bump_current_epoch();
            std::thread::yield_now();
        }
    }

    /// Walk the records laid out in `base[from_off..limit]`, calling `f` with
    /// each record's offset.
    ///
    /// The stop condition is the log's central layout invariant: pages are
    /// zeroed before reuse and every live record's header word has at least one
    /// of Valid or Sealed set, so an all-zero header means "no record here" and
    /// nothing beyond it on this page is data. That is why no page needs an
    /// explicit end-of-data marker.
    ///
    /// # Safety
    /// `base` must point to at least `limit` bytes that are readable and
    /// writable for the duration of the call, 8-byte aligned, and laid out as a
    /// sequence of records starting at `from_off` (or zeroed). In practice the
    /// caller holds either an epoch-protected page or a buffer it owns.
    pub(crate) unsafe fn for_each_record_in_buf(
        base: *mut u8,
        from_off: usize,
        limit: usize,
        mut f: impl FnMut(usize, RecordPtr),
    ) {
        let mut off = from_off.max(PAGE_HEADER_SIZE);
        while off + MIN_DISK_READ <= limit {
            let rec = RecordPtr::new(unsafe { base.add(off) });
            if rec.info().load().0 == 0 && rec.key_len() == 0 && rec.value_cap() == 0 {
                return;
            }
            let size = rec.total_size();
            if off + size > limit {
                return;
            }
            f(off, rec);
            off += size;
        }
    }

    /// Walk records on one in-memory page (stops at the first all-zero header).
    pub fn for_each_record_in_memory_page(&self, page: u64, mut f: impl FnMut(RecordPtr)) {
        let base = self.page_base_ptr(page);
        let tail = self.tail_address();
        let page_start = page << self.page_bits;
        let limit = if self.page_of(tail) == page {
            (tail - page_start) as usize
        } else {
            self.page_size
        };
        // SAFETY: `base` is this log's page buffer, `limit` is within it, and
        // the caller holds the epoch so the page cannot be freed.
        unsafe { Self::for_each_record_in_buf(base, PAGE_HEADER_SIZE, limit, |_, rec| f(rec)) };
    }

    // -- disk reads --------------------------------------------------------

    /// Read the record at an on-disk address into an aligned buffer.
    pub fn read_record_from_disk(&self, addr: u64) -> io::Result<AlignedBuf> {
        let mut hdr = [0u8; MIN_DISK_READ];
        self.device.read_at(addr, &mut hdr)?;
        let size = total_size_from_header(&hdr);
        let mut buf = AlignedBuf::with_len(size);
        self.device.read_at(addr, buf.as_bytes_mut(size))?;
        Ok(buf)
    }

    /// Read `len` bytes starting at on-disk address `addr` into an aligned
    /// buffer laid out so that `buf.as_ptr() + (a - page_start)` addresses
    /// record `a` (i.e. the buffer starts at the page boundary).
    pub fn read_page_prefix_from_disk(&self, page: u64, len: usize) -> io::Result<AlignedBuf> {
        let mut buf = AlignedBuf::with_len(self.page_size);
        if len > 0 {
            self.device
                .read_at(page << self.page_bits, buf.as_bytes_mut(len))?;
        }
        Ok(buf)
    }

    #[inline]
    pub fn page_bits(&self) -> u32 {
        self.page_bits
    }

    /// Visit every record in `[from, to)` in address order, reading from disk
    /// below `head` and from memory above it. `f` gets (address, record).
    /// Caller holds the epoch so in-memory pages stay put.
    ///
    /// A page can straddle `head`: its `[page_start, flushed_until)` prefix is
    /// read from the device (immutable, so identical to the memory copy) and
    /// the remainder is walked in memory.
    pub fn scan(&self, from: u64, to: u64, mut f: impl FnMut(u64, RecordPtr)) -> io::Result<()> {
        let mut addr = from.max(FIRST_VALID_ADDRESS);
        while addr < to {
            let page = self.page_of(addr);
            let page_start = page << self.page_bits;
            let page_end = (page_start + self.page_size as u64).min(to);
            let head = self.head_address();
            let flushed = self.flushed_until_address();
            let disk_limit = if addr < head {
                (page_end.min(flushed) - page_start) as usize
            } else {
                0
            };
            let disk_buf = if disk_limit > 0 {
                Some(self.read_page_prefix_from_disk(page, disk_limit)?)
            } else {
                None
            };
            let mem_base = self.page_base_ptr(page);
            let start_off = ((addr - page_start) as usize).max(PAGE_HEADER_SIZE);
            let limit = (page_end - page_start) as usize;

            // The flushed prefix of this page is read from the device; the rest
            // is walked in memory. Both spans use the same record walker.
            let mut next_off = start_off;
            if start_off < disk_limit {
                let dbuf = disk_buf.as_ref().unwrap();
                // SAFETY: `dbuf` is a page-sized buffer this function owns.
                unsafe {
                    Self::for_each_record_in_buf(
                        dbuf.as_ptr(),
                        start_off,
                        disk_limit,
                        |off, rec| {
                            f(page_start + off as u64, rec);
                            next_off = off + rec.total_size();
                        },
                    )
                };
            }
            if next_off < limit {
                // SAFETY: in-memory page held by the caller's epoch.
                unsafe {
                    Self::for_each_record_in_buf(
                        mem_base,
                        next_off.max(disk_limit),
                        limit,
                        |off, rec| f(page_start + off as u64, rec),
                    )
                };
            }
            addr = page_start + self.page_size as u64;
        }
        Ok(())
    }

    // -- recovery support --------------------------------------------------

    /// Reset the log so that `[begin, tail)` is considered entirely on disk
    /// (cold recovery): head = tail, nothing in memory.
    pub fn recover_addresses(&self, begin: u64, tail: u64) {
        let page = self.page_of(tail);
        unsafe { std::ptr::write_bytes(self.page_base_ptr(page), 0, self.page_size) };
        self.begin_address.store(begin, Ordering::Release);
        self.closed_until_address
            .store(page << self.page_bits, Ordering::Release);
        self.safe_head_address.store(tail, Ordering::Release);
        self.head_address.store(tail, Ordering::Release);
        self.flushed_until_address.store(tail, Ordering::Release);
        self.safe_read_only_address.store(tail, Ordering::Release);
        self.read_only_address.store(tail, Ordering::Release);
        self.tail
            .store(pack(page, tail & self.page_mask), Ordering::Release);
    }

    pub fn device(&self) -> &dyn Device {
        &*self.device
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemDevice;
    use crate::record::RecordSizeInfo;

    fn small_log(pages: usize) -> Arc<HybridLog> {
        let epoch = Arc::new(LightEpoch::new());
        HybridLog::new(
            LogSettings {
                page_bits: 9,
                memory_pages: pages,
                mutable_fraction: 0.5,
            },
            epoch,
            Box::new(MemDevice::new()),
        )
    }

    fn alloc(log: &Arc<HybridLog>, size: usize) -> u64 {
        loop {
            match log.try_allocate(size) {
                AllocResult::Ok(a) => return a,
                AllocResult::RetryNow => {}
                AllocResult::RetryLater => {
                    log.epoch.refresh();
                    std::thread::yield_now();
                }
            }
        }
    }

    #[test]
    fn allocates_sequentially_and_turns_pages() {
        let log = small_log(4);
        let _g = crate::epoch::EpochGuard::new(&log.epoch);
        let a = alloc(&log, 64);
        assert_eq!(a, 64);
        let b = alloc(&log, 64);
        assert_eq!(b, 128);
        // Fill page 0 (512 bytes: 64 header + 7*64 = 512).
        for _ in 0..5 {
            alloc(&log, 64);
        }
        let c = alloc(&log, 64);
        assert_eq!(c, 512 + 64, "first slot of page 1");
        assert_eq!(log.tail_address(), 512 + 128);
    }

    #[test]
    fn eviction_flushes_then_frees_and_disk_read_works() {
        let log = small_log(4);
        let size = RecordSizeInfo {
            key_len: 4,
            value_cap: 8,
            has_expiration: false,
        };
        let mut addrs = Vec::new();
        {
            let g = crate::epoch::EpochGuard::new(&log.epoch);
            // Allocate through many pages (40-byte records, 11 per 512-byte page).
            for i in 0..200u32 {
                let a = alloc(&log, size.total_size());
                let rec = log.record_at(a);
                rec.init(0, false, &size, &i.to_le_bytes());
                rec.value_buf_mut()[..8].copy_from_slice(&(i as u64).to_le_bytes());
                rec.set_value_len(8);
                rec.info().unseal_and_validate();
                addrs.push((i, a));
                g.refresh();
            }
        }
        log.epoch.drain_all_blocking();
        assert!(
            log.head_address() > FIRST_VALID_ADDRESS,
            "head must have moved"
        );
        assert!(log.flushed_until_address() >= log.head_address());
        assert!(log.closed_until_address() <= log.safe_head_address());
        // Everything below head is readable from the device.
        let _g = crate::epoch::EpochGuard::new(&log.epoch);
        for (i, a) in &addrs {
            if *a < log.head_address() {
                let buf = log.read_record_from_disk(*a).unwrap();
                let rec = buf.record();
                assert_eq!(rec.key(), &i.to_le_bytes());
                assert_eq!(rec.value(), &(*i as u64).to_le_bytes());
            } else {
                let rec = log.record_at(*a);
                assert_eq!(rec.key(), &i.to_le_bytes());
            }
        }
        // Scan sees every record exactly once.
        let mut n = 0;
        log.scan(log.begin_address(), log.tail_address(), |_, rec| {
            assert!(rec.info().load().is_valid());
            n += 1;
        })
        .unwrap();
        assert_eq!(n, 200);
    }

    #[test]
    fn shift_read_only_to_tail_flushes_partial_page() {
        let log = small_log(4);
        let a = {
            let _g = crate::epoch::EpochGuard::new(&log.epoch);
            let a = alloc(&log, 64);
            let size = RecordSizeInfo {
                key_len: 1,
                value_cap: 8,
                has_expiration: false,
            };
            let rec = log.record_at(a);
            rec.init(0, false, &size, b"k");
            rec.info().unseal_and_validate();
            a
        };
        let tail = log.shift_read_only_to_tail();
        log.wait_flushed(tail);
        assert_eq!(log.flushed_until_address(), tail);
        let buf = log.read_record_from_disk(a).unwrap();
        assert_eq!(buf.record().key(), b"k");
    }
}
