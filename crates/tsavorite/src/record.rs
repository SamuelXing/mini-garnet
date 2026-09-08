//! Record layout in the hybrid log (paper Fig. 3, `RecordInfo.cs`,
//! `RecordDataHeader.cs`).
//!
//! ```text
//! +----------------+----------------+-------------+-------+---------------+
//! | RecordInfo 8B  | DataHeader 16B | [expiry 8B] |  key  | value (cap)   | pad to 8
//! +----------------+----------------+-------------+-------+---------------+
//! ```
//!
//! `RecordInfo` is one atomic 64-bit word so that all structural state of a
//! record (previous address in the hash chain + flag bits) is published or
//! changed with a single atomic store/CAS:
//!
//! ```text
//!  63 .. 53 | 52     | 51       | 50           | 49    | 48        | 47 .. 0
//!  unused   | Sealed | Modified | InNewVersion | Valid | Tombstone | PreviousAddress
//! ```
//!
//! * `PreviousAddress` – next older record with the same (bucket, tag). Chains
//!   are strictly address-descending.
//! * `Valid` – cleared while a freshly allocated record is being filled and
//!   before it is CAS'd into the index (and permanently if that CAS fails).
//! * `Sealed` – set on the *old* record when an update is copied to the tail
//!   (RCU). Any operation that sees a sealed record retries (`RETRY_LATER`),
//!   which guarantees a lost update can never land on the stale copy.
//! * `Tombstone` – logical delete.
//! * `InNewVersion` – record written after the checkpoint version bump (CPR).
//!
//! The data header stores explicit key/value lengths and the value capacity
//! (Tsavorite derives capacity from a trailing "filler" length; storing it is
//! simpler and shows the same idea: an in-place update may grow the value up to
//! the capacity without touching the index).

use std::sync::atomic::{AtomicU64, Ordering};

pub const ADDRESS_BITS: u32 = 48;
pub const ADDRESS_MASK: u64 = (1 << ADDRESS_BITS) - 1;

pub const TOMBSTONE_BIT: u64 = 1 << 48;
pub const VALID_BIT: u64 = 1 << 49;
pub const IN_NEW_VERSION_BIT: u64 = 1 << 50;
pub const MODIFIED_BIT: u64 = 1 << 51;
pub const SEALED_BIT: u64 = 1 << 52;

/// Address 0 is never valid.
pub const INVALID_ADDRESS: u64 = 0;
/// Placeholder stored in a freshly claimed index slot before any record exists.
pub const TEMP_INVALID_ADDRESS: u64 = 1;

/// A decoded `RecordInfo` word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordInfo(pub u64);

impl RecordInfo {
    #[inline]
    pub fn new(previous_address: u64, in_new_version: bool) -> Self {
        let mut w = previous_address & ADDRESS_MASK;
        if in_new_version {
            w |= IN_NEW_VERSION_BIT;
        }
        RecordInfo(w)
    }
    #[inline]
    pub fn previous_address(self) -> u64 {
        self.0 & ADDRESS_MASK
    }
    #[inline]
    pub fn is_valid(self) -> bool {
        self.0 & VALID_BIT != 0
    }
    #[inline]
    pub fn is_tombstone(self) -> bool {
        self.0 & TOMBSTONE_BIT != 0
    }
    #[inline]
    pub fn is_sealed(self) -> bool {
        self.0 & SEALED_BIT != 0
    }
    #[inline]
    pub fn in_new_version(self) -> bool {
        self.0 & IN_NEW_VERSION_BIT != 0
    }
    /// "Closed" = not usable by an in-flight operation: invalid or sealed.
    #[inline]
    pub fn is_closed(self) -> bool {
        !self.is_valid() || self.is_sealed()
    }
}

/// Atomic view of the record header word living inside a log page.
pub struct RecordInfoRef<'a>(pub &'a AtomicU64);

impl RecordInfoRef<'_> {
    #[inline]
    pub fn load(&self) -> RecordInfo {
        RecordInfo(self.0.load(Ordering::Acquire))
    }
    #[inline]
    pub fn store(&self, info: RecordInfo) {
        self.0.store(info.0, Ordering::Release)
    }
    /// Publish the record: set Valid (and clear Sealed which is set during fill).
    #[inline]
    pub fn unseal_and_validate(&self) {
        let mut w = self.0.load(Ordering::Acquire);
        loop {
            let n = (w | VALID_BIT) & !SEALED_BIT;
            match self
                .0
                .compare_exchange_weak(w, n, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(cur) => w = cur,
            }
        }
    }
    /// Seal (RCU source). Returns false if already sealed/invalid.
    #[inline]
    pub fn try_seal(&self) -> bool {
        let mut w = self.0.load(Ordering::Acquire);
        loop {
            if w & SEALED_BIT != 0 || w & VALID_BIT == 0 {
                return false;
            }
            match self.0.compare_exchange_weak(
                w,
                w | SEALED_BIT,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(cur) => w = cur,
            }
        }
    }
    #[inline]
    pub fn seal_and_invalidate(&self) {
        self.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |w| {
                Some((w | SEALED_BIT) & !VALID_BIT)
            })
            .ok();
    }
    #[inline]
    pub fn set_invalid(&self) {
        self.0.fetch_and(!VALID_BIT, Ordering::AcqRel);
    }
    #[inline]
    pub fn set_tombstone(&self) {
        self.0.fetch_or(TOMBSTONE_BIT, Ordering::AcqRel);
    }
    #[inline]
    pub fn set_modified(&self) {
        self.0.fetch_or(MODIFIED_BIT, Ordering::AcqRel);
    }
    /// CAS the previous-address field (used when repairing chains).
    #[inline]
    pub fn try_update_previous_address(&self, expected: u64, new: u64) -> bool {
        let mut w = self.0.load(Ordering::Acquire);
        loop {
            if w & ADDRESS_MASK != expected & ADDRESS_MASK {
                return false;
            }
            let n = (w & !ADDRESS_MASK) | (new & ADDRESS_MASK);
            match self
                .0
                .compare_exchange_weak(w, n, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(cur) => w = cur,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Data header + layout arithmetic
// ---------------------------------------------------------------------------

pub const RECORD_INFO_SIZE: usize = 8;
pub const DATA_HEADER_SIZE: usize = 16;
pub const FIXED_HEADER_SIZE: usize = RECORD_INFO_SIZE + DATA_HEADER_SIZE;
pub const EXPIRATION_SIZE: usize = 8;

pub const FLAG_HAS_EXPIRATION: u32 = 1;

/// What a record needs to look like; produced by the `GetLength`-style
/// callbacks so the store can allocate before invoking the writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordSizeInfo {
    pub key_len: usize,
    /// Bytes reserved for the value (>= the initial value length).
    pub value_cap: usize,
    pub has_expiration: bool,
}

impl RecordSizeInfo {
    #[inline]
    pub fn total_size(&self) -> usize {
        let raw = FIXED_HEADER_SIZE
            + if self.has_expiration {
                EXPIRATION_SIZE
            } else {
                0
            }
            + self.key_len
            + self.value_cap;
        (raw + 7) & !7
    }
}

#[inline]
pub fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// Raw accessor over a record living at `ptr` (in a log page or a disk read
/// buffer). All methods are `unsafe`-free at the API level; the *caller*
/// (the store) guarantees, via epoch protection and bucket latches, that the
/// memory stays valid and is not concurrently mutated in a conflicting way.
#[derive(Clone, Copy)]
pub struct RecordPtr {
    pub ptr: *mut u8,
}

unsafe impl Send for RecordPtr {}
unsafe impl Sync for RecordPtr {}

impl RecordPtr {
    #[inline]
    pub fn new(ptr: *mut u8) -> Self {
        RecordPtr { ptr }
    }

    #[inline]
    pub fn info(&self) -> RecordInfoRef<'_> {
        // SAFETY: the first 8 bytes of a record are the header word; pages are
        // 8-byte aligned and records are 8-byte aligned within pages.
        RecordInfoRef(unsafe { &*(self.ptr as *const AtomicU64) })
    }

    #[inline]
    fn hdr_u32(&self, i: usize) -> u32 {
        unsafe {
            (self.ptr.add(RECORD_INFO_SIZE) as *const u32)
                .add(i)
                .read_unaligned()
        }
    }
    #[inline]
    fn set_hdr_u32(&self, i: usize, v: u32) {
        unsafe {
            (self.ptr.add(RECORD_INFO_SIZE) as *mut u32)
                .add(i)
                .write_unaligned(v)
        }
    }

    #[inline]
    pub fn key_len(&self) -> usize {
        self.hdr_u32(0) as usize
    }
    #[inline]
    pub fn value_len(&self) -> usize {
        self.hdr_u32(1) as usize
    }
    #[inline]
    pub fn value_cap(&self) -> usize {
        self.hdr_u32(2) as usize
    }
    #[inline]
    pub fn flags(&self) -> u32 {
        self.hdr_u32(3)
    }
    #[inline]
    pub fn has_expiration_slot(&self) -> bool {
        self.flags() & FLAG_HAS_EXPIRATION != 0
    }

    #[inline]
    fn key_offset(&self) -> usize {
        FIXED_HEADER_SIZE
            + if self.has_expiration_slot() {
                EXPIRATION_SIZE
            } else {
                0
            }
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.add(self.key_offset()), self.key_len()) }
    }
    #[inline]
    pub fn value(&self) -> &[u8] {
        let off = self.key_offset() + self.key_len();
        unsafe { std::slice::from_raw_parts(self.ptr.add(off), self.value_len()) }
    }
    /// Mutable view over the whole value *capacity*.
    ///
    /// Takes `&self` by design: records live in shared log pages behind raw
    /// pointers, and Tsavorite mutates them in place under external
    /// synchronization (the caller holds the record's bucket latch, or owns a
    /// freshly allocated not-yet-published record). This mirrors the C#
    /// pointer-based record API and is why the borrow is from `&self`.
    #[allow(clippy::mut_from_ref)]
    #[inline]
    pub fn value_buf_mut(&self) -> &mut [u8] {
        let off = self.key_offset() + self.key_len();
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(off), self.value_cap()) }
    }
    #[inline]
    pub fn set_value_len(&self, n: usize) {
        debug_assert!(n <= self.value_cap());
        self.set_hdr_u32(1, n as u32);
    }
    /// Expiration timestamp (0 = none). Returns 0 when the record has no slot.
    #[inline]
    pub fn expiration(&self) -> i64 {
        if !self.has_expiration_slot() {
            return 0;
        }
        unsafe { (self.ptr.add(FIXED_HEADER_SIZE) as *const i64).read_unaligned() }
    }
    /// Returns false if the record has no expiration slot (caller must RCU).
    #[inline]
    pub fn set_expiration(&self, ts: i64) -> bool {
        if !self.has_expiration_slot() {
            return false;
        }
        unsafe { (self.ptr.add(FIXED_HEADER_SIZE) as *mut i64).write_unaligned(ts) };
        true
    }

    /// Total bytes occupied by this record (8-aligned).
    #[inline]
    pub fn total_size(&self) -> usize {
        align8(self.key_offset() + self.key_len() + self.value_cap())
    }

    /// Initialize a freshly allocated record: header word is written *sealed and
    /// invalid* so concurrent readers that reach it via a torn chain skip it
    /// until it is CAS'd into the index and validated.
    pub fn init(
        &self,
        previous_address: u64,
        in_new_version: bool,
        size: &RecordSizeInfo,
        key: &[u8],
    ) {
        debug_assert_eq!(size.key_len, key.len());
        let info = RecordInfo::new(previous_address, in_new_version).0 | SEALED_BIT;
        self.info().0.store(info, Ordering::Release);
        self.set_hdr_u32(0, key.len() as u32);
        self.set_hdr_u32(1, 0);
        self.set_hdr_u32(2, size.value_cap as u32);
        self.set_hdr_u32(
            3,
            if size.has_expiration {
                FLAG_HAS_EXPIRATION
            } else {
                0
            },
        );
        if size.has_expiration {
            self.set_expiration(0);
        }
        unsafe {
            std::ptr::copy_nonoverlapping(key.as_ptr(), self.ptr.add(self.key_offset()), key.len());
        }
    }

    /// Copy the raw bytes of this record into a Vec (used when reading from disk).
    pub fn to_bytes(&self) -> Vec<u8> {
        unsafe { std::slice::from_raw_parts(self.ptr, self.total_size()).to_vec() }
    }
}

/// Header bytes needed to compute a record's total size (read this much from
/// disk first, then the rest).
pub const MIN_DISK_READ: usize = FIXED_HEADER_SIZE;

/// Given the first `MIN_DISK_READ` bytes of a record, compute its total size.
pub fn total_size_from_header(hdr: &[u8]) -> usize {
    debug_assert!(hdr.len() >= FIXED_HEADER_SIZE);
    let u = |i: usize| {
        u32::from_le_bytes(
            hdr[RECORD_INFO_SIZE + 4 * i..RECORD_INFO_SIZE + 4 * i + 4]
                .try_into()
                .unwrap(),
        ) as usize
    };
    let key_len = u(0);
    let value_cap = u(2);
    let flags = u(3) as u32;
    let exp = if flags & FLAG_HAS_EXPIRATION != 0 {
        EXPIRATION_SIZE
    } else {
        0
    };
    align8(FIXED_HEADER_SIZE + exp + key_len + value_cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_roundtrip() {
        let size = RecordSizeInfo {
            key_len: 3,
            value_cap: 10,
            has_expiration: true,
        };
        assert_eq!(size.total_size(), align8(24 + 8 + 3 + 10));
        let mut buf = vec![0u64; size.total_size() / 8];
        let rec = RecordPtr::new(buf.as_mut_ptr() as *mut u8);
        rec.init(0x1234, true, &size, b"abc");
        assert!(rec.info().load().is_sealed());
        assert!(!rec.info().load().is_valid());
        assert_eq!(rec.info().load().previous_address(), 0x1234);
        assert!(rec.info().load().in_new_version());
        rec.value_buf_mut()[..5].copy_from_slice(b"hello");
        rec.set_value_len(5);
        assert!(rec.set_expiration(99));
        rec.info().unseal_and_validate();
        let i = rec.info().load();
        assert!(i.is_valid() && !i.is_sealed());
        assert_eq!(rec.key(), b"abc");
        assert_eq!(rec.value(), b"hello");
        assert_eq!(rec.expiration(), 99);
        assert_eq!(rec.total_size(), size.total_size());
        assert_eq!(total_size_from_header(&rec.to_bytes()), size.total_size());
        assert!(rec.info().try_seal());
        assert!(!rec.info().try_seal());
        assert!(rec.info().load().is_closed());
    }

    #[test]
    fn no_expiration_slot() {
        let size = RecordSizeInfo {
            key_len: 1,
            value_cap: 1,
            has_expiration: false,
        };
        let mut buf = vec![0u64; size.total_size() / 8];
        let rec = RecordPtr::new(buf.as_mut_ptr() as *mut u8);
        rec.init(0, false, &size, b"k");
        assert!(!rec.set_expiration(5));
        assert_eq!(rec.expiration(), 0);
        assert_eq!(rec.total_size(), 32);
    }
}
