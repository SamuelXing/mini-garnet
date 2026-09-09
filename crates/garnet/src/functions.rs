//! `StringFunctions`: the RESP string data type expressed against Tsavorite's
//! RUMDS callbacks (paper §4.2, Table 3; `MainSessionFunctions`).
//!
//! This is the "user logic atop the narrow waist": the store knows nothing
//! about RESP; it only calls `reader` / `initial_writer` / `in_place_updater`
//! / `copy_updater` etc., and this module decides what each RESP command does
//! to a record. Adding a command means editing only this file — exactly the
//! extensibility the paper highlights.
//!
//! Key mappings (see `commands.rs` for who calls Read vs Upsert vs RMW):
//! * GET / GETRANGE / STRLEN / TTL / TYPE → Read (`reader`)
//! * plain SET → Upsert (`initial_writer` / `in_place_writer`)
//! * SET NX|XX|GET|EX|KEEPTTL, SETNX, SETEX, GETSET, APPEND, SETRANGE,
//!   INCR/DECR family, GETDEL, GETEX, EXPIRE/PERSIST → RMW
//! * DEL → Delete
//!
//! **Lazy expiration** is enforced here, on every access, using the
//! deterministic `now_ms` carried in the input (never the wall clock inside a
//! callback) so that AOF replay expires exactly the same records
//! (`RespInputHeader.CheckExpiry` + the Deterministic flag).
//!
//! Values point into the network receive buffer via [`Bytes`], mirroring
//! Garnet's `ArgSlice`/`PinnedSpanByte`: no copy until the bytes land in the
//! record.

use crate::resp::{i64_digit_len, parse_i64, I64Buf};
use tsavorite::record::{RecordPtr, RecordSizeInfo};
use tsavorite::store::{CopyDecision, InPlaceResult, SessionFunctions};

/// A slice into a buffer owned elsewhere (the network receive buffer). Valid
/// for the duration of the synchronous store operation. Mirrors `ArgSlice`.
#[derive(Clone, Copy)]
pub struct Bytes {
    ptr: *const u8,
    len: usize,
}

impl Bytes {
    #[inline]
    pub fn new(s: &[u8]) -> Self {
        Bytes {
            ptr: s.as_ptr(),
            len: s.len(),
        }
    }
    #[inline]
    pub fn empty() -> Self {
        Self::new(b"")
    }
    #[inline]
    pub fn get(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StringOp {
    // Reads
    Get,
    GetRange {
        start: i64,
        end: i64,
    },
    Strlen,
    /// TTL/PTTL: remaining time (ms if `ms`), or -1 (no expiry) / -2 (missing).
    Ttl {
        ms: bool,
    },
    // Blind write
    Set,
    // RMW writes
    SetKeepTtl,
    SetNx,
    SetXx,
    /// SET ... GET: return old value, then set.
    SetGet {
        keep_ttl: bool,
    },
    IncrBy,
    Append,
    SetRange {
        offset: usize,
    },
    GetDel,
    GetEx,
    /// Existence check only: no payload, so the reader copies nothing.
    Exists,
    Expire,
    Persist,
}

/// Input to a string operation. All non-determinism (`now_ms`, the target
/// `expiration`) is captured here so replay is deterministic.
#[derive(Clone, Copy)]
pub struct StringInput {
    pub op: StringOp,
    /// The value / appended bytes (points into the receive buffer).
    pub operand: Bytes,
    /// INCR/DECR delta.
    pub delta: i64,
    /// Absolute expiration in ms since epoch. 0 = no expiration; used by
    /// SET EX, SETEX, EXPIRE, GETEX.
    pub expiration: i64,
    /// Deterministic "now" in ms; expiry is checked against this.
    pub now_ms: i64,
}

impl StringInput {
    pub fn read(op: StringOp, now_ms: i64) -> Self {
        Self::simple(op, Bytes::empty(), now_ms)
    }
    pub fn simple(op: StringOp, operand: Bytes, now_ms: i64) -> Self {
        StringInput {
            op,
            operand,
            delta: 0,
            expiration: 0,
            now_ms,
        }
    }
}

/// Output of a string operation.
#[derive(Default, Clone)]
pub struct StringOutput {
    /// Value bytes for value-returning commands (GET, GETSET/GETDEL old value…).
    pub bytes: Vec<u8>,
    pub has_bytes: bool,
    /// Integer result (INCR new value, STRLEN, APPEND/SETRANGE length, TTL…).
    pub int: i64,
    pub has_int: bool,
}

impl StringOutput {
    fn set_bytes(&mut self, b: &[u8]) {
        self.bytes.clear();
        self.bytes.extend_from_slice(b);
        self.has_bytes = true;
    }
    fn set_int(&mut self, v: i64) {
        self.int = v;
        self.has_int = true;
    }
}

pub struct StringFunctions;

/// Is a record with this expiration slot expired at `now`?
#[inline]
fn expired(rec: RecordPtr, now: i64) -> bool {
    rec.has_expiration_slot() && rec.expiration() != 0 && rec.expiration() <= now
}

/// Normalize GETRANGE bounds to `[start, end)` byte offsets over `len`.
fn range_bounds(start: i64, end: i64, len: usize) -> (usize, usize) {
    let len = len as i64;
    let norm = |i: i64| if i < 0 { (len + i).max(0) } else { i.min(len) };
    let s = norm(start);
    let e = if end < 0 { len + end } else { end.min(len - 1) };
    if len == 0 || s > e || s >= len {
        (0, 0)
    } else {
        (s as usize, (e + 1).min(len) as usize)
    }
}

impl SessionFunctions for StringFunctions {
    type Input = StringInput;
    type Output = StringOutput;

    // ---- Read ----
    fn reader(
        &mut self,
        _key: &[u8],
        input: &StringInput,
        rec: RecordPtr,
        out: &mut StringOutput,
    ) -> bool {
        if expired(rec, input.now_ms) {
            return false; // treated as NotFound (lazy expiration)
        }
        match input.op {
            StringOp::GetRange { start, end } => {
                let (s, e) = range_bounds(start, end, rec.value_len());
                out.set_bytes(&rec.value()[s..e]);
            }
            StringOp::Exists => {} // presence is the whole answer
            StringOp::Strlen => out.set_int(rec.value_len() as i64),
            StringOp::Ttl { ms } => {
                let v = if !rec.has_expiration_slot() || rec.expiration() == 0 {
                    -1
                } else {
                    let rem = rec.expiration() - input.now_ms;
                    let rem = rem.max(0);
                    if ms {
                        rem
                    } else {
                        (rem + 999) / 1000
                    }
                };
                out.set_int(v);
            }
            _ => out.set_bytes(rec.value()),
        }
        true
    }

    // ---- Upsert (plain SET) ----
    fn upsert_size(&mut self, key: &[u8], input: &StringInput) -> RecordSizeInfo {
        RecordSizeInfo {
            key_len: key.len(),
            value_cap: input.operand.len(),
            has_expiration: input.expiration != 0,
        }
    }
    fn initial_writer(
        &mut self,
        _key: &[u8],
        input: &StringInput,
        rec: RecordPtr,
        _out: &mut StringOutput,
    ) {
        write_val(rec, input.operand.get());
        if input.expiration != 0 {
            rec.set_expiration(input.expiration);
        }
    }
    fn in_place_writer(
        &mut self,
        _key: &[u8],
        input: &StringInput,
        rec: RecordPtr,
        _out: &mut StringOutput,
    ) -> bool {
        let v = input.operand.get();
        if v.len() > rec.value_cap() {
            return false;
        }
        // Plain SET clears any prior TTL.
        if rec.has_expiration_slot() {
            rec.set_expiration(input.expiration);
        } else if input.expiration != 0 {
            return false; // need a slot -> copy
        }
        write_val(rec, v);
        true
    }

    // ---- RMW ----
    fn need_initial_update(
        &mut self,
        _key: &[u8],
        input: &StringInput,
        _out: &mut StringOutput,
    ) -> bool {
        // SET ... GET (without XX) still creates when absent, returning nil.
        !matches!(
            input.op,
            StringOp::SetXx
                | StringOp::GetDel
                | StringOp::GetEx
                | StringOp::Expire
                | StringOp::Persist
        )
    }

    fn initial_size(&mut self, key: &[u8], input: &StringInput) -> RecordSizeInfo {
        let cap = match input.op {
            StringOp::IncrBy => i64_digit_len(input.delta),
            StringOp::SetRange { offset } => offset + input.operand.len(),
            _ => input.operand.len(),
        };
        RecordSizeInfo {
            key_len: key.len(),
            value_cap: cap,
            has_expiration: input.expiration != 0,
        }
    }
    fn initial_updater(
        &mut self,
        _key: &[u8],
        input: &StringInput,
        rec: RecordPtr,
        out: &mut StringOutput,
    ) {
        match input.op {
            StringOp::IncrBy => {
                write_val(rec, I64Buf::new().fmt(input.delta));
                out.set_int(input.delta);
            }
            StringOp::SetRange { offset } => {
                let buf = rec.value_buf_mut();
                buf[..offset].fill(0);
                let v = input.operand.get();
                buf[offset..offset + v.len()].copy_from_slice(v);
                rec.set_value_len(offset + v.len());
                out.set_int((offset + v.len()) as i64);
            }
            StringOp::Append => {
                let v = input.operand.get();
                write_val(rec, v);
                out.set_int(v.len() as i64);
            }
            StringOp::SetGet { .. } => {
                write_val(rec, input.operand.get());
                out.has_bytes = false; // old value was nil
            }
            _ => {
                write_val(rec, input.operand.get());
            }
        }
        if input.expiration != 0 {
            rec.set_expiration(input.expiration);
        }
    }

    fn in_place_updater(
        &mut self,
        _key: &[u8],
        input: &StringInput,
        rec: RecordPtr,
        out: &mut StringOutput,
    ) -> InPlaceResult {
        if expired(rec, input.now_ms) {
            return InPlaceResult::Expired; // tombstone, then re-run as initial
        }
        match input.op {
            StringOp::SetNx => InPlaceResult::Cancel, // key exists: do nothing
            StringOp::SetXx | StringOp::Set | StringOp::SetKeepTtl | StringOp::SetGet { .. } => {
                let v = input.operand.get();
                if v.len() > rec.value_cap() {
                    return InPlaceResult::NeedCopy;
                }
                if matches!(input.op, StringOp::SetGet { .. }) {
                    out.set_bytes(rec.value());
                }
                if !apply_ttl_in_place(rec, input) {
                    return InPlaceResult::NeedCopy;
                }
                write_val(rec, v);
                InPlaceResult::Updated
            }
            StringOp::IncrBy => {
                let Some(cur) = parse_i64(rec.value()) else {
                    // Non-integer value: signal the caller with a sentinel.
                    out.set_int(i64::MIN);
                    return InPlaceResult::Cancel;
                };
                let Some(n) = cur.checked_add(input.delta) else {
                    out.set_int(i64::MIN);
                    return InPlaceResult::Cancel;
                };
                let mut buf = I64Buf::new();
                let s = buf.fmt(n);
                if s.len() > rec.value_cap() {
                    return InPlaceResult::NeedCopy;
                }
                write_val(rec, s);
                out.set_int(n);
                InPlaceResult::Updated
            }
            StringOp::Append => {
                let v = input.operand.get();
                let n = rec.value_len() + v.len();
                if n > rec.value_cap() {
                    return InPlaceResult::NeedCopy;
                }
                let start = rec.value_len();
                rec.value_buf_mut()[start..n].copy_from_slice(v);
                rec.set_value_len(n);
                out.set_int(n as i64);
                InPlaceResult::Updated
            }
            StringOp::SetRange { offset } => {
                let v = input.operand.get();
                let n = (offset + v.len()).max(rec.value_len());
                if n > rec.value_cap() {
                    return InPlaceResult::NeedCopy;
                }
                let old_len = rec.value_len();
                let buf = rec.value_buf_mut();
                if offset > old_len {
                    buf[old_len..offset].fill(0);
                }
                buf[offset..offset + v.len()].copy_from_slice(v);
                rec.set_value_len(n);
                out.set_int(n as i64);
                InPlaceResult::Updated
            }
            StringOp::GetDel => {
                out.set_bytes(rec.value());
                InPlaceResult::Delete
            }
            StringOp::GetEx => {
                out.set_bytes(rec.value());
                if !apply_ttl_in_place(rec, input) {
                    return InPlaceResult::NeedCopy;
                }
                InPlaceResult::Updated
            }
            StringOp::Expire => {
                if rec.has_expiration_slot() {
                    rec.set_expiration(input.expiration);
                    out.set_int(1);
                    InPlaceResult::Updated
                } else {
                    InPlaceResult::NeedCopy
                }
            }
            StringOp::Persist => {
                if rec.has_expiration_slot() && rec.expiration() != 0 {
                    rec.set_expiration(0);
                    out.set_int(1);
                    InPlaceResult::Updated
                } else {
                    out.set_int(0);
                    InPlaceResult::Cancel
                }
            }
            _ => InPlaceResult::NeedCopy,
        }
    }

    fn need_copy_update(
        &mut self,
        _key: &[u8],
        input: &StringInput,
        old: RecordPtr,
        out: &mut StringOutput,
    ) -> CopyDecision {
        if expired(old, input.now_ms) {
            // Old value logically gone; caller falls to the initial path.
            return if matches!(
                input.op,
                StringOp::Set
                    | StringOp::SetKeepTtl
                    | StringOp::SetNx
                    | StringOp::IncrBy
                    | StringOp::Append
                    | StringOp::SetRange { .. }
                    | StringOp::GetEx
            ) {
                CopyDecision::Copy
            } else {
                CopyDecision::Cancel
            };
        }
        match input.op {
            StringOp::SetNx => CopyDecision::Cancel, // exists -> don't overwrite
            // The same guard `in_place_updater` applies, at the copy boundary.
            // Without it INCR would answer differently depending on which log
            // region the record has drifted into, and `copy_updater` would
            // silently reset a non-integer value to the delta.
            StringOp::IncrBy => {
                match parse_i64(old.value()).and_then(|cur| cur.checked_add(input.delta)) {
                    Some(_) => CopyDecision::Copy,
                    None => CopyDecision::Cancel,
                }
            }
            StringOp::GetDel => {
                // Return the old value and tombstone it in the same pass.
                out.set_bytes(old.value());
                CopyDecision::Delete
            }
            StringOp::Expire => {
                out.set_int(1);
                CopyDecision::Copy
            }
            _ => CopyDecision::Copy,
        }
    }

    fn copy_size(&mut self, key: &[u8], input: &StringInput, old: RecordPtr) -> RecordSizeInfo {
        let old_expired = expired(old, input.now_ms);
        let old_val = if old_expired { &b""[..] } else { old.value() };
        let cap = match input.op {
            StringOp::IncrBy => {
                let cur = if old_expired {
                    0
                } else {
                    parse_i64(old_val).unwrap_or(0)
                };
                i64_digit_len(cur.wrapping_add(input.delta))
            }
            StringOp::Append => old_val.len() + input.operand.len(),
            StringOp::SetRange { offset } => (offset + input.operand.len()).max(old_val.len()),
            StringOp::GetEx | StringOp::Expire | StringOp::Persist => old_val.len(),
            _ => input.operand.len(),
        };
        let has_exp = match input.op {
            StringOp::Persist => false,
            StringOp::Set => input.expiration != 0,
            _ => input.expiration != 0 || (old.has_expiration_slot() && old.expiration() != 0),
        };
        RecordSizeInfo {
            key_len: key.len(),
            value_cap: cap,
            has_expiration: has_exp,
        }
    }

    fn copy_updater(
        &mut self,
        _key: &[u8],
        input: &StringInput,
        old: RecordPtr,
        new: RecordPtr,
        out: &mut StringOutput,
    ) {
        let old_expired = expired(old, input.now_ms);
        let old_val = if old_expired { &b""[..] } else { old.value() };
        // Carry TTL forward where the semantics require it.
        let carry_ttl = old.has_expiration_slot() && old.expiration() != 0 && !old_expired;
        match input.op {
            StringOp::IncrBy => {
                let cur = if old_expired {
                    0
                } else {
                    parse_i64(old_val).unwrap_or(0)
                };
                let n = cur.wrapping_add(input.delta);
                write_val(new, I64Buf::new().fmt(n));
                out.set_int(n);
            }
            StringOp::Append => {
                let appended = input.operand.get();
                let new_len = old_val.len() + appended.len();
                let buf = new.value_buf_mut();
                buf[..old_val.len()].copy_from_slice(old_val);
                buf[old_val.len()..new_len].copy_from_slice(appended);
                new.set_value_len(new_len);
                out.set_int(new_len as i64);
            }
            StringOp::SetRange { offset } => {
                let v = input.operand.get();
                let n = (offset + v.len()).max(old_val.len());
                let buf = new.value_buf_mut();
                buf[..n].fill(0);
                buf[..old_val.len()].copy_from_slice(old_val);
                buf[offset..offset + v.len()].copy_from_slice(v);
                new.set_value_len(n);
                out.set_int(n as i64);
            }
            StringOp::SetGet { .. } => {
                out.set_bytes(old_val);
                write_val(new, input.operand.get());
            }
            StringOp::GetEx => {
                out.set_bytes(old_val);
                write_val(new, old_val);
            }
            StringOp::Expire => {
                write_val(new, old_val);
            }
            StringOp::Persist => {
                write_val(new, old_val);
                out.set_int(1);
            }
            _ => {
                write_val(new, input.operand.get());
            }
        }
        // TTL for the new record.
        if new.has_expiration_slot() {
            let exp = match input.op {
                // Plain SET drops TTL unless EX is given.
                StringOp::Set | StringOp::Expire | StringOp::GetEx => input.expiration,
                StringOp::Persist => 0,
                _ => {
                    if input.expiration != 0 {
                        input.expiration
                    } else if carry_ttl {
                        old.expiration()
                    } else {
                        0
                    }
                }
            };
            new.set_expiration(exp);
        }
    }
}

#[inline]
fn write_val(rec: RecordPtr, v: &[u8]) {
    rec.value_buf_mut()[..v.len()].copy_from_slice(v);
    rec.set_value_len(v.len());
}

/// Apply the input's TTL to an existing record in place. Returns false if a
/// TTL is requested but the record has no expiration slot (→ copy).
#[inline]
fn apply_ttl_in_place(rec: RecordPtr, input: &StringInput) -> bool {
    match input.op {
        StringOp::SetKeepTtl => true, // leave the slot alone
        StringOp::Set
        | StringOp::SetXx
        | StringOp::SetGet { keep_ttl: false }
        | StringOp::GetEx => {
            if input.expiration != 0 {
                rec.set_expiration(input.expiration)
            } else if rec.has_expiration_slot() {
                rec.set_expiration(0)
            } else {
                true
            }
        }
        StringOp::SetGet { keep_ttl: true } => true,
        _ => {
            if input.expiration != 0 {
                rec.set_expiration(input.expiration)
            } else {
                true
            }
        }
    }
}
