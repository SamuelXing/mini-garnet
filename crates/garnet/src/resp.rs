//! RESP (REdis Serialization Protocol) parsing and response writing.
//!
//! Design notes (mirrors Garnet's `RespServerSession` / `RespWriteUtils`):
//!
//! * Parsing is **incremental and zero-copy**. A receive buffer may hold many
//!   pipelined commands and may end in the middle of one. [`parse_command`]
//!   returns `Ok(None)` for an incomplete command; the caller keeps the
//!   unconsumed tail (Garnet's `readHead`/`bytesRead` leftover copy) and
//!   parses again once more bytes arrive. Arguments are returned as byte
//!   ranges into the buffer (Garnet's `ArgSlice`) so that values can be copied
//!   straight from the network buffer into the store record.
//! * Responses are appended to a send buffer that is flushed once per batch.

use std::fmt;

/// A parsed command: argument byte ranges into the receive buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgSlice {
    pub start: usize,
    pub end: usize,
}

impl ArgSlice {
    #[inline]
    pub fn bytes<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
        &buf[self.start..self.end]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// Unparseable input; the connection should be closed.
    Malformed(&'static str),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Malformed(m) => write!(f, "protocol error: {m}"),
        }
    }
}

/// Absolute offset in the receive buffer just past a command's final CRLF.
pub type Next = usize;

/// Parse one command starting at `pos`, appending its argument ranges to
/// `args`. Returns the offset just past the command.
///
/// `args` is caller-owned and cleared here, so a connection can reuse one
/// allocation for its whole lifetime. That matters: this is the per-command hot
/// path, and a parser that advertises zero-copy arguments should not malloc a
/// vector to describe them.
///
/// Supports the RESP array-of-bulk-strings form that every real client sends,
/// and the inline form (`PING\r\n`) that telnet and nc send.
///
/// Returns `Ok(None)` when the buffer ends before the command is complete; the
/// caller keeps the bytes and re-parses once more arrive.
pub fn parse_command(
    buf: &[u8],
    pos: usize,
    args: &mut Vec<ArgSlice>,
) -> Result<Option<Next>, ProtocolError> {
    args.clear();
    if pos >= buf.len() {
        return Ok(None);
    }
    match buf[pos] {
        b'*' => parse_array(buf, pos, args),
        _ => parse_inline(buf, pos, args),
    }
}

/// Find CRLF at or after `pos`; returns index of '\r'.
#[inline]
fn find_crlf(buf: &[u8], pos: usize) -> Option<usize> {
    let mut i = pos;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Parse a decimal integer from `buf[pos..end]`.
#[inline]
fn parse_int(s: &[u8]) -> Result<i64, ProtocolError> {
    if s.is_empty() {
        return Err(ProtocolError::Malformed("empty integer"));
    }
    let (neg, digits) = if s[0] == b'-' {
        (true, &s[1..])
    } else {
        (false, s)
    };
    if digits.is_empty() {
        return Err(ProtocolError::Malformed("empty integer"));
    }
    let mut v: i64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return Err(ProtocolError::Malformed("non-digit in integer"));
        }
        v = v
            .checked_mul(10)
            .and_then(|v| v.checked_add((c - b'0') as i64))
            .ok_or(ProtocolError::Malformed("integer overflow"))?;
    }
    Ok(if neg { -v } else { v })
}

fn parse_array(
    buf: &[u8],
    pos: usize,
    args: &mut Vec<ArgSlice>,
) -> Result<Option<Next>, ProtocolError> {
    debug_assert_eq!(buf[pos], b'*');
    let Some(cr) = find_crlf(buf, pos + 1) else {
        return Ok(None);
    };
    let count = parse_int(&buf[pos + 1..cr])?;
    if count < 0 {
        return Err(ProtocolError::Malformed("negative array length"));
    }
    if count > 1024 * 1024 {
        return Err(ProtocolError::Malformed("array too large"));
    }
    let mut cur = cr + 2;
    args.reserve(count as usize);
    for _ in 0..count {
        if cur >= buf.len() {
            return Ok(None);
        }
        if buf[cur] != b'$' {
            return Err(ProtocolError::Malformed("expected bulk string"));
        }
        let Some(cr) = find_crlf(buf, cur + 1) else {
            return Ok(None);
        };
        let len = parse_int(&buf[cur + 1..cr])?;
        if len < 0 {
            return Err(ProtocolError::Malformed("negative bulk length"));
        }
        let len = len as usize;
        let start = cr + 2;
        let end = start + len;
        // Need data + trailing CRLF.
        if end + 2 > buf.len() {
            return Ok(None);
        }
        if buf[end] != b'\r' || buf[end + 1] != b'\n' {
            return Err(ProtocolError::Malformed("bulk string not CRLF terminated"));
        }
        args.push(ArgSlice { start, end });
        cur = end + 2;
    }
    Ok(Some(cur))
}

fn parse_inline(
    buf: &[u8],
    pos: usize,
    args: &mut Vec<ArgSlice>,
) -> Result<Option<Next>, ProtocolError> {
    let Some(cr) = find_crlf(buf, pos) else {
        return Ok(None);
    };
    let mut i = pos;
    while i < cr {
        while i < cr && buf[i] == b' ' {
            i += 1;
        }
        if i >= cr {
            break;
        }
        let start = i;
        while i < cr && buf[i] != b' ' {
            i += 1;
        }
        args.push(ArgSlice { start, end: i });
    }
    Ok(Some(cr + 2))
}

// ---------------------------------------------------------------------------
// Response writer: append RESP replies to a send buffer.
// ---------------------------------------------------------------------------

pub const OK: &[u8] = b"+OK\r\n";
pub const PONG: &[u8] = b"+PONG\r\n";
pub const QUEUED: &[u8] = b"+QUEUED\r\n";
pub const NULL_BULK: &[u8] = b"$-1\r\n";
pub const NULL_ARRAY: &[u8] = b"*-1\r\n";
pub const EMPTY_ARRAY: &[u8] = b"*0\r\n";

#[inline]
pub fn write_simple(out: &mut Vec<u8>, s: &str) {
    out.push(b'+');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_error(out: &mut Vec<u8>, s: &str) {
    out.push(b'-');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_integer(out: &mut Vec<u8>, v: i64) {
    out.push(b':');
    write_i64_digits(out, v);
    out.extend_from_slice(b"\r\n");
}

/// Bulk string header only (`$len\r\n`). The caller then appends `len` bytes
/// and CRLF. Used when the value is copied directly out of the store record
/// into the send buffer without an intermediate allocation.
#[inline]
pub fn write_bulk_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'$');
    write_i64_digits(out, len as i64);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_bulk(out: &mut Vec<u8>, data: &[u8]) {
    write_bulk_header(out, data.len());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_array_header(out: &mut Vec<u8>, n: usize) {
    out.push(b'*');
    write_i64_digits(out, n as i64);
    out.extend_from_slice(b"\r\n");
}

/// Append the decimal digits of `v` without any framing.
#[inline]
pub fn write_i64_digits(out: &mut Vec<u8>, v: i64) {
    let mut buf = I64Buf::new();
    out.extend_from_slice(buf.fmt(v));
}

/// Stack buffer for formatting an i64 without allocating. `i64::MIN` is the
/// widest case at 20 bytes.
pub struct I64Buf([u8; 20]);

impl Default for I64Buf {
    fn default() -> Self {
        Self::new()
    }
}

impl I64Buf {
    #[inline]
    pub fn new() -> Self {
        I64Buf([0; 20])
    }

    #[inline]
    pub fn fmt(&mut self, v: i64) -> &[u8] {
        let mut i = self.0.len();
        let mut u = v.unsigned_abs();
        if u == 0 {
            i -= 1;
            self.0[i] = b'0';
        }
        while u > 0 {
            i -= 1;
            self.0[i] = b'0' + (u % 10) as u8;
            u /= 10;
        }
        if v < 0 {
            i -= 1;
            self.0[i] = b'-';
        }
        &self.0[i..]
    }
}

/// Parse an i64 from a *command argument*. Distinct from [`parse_int`], which
/// parses protocol framing lengths and must be strict.
pub fn parse_i64(s: &[u8]) -> Option<i64> {
    std::str::from_utf8(s).ok()?.trim().parse().ok()
}

/// Decimal length of an i64 (used by INCR-family in-place update sizing).
#[inline]
pub fn i64_digit_len(v: i64) -> usize {
    let mut n = if v < 0 { 1 } else { 0 };
    let mut u = v.unsigned_abs();
    if u == 0 {
        return 1;
    }
    while u > 0 {
        n += 1;
        u /= 10;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(buf: &[u8], pos: usize) -> Option<(Vec<Vec<u8>>, usize)> {
        let mut args = Vec::new();
        let next = parse_command(buf, pos, &mut args).unwrap()?;
        Some((args.iter().map(|a| a.bytes(buf).to_vec()).collect(), next))
    }

    #[test]
    fn parses_array_command() {
        let buf = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n";
        let (args, next) = parse(buf, 0).unwrap();
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"k".to_vec(), b"hello".to_vec()]
        );
        assert_eq!(next, buf.len());
    }

    #[test]
    fn parses_pipelined_batch_and_partial_tail() {
        let full = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n*2\r\n$3\r\nGET\r\n$1\r\nb\r\n".to_vec();
        // Cut the second command in the middle of its bulk string.
        let cut = &full[..full.len() - 3];
        let (args, next1) = parse(cut, 0).unwrap();
        assert_eq!(args[1], b"a");
        assert!(parse(cut, next1).is_none(), "second is incomplete");
        // After the rest arrives, it parses.
        let (args, next2) = parse(&full, next1).unwrap();
        assert_eq!(args[1], b"b");
        assert_eq!(next2, full.len());
        assert!(parse(&full, next2).is_none());
    }

    #[test]
    fn parses_inline() {
        let buf = b"PING\r\nSET  a b\r\n";
        let (args, next) = parse(buf, 0).unwrap();
        assert_eq!(args, vec![b"PING".to_vec()]);
        let (args, _) = parse(buf, next).unwrap();
        assert_eq!(args, vec![b"SET".to_vec(), b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn rejects_malformed() {
        let mut a = Vec::new();
        assert!(parse_command(b"*1\r\n:5\r\n", 0, &mut a).is_err());
        assert!(parse_command(b"*1\r\n$2\r\nabc\r\n", 0, &mut a).is_err());
        assert!(parse_command(b"*x\r\n", 0, &mut a).is_err());
    }

    #[test]
    fn writes_replies() {
        let mut out = Vec::new();
        write_integer(&mut out, -42);
        write_bulk(&mut out, b"hi");
        write_array_header(&mut out, 2);
        write_simple(&mut out, "OK");
        write_error(&mut out, "ERR x");
        assert_eq!(out, b":-42\r\n$2\r\nhi\r\n*2\r\n+OK\r\n-ERR x\r\n");
        assert_eq!(i64_digit_len(0), 1);
        assert_eq!(i64_digit_len(-105), 4);
        assert_eq!(i64_digit_len(i64::MIN), 20);
    }
}
