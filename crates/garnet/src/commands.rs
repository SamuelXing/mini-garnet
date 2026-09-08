//! RESP command dispatch: the "query processing" layer that maps hundreds of
//! RESP commands onto the five RUMDS operations (paper §3.2, §4.2;
//! `RespServerSession`, `BasicCommands`, `MainStoreOps`).
//!
//! A [`RespSession`] owns one Tsavorite session (FIFO, per connection) plus the
//! transaction state. [`RespSession::dispatch`] takes the parsed argument
//! slices (borrowed from the receive buffer) and appends the RESP reply to the
//! per-connection send buffer — exactly one place decides the reply shape,
//! while `functions.rs` decides the record mutation.
//!
//! Writes are logged to the AOF (DOL) after they succeed, and watched-key
//! versions are bumped so MULTI/EXEC can detect conflicts (paper §4.6).

use std::sync::Arc;

use tsavorite::store::{LockType, Session, Status};

use crate::aof::AofOp;
use crate::functions::{Bytes, StringFunctions, StringInput, StringOp, StringOutput};
use crate::resp;
use crate::resp::parse_i64;
use crate::shared::Shared;

pub enum Action {
    Continue,
    Close,
}

#[derive(Default)]
struct TxnState {
    in_multi: bool,
    /// A queued command failed to parse/validate; EXEC will abort.
    dirty: bool,
    queued: Vec<Vec<u8>>,
    /// Watched (slot, version-at-watch).
    watches: Vec<(usize, u64)>,
}

pub struct RespSession {
    pub shared: Arc<Shared>,
    store: Session<StringFunctions>,
    txn: TxnState,
    /// Deterministic clock for the command currently executing.
    now_ms: i64,
    /// True while replaying the AOF (suppress re-logging and use logged clock).
    replay: bool,
    out_scratch: StringOutput,
}

impl RespSession {
    pub fn new(shared: Arc<Shared>) -> Self {
        let store = shared.store.new_session(StringFunctions);
        RespSession {
            shared,
            store,
            txn: TxnState::default(),
            now_ms: 0,
            replay: false,
            out_scratch: StringOutput::default(),
        }
    }

    pub fn set_replay(&mut self, replay: bool) {
        self.replay = replay;
    }

    /// Encode `parts` as a RESP command, dispatch it, and return the raw reply
    /// bytes. For in-process use (tests, embedded) without a socket.
    pub fn run(&mut self, parts: &[&[u8]]) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            raw.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            raw.extend_from_slice(p);
            raw.extend_from_slice(b"\r\n");
        }
        let mut out = Vec::new();
        let mut slices = Vec::new();
        if let Ok(Some(_)) = crate::resp::parse_command(&raw, 0, &mut slices) {
            let args: Vec<&[u8]> = slices.iter().map(|a| a.bytes(&raw)).collect();
            self.dispatch(&raw, &args, &mut out);
        }
        out
    }

    // -- helpers -----------------------------------------------------------

    fn log_write(&self, raw: &[u8]) {
        if self.replay {
            return;
        }
        if let Some(aof) = &self.shared.aof {
            let end = aof.enqueue(AofOp::Command, self.now_ms, raw).unwrap_or(0);
            if self.shared.aof_commit_wait {
                let _ = aof.wait_committed(end);
            }
        }
    }

    fn bump_watch(&self, key: &[u8]) {
        self.shared.watch.bump(key);
    }

    /// Bookkeeping every successful write owes: invalidate anyone WATCHing the
    /// key, then append the command to the operation log. Kept in one place so
    /// a new command cannot do half of it.
    fn after_write(&mut self, key: &[u8], raw: &[u8]) {
        self.bump_watch(key);
        self.log_write(raw);
    }

    // -- entry point -------------------------------------------------------

    /// Dispatch one command. `raw` is the exact RESP bytes of the command
    /// (used for AOF logging and for queuing inside MULTI).
    pub fn dispatch(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>) -> Action {
        if args.is_empty() {
            return Action::Continue;
        }
        if !self.replay {
            self.now_ms = self.shared.now_ms();
        }
        let (name, nlen) = command_name(args[0]);
        let cmd = &name[..nlen];

        // Inside MULTI, queue everything except the control commands.
        if self.txn.in_multi
            && !matches!(
                cmd,
                b"EXEC" | b"DISCARD" | b"MULTI" | b"WATCH" | b"RESET" | b"QUIT"
            )
        {
            if let Some(err) = validate_queueable(cmd) {
                self.txn.dirty = true;
                resp::write_error(out, err);
            } else {
                self.txn.queued.push(raw.to_vec());
                out.extend_from_slice(resp::QUEUED);
            }
            return Action::Continue;
        }

        self.exec_command(cmd, raw, args, out)
    }

    fn exec_command(
        &mut self,
        cmd: &[u8],
        raw: &[u8],
        args: &[&[u8]],
        out: &mut Vec<u8>,
    ) -> Action {
        match cmd {
            b"PING" => {
                if args.len() >= 2 {
                    resp::write_bulk(out, args[1]);
                } else {
                    out.extend_from_slice(resp::PONG);
                }
            }
            b"ECHO" if args.len() == 2 => resp::write_bulk(out, args[1]),
            b"QUIT" => {
                out.extend_from_slice(resp::OK);
                return Action::Close;
            }
            b"SELECT" => out.extend_from_slice(resp::OK),
            b"HELLO" => self.cmd_hello(out),
            b"COMMAND" => out.extend_from_slice(resp::EMPTY_ARRAY),
            b"CONFIG" => out.extend_from_slice(resp::EMPTY_ARRAY),
            b"CLIENT" => out.extend_from_slice(resp::OK),
            b"INFO" => resp::write_bulk(out, b"# Server\r\nmini_garnet:1\r\n"),
            b"RESET" => {
                self.discard();
                resp::write_simple(out, "RESET");
            }
            b"DBSIZE" => resp::write_integer(out, self.shared.store.count_live() as i64),
            b"FLUSHDB" | b"FLUSHALL" => {
                self.shared.store.index.clear();
                out.extend_from_slice(resp::OK);
            }

            b"GET" if args.len() == 2 => self.cmd_get(args[1], out),
            b"SET" => self.cmd_set(raw, args, out),
            b"SETNX" if args.len() == 3 => self.cmd_setnx(raw, args, out),
            b"SETEX" if args.len() == 4 => self.cmd_setex(raw, args, out, false),
            b"PSETEX" if args.len() == 4 => self.cmd_setex(raw, args, out, true),
            b"GETSET" if args.len() == 3 => self.cmd_getset(raw, args, out),
            b"GETDEL" if args.len() == 2 => self.cmd_getdel(raw, args, out),
            b"GETEX" => self.cmd_getex(raw, args, out),
            b"APPEND" if args.len() == 3 => {
                self.cmd_rmw_int(raw, args[1], StringOp::Append, Bytes::new(args[2]), 0, out)
            }
            b"STRLEN" if args.len() == 2 => self.cmd_read_int(args[1], StringOp::Strlen, out),
            b"GETRANGE" | b"SUBSTR" if args.len() == 4 => self.cmd_getrange(args, out),
            b"SETRANGE" if args.len() == 4 => self.cmd_setrange(raw, args, out),
            b"INCR" if args.len() == 2 => self.cmd_incr(raw, args[1], 1, out),
            b"DECR" if args.len() == 2 => self.cmd_incr(raw, args[1], -1, out),
            b"INCRBY" if args.len() == 3 => match parse_i64(args[2]) {
                Some(d) => self.cmd_incr(raw, args[1], d, out),
                None => resp::write_error(out, "ERR value is not an integer or out of range"),
            },
            b"DECRBY" if args.len() == 3 => match parse_i64(args[2]) {
                Some(d) => self.cmd_incr(raw, args[1], d.wrapping_neg(), out),
                None => resp::write_error(out, "ERR value is not an integer or out of range"),
            },
            b"MSET" if args.len() >= 3 && args.len() % 2 == 1 => self.cmd_mset(raw, args, out),
            b"MGET" if args.len() >= 2 => self.cmd_mget(args, out),

            b"DEL" | b"UNLINK" if args.len() >= 2 => self.cmd_del(raw, args, out),
            b"EXISTS" if args.len() >= 2 => self.cmd_exists(args, out),
            b"EXPIRE" if args.len() == 3 => {
                self.cmd_expire(raw, args[1], args[2], 1000, false, out)
            }
            b"PEXPIRE" if args.len() == 3 => self.cmd_expire(raw, args[1], args[2], 1, false, out),
            b"EXPIREAT" if args.len() == 3 => {
                self.cmd_expire(raw, args[1], args[2], 1000, true, out)
            }
            b"PEXPIREAT" if args.len() == 3 => self.cmd_expire(raw, args[1], args[2], 1, true, out),
            b"TTL" if args.len() == 2 => self.cmd_ttl(args[1], false, out),
            b"PTTL" if args.len() == 2 => self.cmd_ttl(args[1], true, out),
            b"PERSIST" if args.len() == 2 => self.cmd_persist(raw, args[1], out),
            b"TYPE" if args.len() == 2 => self.cmd_type(args[1], out),
            b"KEYS" if args.len() == 2 => self.cmd_keys(args[1], out),
            b"SCAN" if args.len() >= 2 => self.cmd_scan(args, out),

            b"MULTI" => self.cmd_multi(out),
            b"EXEC" => return self.cmd_exec(out),
            b"DISCARD" => {
                if self.txn.in_multi {
                    self.discard();
                    out.extend_from_slice(resp::OK);
                } else {
                    resp::write_error(out, "ERR DISCARD without MULTI");
                }
            }
            b"WATCH" if args.len() >= 2 => self.cmd_watch(args, out),
            b"UNWATCH" => {
                self.txn.watches.clear();
                out.extend_from_slice(resp::OK);
            }

            b"SAVE" | b"BGSAVE" => self.cmd_save(out),
            b"BGREWRITEAOF" => out.extend_from_slice(resp::OK),
            b"LASTSAVE" => resp::write_integer(out, self.shared.last_save_ms() / 1000),

            _ => {
                let n = String::from_utf8_lossy(args[0]);
                resp::write_error(
                    out,
                    &format!("ERR unknown command '{n}' or wrong number of arguments"),
                );
            }
        }
        Action::Continue
    }

    // -- string commands ---------------------------------------------------

    fn cmd_get(&mut self, key: &[u8], out: &mut Vec<u8>) {
        let input = StringInput::read(StringOp::Get, self.now_ms);
        let output = &mut self.out_scratch;
        let status = self.store.read(key, &input, output);
        match status {
            Status::Found => resp::write_bulk(out, &output.bytes),
            _ => out.extend_from_slice(resp::NULL_BULK),
        }
    }

    fn cmd_read_int(&mut self, key: &[u8], op: StringOp, out: &mut Vec<u8>) {
        let input = StringInput::read(op, self.now_ms);
        let output = &mut self.out_scratch;
        let status = self.store.read(key, &input, output);
        match status {
            Status::Found => resp::write_integer(out, output.int),
            _ => resp::write_integer(out, 0),
        }
    }

    fn cmd_getrange(&mut self, args: &[&[u8]], out: &mut Vec<u8>) {
        let (Some(s), Some(e)) = (parse_i64(args[2]), parse_i64(args[3])) else {
            resp::write_error(out, "ERR value is not an integer or out of range");
            return;
        };
        let input = StringInput::read(StringOp::GetRange { start: s, end: e }, self.now_ms);
        let output = &mut self.out_scratch;
        let status = self.store.read(args[1], &input, output);
        match status {
            Status::Found => resp::write_bulk(out, &output.bytes),
            _ => resp::write_bulk(out, b""),
        }
    }

    fn cmd_set(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'set' command");
            return;
        }
        let options = match parse_set_opts(&args[3..], self.now_ms) {
            Ok(options) => options,
            Err(error) => {
                resp::write_error(out, error);
                return;
            }
        };
        let key = args[1];
        let op = if options.get {
            StringOp::SetGet {
                keep_ttl: options.keep_ttl,
            }
        } else if options.nx {
            StringOp::SetNx
        } else if options.xx {
            StringOp::SetXx
        } else if options.keep_ttl {
            StringOp::SetKeepTtl
        } else {
            StringOp::Set
        };
        let input = StringInput {
            op,
            operand: Bytes::new(args[2]),
            delta: 0,
            expiration: options.expiration,
            now_ms: self.now_ms,
        };
        let output = &mut self.out_scratch;
        // Plain SET is a blind write; the other forms inspect the current record.
        let status = if op == StringOp::Set {
            self.store.upsert(key, &input, output)
        } else {
            self.store.rmw(key, &input, output)
        };
        if options.get {
            if output.has_bytes {
                resp::write_bulk(out, &output.bytes);
            } else {
                out.extend_from_slice(resp::NULL_BULK);
            }
        } else if op == StringOp::Set || status.is_updated() {
            out.extend_from_slice(resp::OK);
        } else {
            out.extend_from_slice(resp::NULL_BULK);
        }
        if status.is_updated() {
            self.after_write(key, raw);
        }
    }

    fn cmd_setnx(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
        let input = StringInput::simple(StringOp::SetNx, Bytes::new(args[2]), self.now_ms);
        let output = &mut self.out_scratch;
        let status = self.store.rmw(args[1], &input, output);

        let set = status == Status::Created;
        resp::write_integer(out, set as i64);
        if set {
            self.after_write(args[1], raw);
        }
    }

    fn cmd_setex(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>, ms: bool) {
        let Some(t) = parse_i64(args[2]).filter(|t| *t > 0) else {
            resp::write_error(out, "ERR invalid expire time in 'setex' command");
            return;
        };
        let expiration = self.now_ms + if ms { t } else { t * 1000 };
        let input = StringInput {
            op: StringOp::Set,
            operand: Bytes::new(args[3]),
            delta: 0,
            expiration,
            now_ms: self.now_ms,
        };
        let output = &mut self.out_scratch;
        self.store.upsert(args[1], &input, output);

        out.extend_from_slice(resp::OK);
        self.after_write(args[1], raw);
    }

    fn cmd_getset(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
        let input = StringInput::simple(
            StringOp::SetGet { keep_ttl: false },
            Bytes::new(args[2]),
            self.now_ms,
        );
        let output = &mut self.out_scratch;
        self.store.rmw(args[1], &input, output);
        if output.has_bytes {
            resp::write_bulk(out, &output.bytes);
        } else {
            out.extend_from_slice(resp::NULL_BULK);
        }

        self.after_write(args[1], raw);
    }

    fn cmd_getdel(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
        // One RMW: the callback returns the old value and tombstones the record
        // in the same pass, so no other client can slip in between.
        let input = StringInput::read(StringOp::GetDel, self.now_ms);
        let output = &mut self.out_scratch;
        output.bytes.clear();
        let status = self.store.rmw(args[1], &input, output);
        if status == Status::Deleted {
            resp::write_bulk(out, &output.bytes);
            self.after_write(args[1], raw);
        } else {
            out.extend_from_slice(resp::NULL_BULK);
        }
    }

    fn cmd_getex(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
        if args.len() == 2 {
            self.cmd_get(args[1], out);
            return;
        }
        // GETEX key EX s | PX ms | EXAT | PXAT | PERSIST
        let mut expiration = 0i64;
        let mut i = 2;
        let mut persist = false;
        while i < args.len() {
            let opt = args[i];
            if opt.eq_ignore_ascii_case(b"PERSIST") {
                persist = true;
                i += 1;
            } else if is_expiry_keyword(opt) && i + 1 < args.len() {
                let Some(t) = parse_i64(args[i + 1]) else {
                    resp::write_error(out, "ERR value is not an integer or out of range");
                    return;
                };
                expiration = expire_at_ms(opt, t, self.now_ms);
                i += 2;
            } else {
                resp::write_error(out, "ERR syntax error");
                return;
            }
        }
        // Always GetEx, never the standalone Persist op: GETEX owes the caller
        // the old value, and `expiration == 0` already means "clear the TTL".
        debug_assert!(!persist || expiration == 0);
        let input = StringInput {
            op: StringOp::GetEx,
            operand: Bytes::empty(),
            delta: 0,
            expiration,
            now_ms: self.now_ms,
        };
        let output = &mut self.out_scratch;
        output.bytes.clear();
        // One RMW: the callback writes the old value out and applies the new TTL.
        let status = self.store.rmw(args[1], &input, output);
        if status.is_updated() {
            resp::write_bulk(out, &output.bytes);
            self.after_write(args[1], raw);
        } else {
            out.extend_from_slice(resp::NULL_BULK);
        }
    }

    fn cmd_incr(&mut self, raw: &[u8], key: &[u8], delta: i64, out: &mut Vec<u8>) {
        let input = StringInput {
            op: StringOp::IncrBy,
            operand: Bytes::empty(),
            delta,
            expiration: 0,
            now_ms: self.now_ms,
        };
        let output = &mut self.out_scratch;
        let status = self.store.rmw(key, &input, output);
        if status.is_updated() {
            resp::write_integer(out, output.int);
            self.after_write(key, raw);
        } else {
            resp::write_error(out, "ERR value is not an integer or out of range");
        }
    }

    fn cmd_rmw_int(
        &mut self,
        raw: &[u8],
        key: &[u8],
        op: StringOp,
        operand: Bytes,
        delta: i64,
        out: &mut Vec<u8>,
    ) {
        let input = StringInput {
            op,
            operand,
            delta,
            expiration: 0,
            now_ms: self.now_ms,
        };
        let output = &mut self.out_scratch;
        let status = self.store.rmw(key, &input, output);
        if status.is_updated() {
            resp::write_integer(out, output.int);
            self.after_write(key, raw);
        } else {
            resp::write_integer(out, 0);
        }
    }

    fn cmd_setrange(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
        let Some(off) = parse_i64(args[2]).filter(|offset| *offset >= 0) else {
            resp::write_error(out, "ERR offset is out of range");
            return;
        };
        self.cmd_rmw_int(
            raw,
            args[1],
            StringOp::SetRange {
                offset: off as usize,
            },
            Bytes::new(args[3]),
            0,
            out,
        );
    }

    fn cmd_mset(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
        let (pairs, _) = args[1..].as_chunks::<2>();
        for &[key, value] in pairs {
            let input = StringInput::simple(StringOp::Set, Bytes::new(value), self.now_ms);
            self.store.upsert(key, &input, &mut self.out_scratch);
            self.bump_watch(key);
        }
        out.extend_from_slice(resp::OK);
        self.log_write(raw);
    }

    fn cmd_mget(&mut self, args: &[&[u8]], out: &mut Vec<u8>) {
        resp::write_array_header(out, args.len() - 1);
        for &key in &args[1..] {
            self.cmd_get(key, out);
        }
    }

    // -- generic commands --------------------------------------------------

    fn cmd_del(&mut self, raw: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
        let mut n = 0;
        for &key in &args[1..] {
            if self.store.delete(key) == Status::Deleted {
                n += 1;
                self.bump_watch(key);
            }
        }
        resp::write_integer(out, n);
        if n > 0 {
            self.log_write(raw);
        }
    }

    fn cmd_exists(&mut self, args: &[&[u8]], out: &mut Vec<u8>) {
        let mut n = 0;
        for &key in &args[1..] {
            let input = StringInput::read(StringOp::Exists, self.now_ms);
            let output = &mut self.out_scratch;
            if self.store.read(key, &input, output) == Status::Found {
                n += 1;
            }
        }
        resp::write_integer(out, n);
    }

    fn cmd_expire(
        &mut self,
        raw: &[u8],
        key: &[u8],
        secs: &[u8],
        unit: i64,
        at: bool,
        out: &mut Vec<u8>,
    ) {
        let Some(t) = parse_i64(secs) else {
            resp::write_error(out, "ERR value is not an integer or out of range");
            return;
        };
        let expiration = if at { t * unit } else { self.now_ms + t * unit };
        let input = StringInput {
            op: StringOp::Expire,
            operand: Bytes::empty(),
            delta: 0,
            expiration,
            now_ms: self.now_ms,
        };
        let output = &mut self.out_scratch;
        let status = self.store.rmw(key, &input, output);

        let set = status.is_updated();
        resp::write_integer(out, set as i64);
        if set {
            self.after_write(key, raw);
        }
    }

    fn cmd_ttl(&mut self, key: &[u8], ms: bool, out: &mut Vec<u8>) {
        let input = StringInput::read(StringOp::Ttl { ms }, self.now_ms);
        let output = &mut self.out_scratch;
        let status = self.store.read(key, &input, output);
        match status {
            Status::Found => resp::write_integer(out, output.int),
            _ => resp::write_integer(out, -2),
        }
    }

    fn cmd_persist(&mut self, raw: &[u8], key: &[u8], out: &mut Vec<u8>) {
        let input = StringInput {
            op: StringOp::Persist,
            operand: Bytes::empty(),
            delta: 0,
            expiration: 0,
            now_ms: self.now_ms,
        };
        let output = &mut self.out_scratch;
        let status = self.store.rmw(key, &input, output);
        let removed = status.is_updated() && output.has_int && output.int == 1;

        resp::write_integer(out, removed as i64);
        if removed {
            self.after_write(key, raw);
        }
    }

    fn cmd_type(&mut self, key: &[u8], out: &mut Vec<u8>) {
        let input = StringInput::read(StringOp::Exists, self.now_ms);
        let output = &mut self.out_scratch;
        let status = self.store.read(key, &input, output);

        if status == Status::Found {
            resp::write_simple(out, "string");
        } else {
            resp::write_simple(out, "none");
        }
    }

    fn cmd_keys(&mut self, pattern: &[u8], out: &mut Vec<u8>) {
        let mut keys: Vec<Vec<u8>> = Vec::new();
        self.shared.store.scan_live(|_, rec| {
            if glob_match(pattern, rec.key()) {
                keys.push(rec.key().to_vec());
            }
        });
        resp::write_array_header(out, keys.len());
        for k in keys {
            resp::write_bulk(out, &k);
        }
    }

    fn cmd_scan(&mut self, args: &[&[u8]], out: &mut Vec<u8>) {
        // Mini SCAN: ignore the cursor, return everything (matching MATCH) with
        // a next-cursor of 0. Real Garnet resumes by log address.
        let mut pattern: &[u8] = b"*";
        let (options, _) = args[2..].as_chunks::<2>();
        for &[name, value] in options {
            if name.eq_ignore_ascii_case(b"MATCH") {
                pattern = value;
            }
        }
        resp::write_array_header(out, 2);
        resp::write_bulk(out, b"0");
        self.cmd_keys(pattern, out);
    }

    // -- transactions ------------------------------------------------------

    fn cmd_multi(&mut self, out: &mut Vec<u8>) {
        if self.txn.in_multi {
            resp::write_error(out, "ERR MULTI calls can not be nested");
            return;
        }
        self.txn.in_multi = true;
        self.txn.dirty = false;
        self.txn.queued.clear();
        out.extend_from_slice(resp::OK);
    }

    fn discard(&mut self) {
        self.txn.in_multi = false;
        self.txn.dirty = false;
        self.txn.queued.clear();
        self.txn.watches.clear();
    }

    fn cmd_watch(&mut self, args: &[&[u8]], out: &mut Vec<u8>) {
        if self.txn.in_multi {
            resp::write_error(out, "ERR WATCH inside MULTI is not allowed");
            return;
        }
        for &key in &args[1..] {
            let (slot, ver) = self.shared.watch.slot_version(key);
            self.txn.watches.push((slot, ver));
        }
        out.extend_from_slice(resp::OK);
    }

    fn cmd_exec(&mut self, out: &mut Vec<u8>) -> Action {
        if !self.txn.in_multi {
            resp::write_error(out, "ERR EXEC without MULTI");
            return Action::Continue;
        }
        if self.txn.dirty {
            self.discard();
            resp::write_error(
                out,
                "EXECABORT Transaction discarded because of previous errors.",
            );
            return Action::Continue;
        }
        // Optimistic watch check.
        let conflict = self
            .txn
            .watches
            .iter()
            .any(|(slot, ver)| self.shared.watch.version_at(*slot) != *ver);
        if conflict {
            self.discard();
            out.extend_from_slice(resp::NULL_ARRAY);
            return Action::Continue;
        }

        let queued = std::mem::take(&mut self.txn.queued);
        self.txn.in_multi = false;
        self.txn.watches.clear();

        // Collect the write/read set for 2PL.
        let mut lock_keys: Vec<(Vec<u8>, LockType)> = Vec::new();
        for q in &queued {
            if let Some(args) = parse_ranges(q) {
                for (k, lt) in command_keys(q, &args) {
                    lock_keys.push((k, lt));
                }
            }
        }
        let key_refs: Vec<(&[u8], LockType)> =
            lock_keys.iter().map(|(k, t)| (k.as_slice(), *t)).collect();

        // Reply is an array of the sub-command results.
        resp::write_array_header(out, queued.len());
        if !self.replay {
            self.store.txn_lock(&key_refs);
        }
        if let Some(aof) = &self.shared.aof {
            if !self.replay {
                let _ = aof.enqueue(AofOp::TxnStart, self.now_ms, b"");
            }
        }
        for q in &queued {
            if let Some(args) = parse_ranges(q) {
                let refs: Vec<&[u8]> = args.iter().map(|&(s, e)| &q[s..e]).collect();
                let (name, n) = command_name(refs[0]);
                self.exec_command(&name[..n], q, &refs, out);
            } else {
                resp::write_error(out, "ERR bad queued command");
            }
        }
        if !self.replay {
            self.store.txn_unlock();
        }
        if let Some(aof) = &self.shared.aof {
            if !self.replay {
                let end = aof.enqueue(AofOp::TxnCommit, self.now_ms, b"").unwrap_or(0);
                if self.shared.aof_commit_wait {
                    let _ = aof.wait_committed(end);
                }
            }
        }
        Action::Continue
    }

    // -- persistence -------------------------------------------------------

    fn cmd_save(&mut self, out: &mut Vec<u8>) {
        match self.shared.checkpoint() {
            Ok(()) => out.extend_from_slice(resp::OK),
            Err(e) => resp::write_error(out, &format!("ERR checkpoint failed: {e}")),
        }
    }

    // -- replay support (used by AOF recovery) -----------------------------

    /// Dispatch a command during replay with the logged timestamp.
    pub fn dispatch_replay(&mut self, raw: &[u8], args: &[&[u8]], ts: i64, out: &mut Vec<u8>) {
        self.now_ms = ts;
        let (name, n) = command_name(args.first().copied().unwrap_or(b""));
        self.exec_command(&name[..n], raw, args, out);
    }

    pub fn replay_multi_begin(&mut self, ts: i64) {
        self.now_ms = ts;
        self.txn.in_multi = true;
        self.txn.dirty = false;
        self.txn.queued.clear();
    }

    pub fn replay_multi_exec(&mut self, out: &mut Vec<u8>) {
        let _ = self.cmd_exec(out);
    }

    fn cmd_hello(&mut self, out: &mut Vec<u8>) {
        // Minimal RESP2 HELLO reply (flat array of key/value pairs).
        resp::write_array_header(out, 6);
        resp::write_bulk(out, b"server");
        resp::write_bulk(out, b"mini-garnet");
        resp::write_bulk(out, b"version");
        resp::write_bulk(out, b"0.1.0");
        resp::write_bulk(out, b"proto");
        resp::write_integer(out, 2);
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

struct SetOpts {
    expiration: i64,
    nx: bool,
    xx: bool,
    get: bool,
    keep_ttl: bool,
}

/// `EX | PX | EXAT | PXAT`: the four ways RESP spells an expiry argument.
fn is_expiry_keyword(opt: &[u8]) -> bool {
    opt.eq_ignore_ascii_case(b"EX")
        || opt.eq_ignore_ascii_case(b"PX")
        || opt.eq_ignore_ascii_case(b"EXAT")
        || opt.eq_ignore_ascii_case(b"PXAT")
}

/// Convert an expiry argument to an absolute millisecond timestamp. Relative
/// (`EX`, `PX`) versus absolute (`EXAT`, `PXAT`) and seconds versus
/// milliseconds, in one place, so SET and GETEX cannot drift apart.
fn expire_at_ms(keyword: &[u8], t: i64, now_ms: i64) -> i64 {
    if keyword.eq_ignore_ascii_case(b"EX") {
        now_ms + t * 1000
    } else if keyword.eq_ignore_ascii_case(b"PX") {
        now_ms + t
    } else if keyword.eq_ignore_ascii_case(b"EXAT") {
        t * 1000
    } else {
        t
    }
}

fn parse_set_opts(args: &[&[u8]], now_ms: i64) -> Result<SetOpts, &'static str> {
    let mut o = SetOpts {
        expiration: 0,
        nx: false,
        xx: false,
        get: false,
        keep_ttl: false,
    };
    let mut i = 0;
    while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"NX") {
            o.nx = true;
        } else if opt.eq_ignore_ascii_case(b"XX") {
            o.xx = true;
        } else if opt.eq_ignore_ascii_case(b"GET") {
            o.get = true;
        } else if opt.eq_ignore_ascii_case(b"KEEPTTL") {
            o.keep_ttl = true;
        } else if is_expiry_keyword(opt) {
            if i + 1 >= args.len() {
                return Err("ERR syntax error");
            }
            let t = parse_i64(args[i + 1]).ok_or("ERR value is not an integer or out of range")?;
            o.expiration = expire_at_ms(opt, t, now_ms);
            i += 1;
        } else {
            return Err("ERR syntax error");
        }
        i += 1;
    }
    if o.nx && o.xx {
        return Err("ERR syntax error");
    }
    Ok(o)
}

/// Longest command name we recognize. Names are normalized into a stack buffer
/// of this size so dispatch never allocates.
const MAX_CMD_NAME: usize = 24;

/// Uppercase a command name into a stack buffer. One spelling of the rule,
/// shared by live dispatch, EXEC replay and AOF replay.
fn command_name(arg: &[u8]) -> ([u8; MAX_CMD_NAME], usize) {
    let mut name = [0u8; MAX_CMD_NAME];
    let n = arg.len().min(MAX_CMD_NAME);
    name[..n].copy_from_slice(&arg[..n]);
    name[..n].make_ascii_uppercase();
    (name, n)
}

/// Return the arg byte ranges of a RESP command stored as raw bytes.
fn parse_ranges(raw: &[u8]) -> Option<Vec<(usize, usize)>> {
    let mut slices = Vec::new();
    resp::parse_command(raw, 0, &mut slices).ok()??;
    Some(slices.iter().map(|a| (a.start, a.end)).collect())
}

/// Keys touched by a command and their lock type (for 2PL in EXEC).
fn command_keys(raw: &[u8], ranges: &[(usize, usize)]) -> Vec<(Vec<u8>, LockType)> {
    if ranges.is_empty() {
        return Vec::new();
    }
    // Every argument from `ranges[from..]`, stepped by `step`, at `lock`.
    let keys = |from: usize, step: usize, lock| -> Vec<(Vec<u8>, LockType)> {
        ranges[from.min(ranges.len())..]
            .iter()
            .step_by(step)
            .map(|&(s, e)| (raw[s..e].to_vec(), lock))
            .collect()
    };
    let (name, n) = command_name(&raw[ranges[0].0..ranges[0].1]);
    let ex = LockType::Exclusive;
    let sh = LockType::Shared;
    match &name[..n] {
        // Read-only, every argument is a key.
        b"GET" | b"STRLEN" | b"GETRANGE" | b"SUBSTR" | b"TTL" | b"PTTL" | b"EXISTS" | b"TYPE"
        | b"MGET" => keys(1, 1, sh),
        // Writes, every argument is a key.
        b"DEL" | b"UNLINK" => keys(1, 1, ex),
        // Writes, every *other* argument is a key.
        b"MSET" => keys(1, 2, ex),
        // Everything else writes its first argument.
        _ if ranges.len() >= 2 => keys(1, ranges.len(), ex),
        _ => Vec::new(),
    }
}

/// Commands that may not be queued in a MULTI (returns an error string).
fn validate_queueable(cmd: &[u8]) -> Option<&'static str> {
    match cmd {
        b"SUBSCRIBE" | b"UNSUBSCRIBE" | b"PSUBSCRIBE" => {
            Some("ERR command not allowed in transaction")
        }
        _ => None,
    }
}

/// Redis-style glob matching: `*`, `?`, `[set]`, `\` escape.
pub fn glob_match(pat: &[u8], s: &[u8]) -> bool {
    fn helper(p: &[u8], s: &[u8]) -> bool {
        let mut pi = 0;
        let mut si = 0;
        let mut star: Option<(usize, usize)> = None;
        while si < s.len() {
            if pi < p.len() {
                match p[pi] {
                    b'*' => {
                        star = Some((pi, si));
                        pi += 1;
                        continue;
                    }
                    b'?' => {
                        pi += 1;
                        si += 1;
                        continue;
                    }
                    b'[' => {
                        // find matching ]
                        let mut j = pi + 1;
                        let negate = j < p.len() && (p[j] == b'^' || p[j] == b'!');
                        if negate {
                            j += 1;
                        }
                        let set_start = j;
                        while j < p.len() && p[j] != b']' {
                            j += 1;
                        }
                        let set = &p[set_start..j.min(p.len())];
                        let matched = in_set(set, s[si]);
                        if matched ^ negate {
                            pi = j + 1;
                            si += 1;
                            continue;
                        }
                    }
                    b'\\' if pi + 1 < p.len() => {
                        if p[pi + 1] == s[si] {
                            pi += 2;
                            si += 1;
                            continue;
                        }
                    }
                    c => {
                        if c == s[si] {
                            pi += 1;
                            si += 1;
                            continue;
                        }
                    }
                }
            }
            if let Some((sp, ss)) = star {
                pi = sp + 1;
                si = ss + 1;
                star = Some((sp, ss + 1));
            } else {
                return false;
            }
        }
        while pi < p.len() && p[pi] == b'*' {
            pi += 1;
        }
        pi == p.len()
    }
    fn in_set(set: &[u8], c: u8) -> bool {
        let mut i = 0;
        while i < set.len() {
            if i + 2 < set.len() && set[i + 1] == b'-' {
                if set[i] <= c && c <= set[i + 2] {
                    return true;
                }
                i += 3;
            } else {
                if set[i] == c {
                    return true;
                }
                i += 1;
            }
        }
        false
    }
    helper(pat, s)
}
