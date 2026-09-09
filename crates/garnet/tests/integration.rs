//! End-to-end tests over a real TCP socket using a tiny RESP client, so no
//! external redis-cli is required.

mod common;
use common::{Client, Reply};

use std::net::TcpListener;

fn spawn_server(persist: bool, aof: bool) -> (String, std::path::PathBuf) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let dir = std::env::temp_dir().join(format!(
        "mini-garnet-it-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    let cfg = garnet::shared::Config {
        bind: addr.clone(),
        dir: dir.clone(),
        persist,
        aof,
        aof_commit_wait: aof,
        commit_mode: if aof {
            garnet::aof::CommitMode::Always
        } else {
            garnet::aof::CommitMode::Never
        },
        store: garnet::store_settings_small(),
        maxclients: garnet::server::DEFAULT_MAX_CLIENTS,
    };
    let shared = garnet::shared::Shared::new(cfg).unwrap();
    shared.recover().unwrap();
    std::thread::spawn(move || {
        garnet::server::serve_on(listener, shared).unwrap();
    });
    (addr, dir)
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn persist_cfg(dir: &std::path::Path) -> garnet::shared::Config {
    garnet::shared::Config {
        bind: "127.0.0.1:0".into(),
        dir: dir.to_path_buf(),
        persist: true,
        aof: true,
        aof_commit_wait: true,
        commit_mode: garnet::aof::CommitMode::Always,
        store: garnet::store_settings_small(),
        maxclients: garnet::server::DEFAULT_MAX_CLIENTS,
    }
}

#[test]
fn strings_and_numbers() {
    let (addr, _dir) = spawn_server(false, false);
    let mut c = Client::connect(&addr);
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    assert_eq!(c.cmd(&["SET", "k", "hello"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["GET", "k"]).as_str(), "hello");
    assert_eq!(c.cmd(&["APPEND", "k", " world"]), Reply::Int(11));
    assert_eq!(c.cmd(&["GET", "k"]).as_str(), "hello world");
    assert_eq!(c.cmd(&["STRLEN", "k"]), Reply::Int(11));
    assert_eq!(c.cmd(&["GETRANGE", "k", "0", "4"]).as_str(), "hello");
    assert_eq!(c.cmd(&["GETRANGE", "k", "-5", "-1"]).as_str(), "world");

    assert_eq!(c.cmd(&["SET", "n", "100"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["INCR", "n"]), Reply::Int(101));
    assert_eq!(c.cmd(&["INCRBY", "n", "9"]), Reply::Int(110));
    assert_eq!(c.cmd(&["DECRBY", "n", "20"]), Reply::Int(90));
    assert_eq!(c.cmd(&["DECR", "n"]), Reply::Int(89));
    // Non-integer -> error.
    assert!(matches!(c.cmd(&["INCR", "k"]), Reply::Error(_)));

    // GET on missing key.
    assert_eq!(c.cmd(&["GET", "missing"]), Reply::Bulk(None));
    // SETNX / SET NX / XX.
    assert_eq!(c.cmd(&["SETNX", "k", "x"]), Reply::Int(0));
    assert_eq!(c.cmd(&["SETNX", "fresh", "x"]), Reply::Int(1));
    assert_eq!(c.cmd(&["SET", "k", "v2", "XX"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["SET", "brand", "v", "XX"]), Reply::Bulk(None));
    assert_eq!(c.cmd(&["SET", "k", "v3", "NX"]), Reply::Bulk(None));
    // SET GET returns old value.
    assert_eq!(c.cmd(&["SET", "k", "v4", "GET"]).as_str(), "v2");

    // DEL / EXISTS / TYPE.
    assert_eq!(c.cmd(&["EXISTS", "k", "n", "nope"]), Reply::Int(2));
    assert_eq!(c.cmd(&["TYPE", "k"]), Reply::Simple("string".into()));
    assert_eq!(c.cmd(&["TYPE", "nope"]), Reply::Simple("none".into()));
    assert_eq!(c.cmd(&["DEL", "k", "n", "nope"]), Reply::Int(2));
    assert_eq!(c.cmd(&["EXISTS", "k"]), Reply::Int(0));

    // MSET / MGET.
    assert_eq!(
        c.cmd(&["MSET", "a", "1", "b", "2", "cc", "3"]),
        Reply::Simple("OK".into())
    );
    match c.cmd(&["MGET", "a", "b", "zzz", "cc"]) {
        Reply::Array(Some(v)) => {
            assert_eq!(v[0].as_str(), "1");
            assert_eq!(v[1].as_str(), "2");
            assert_eq!(v[2], Reply::Bulk(None));
            assert_eq!(v[3].as_str(), "3");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn string_updates_work_in_place_and_when_values_grow() {
    let (addr, _dir) = spawn_server(false, false);
    let mut c = Client::connect(&addr);

    // Shrinking leaves spare capacity, so the first range update happens in place.
    c.cmd(&["SET", "range", "abcdefgh"]);
    c.cmd(&["SET", "range", "a"]);
    assert_eq!(c.cmd(&["SETRANGE", "range", "3", "z"]), Reply::Int(4));
    assert_eq!(c.cmd(&["GET", "range"]).as_str(), "a\0\0z");

    // Growing past that capacity copies the value and zero-fills the gap.
    assert_eq!(c.cmd(&["SETRANGE", "range", "10", "!"]), Reply::Int(11));
    assert_eq!(c.cmd(&["APPEND", "range", "?"]), Reply::Int(12));
    assert_eq!(c.cmd(&["GET", "range"]).as_str(), "a\0\0z\0\0\0\0\0\0!?");
    assert_eq!(c.cmd(&["SETRANGE", "new-range", "2", "x"]), Reply::Int(3));
    assert_eq!(c.cmd(&["GET", "new-range"]).as_str(), "\0\0x");

    // Adding an expiration slot copies an existing record; later updates retain it.
    c.cmd(&["SET", "ttl", "one"]);
    assert_eq!(
        c.cmd(&["GETEX", "ttl", "PXAT", "4102444800000"]).as_str(),
        "one"
    );
    assert_eq!(
        c.cmd(&["SET", "ttl", "two", "GET", "KEEPTTL"]).as_str(),
        "one"
    );
    assert_eq!(
        c.cmd(&["SET", "ttl", "longer value", "GET", "KEEPTTL"])
            .as_str(),
        "two"
    );
    assert!(matches!(c.cmd(&["PTTL", "ttl"]), Reply::Int(ttl) if ttl > 0));
    assert_eq!(c.cmd(&["GETEX", "ttl", "PERSIST"]).as_str(), "longer value");
    assert_eq!(c.cmd(&["PTTL", "ttl"]), Reply::Int(-1));

    // A reused reply buffer must still distinguish missing values and valid MIN integers.
    assert_eq!(c.cmd(&["SET", "fresh", "value", "GET"]), Reply::Bulk(None));
    assert_eq!(c.cmd(&["GETDEL", "fresh"]).as_str(), "value");
    assert_eq!(c.cmd(&["GETDEL", "fresh"]), Reply::Bulk(None));
    assert_eq!(
        c.cmd(&["INCRBY", "minimum", "-9223372036854775808"]),
        Reply::Int(i64::MIN)
    );
    assert_eq!(c.cmd(&["INCRBY", "minimum", "0"]), Reply::Int(i64::MIN));
    assert!(matches!(c.cmd(&["DECR", "minimum"]), Reply::Error(_)));
    assert_eq!(c.cmd(&["GET", "minimum"]).as_str(), "-9223372036854775808");
}

#[test]
fn scan_returns_the_same_keys_as_keys() {
    let (addr, _dir) = spawn_server(false, false);
    let mut c = Client::connect(&addr);
    c.cmd(&["MSET", "user:1", "a", "user:2", "b", "post:1", "c"]);

    for pattern in ["*", "user:*", "user:?", "user:[12]", "missing:*"] {
        let keys = c.cmd(&["KEYS", pattern]);
        let scan = c.cmd(&["SCAN", "0", "mAtCh", pattern, "COUNT", "1"]);
        assert_eq!(
            scan,
            Reply::Array(Some(vec![Reply::Bulk(Some(b"0".to_vec())), keys]))
        );
    }

    let keys = c.cmd(&["KEYS", "*"]);
    assert_eq!(
        c.cmd(&["SCAN", "0"]),
        Reply::Array(Some(vec![Reply::Bulk(Some(b"0".to_vec())), keys]))
    );
}

#[test]
fn expiration_is_lazy_and_ttl_reported() {
    let (addr, _dir) = spawn_server(false, false);
    let mut c = Client::connect(&addr);
    assert_eq!(
        c.cmd(&["SET", "e", "v", "PX", "50"]),
        Reply::Simple("OK".into())
    );
    let ttl = c.cmd(&["PTTL", "e"]);
    match ttl {
        Reply::Int(v) => assert!(v > 0 && v <= 50, "pttl={v}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(c.cmd(&["GET", "e"]).as_str(), "v");
    std::thread::sleep(std::time::Duration::from_millis(70));
    assert_eq!(c.cmd(&["GET", "e"]), Reply::Bulk(None), "expired");
    assert_eq!(c.cmd(&["TTL", "e"]), Reply::Int(-2));
    // EXPIRE / PERSIST on a live key.
    c.cmd(&["SET", "p", "v"]);
    assert_eq!(c.cmd(&["TTL", "p"]), Reply::Int(-1));
    assert_eq!(c.cmd(&["EXPIRE", "p", "1000"]), Reply::Int(1));
    match c.cmd(&["TTL", "p"]) {
        Reply::Int(v) => assert!(v > 990 && v <= 1000),
        other => panic!("{other:?}"),
    }
    assert_eq!(c.cmd(&["PERSIST", "p"]), Reply::Int(1));
    assert_eq!(c.cmd(&["TTL", "p"]), Reply::Int(-1));
    assert_eq!(c.cmd(&["EXPIRE", "missing", "10"]), Reply::Int(0));
}

#[test]
fn pipelining_one_flush() {
    let (addr, _dir) = spawn_server(false, false);
    let mut c = Client::connect(&addr);
    let cmds: Vec<Vec<&str>> = (0..100)
        .map(|_| vec!["SET", "k", "v"])
        .chain(std::iter::once(vec!["GET", "k"]))
        .collect();
    let refs: Vec<&[&str]> = cmds.iter().map(|v| v.as_slice()).collect();
    let replies = c.pipeline(&refs);
    assert_eq!(replies.len(), 101);
    assert_eq!(replies[100].as_str(), "v");
    for r in &replies[..100] {
        assert_eq!(*r, Reply::Simple("OK".into()));
    }
}

/// One thread per connection means a connection costs an epoch-table slot, and
/// that table has a fixed size. Past the ceiling the server must refuse at the
/// door with an error the client can read — not accept and then panic a thread
/// inside the engine. Closing a connection has to give the slot back.
#[test]
fn connections_past_maxclients_are_refused_and_the_slot_is_reusable() {
    use std::io::Read;
    use std::net::TcpStream;

    const LIMIT: usize = 4;
    const REFUSAL: &[u8] = b"-ERR max number of clients reached\r\n";

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let cfg = garnet::shared::Config {
        bind: addr.clone(),
        maxclients: LIMIT,
        ..garnet::shared::Config::default()
    };
    let shared = garnet::shared::Shared::new(cfg).unwrap();
    std::thread::spawn(move || garnet::server::serve_on(listener, shared).unwrap());

    // A refused connection is told so immediately and then closed; an accepted
    // one stays silent until it is asked something, so a short read separates
    // the two without writing to a socket the server may already have dropped.
    let probe_is_refused = |s: &TcpStream| {
        s.set_read_timeout(Some(std::time::Duration::from_millis(250)))
            .unwrap();
        let mut buf = [0u8; 64];
        let mut r = s; // `&TcpStream` reads without needing ownership
        let n = r.read(&mut buf).unwrap_or(0);
        assert!(n == 0 || buf[..n] == *REFUSAL, "unexpected banner");
        n > 0
    };

    // Fill every slot. A completed round trip proves the server has accepted
    // and counted the connection, so the next connect races nothing.
    let mut held: Vec<Client> = (0..LIMIT)
        .map(|_| {
            let mut c = Client::connect(&addr);
            assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
            c
        })
        .collect();

    let over = TcpStream::connect(&addr).unwrap();
    assert!(probe_is_refused(&over), "connection past maxclients");

    // Closing one frees its slot. The server thread has to notice EOF first.
    held.pop();
    let mut readmitted = None;
    for _ in 0..100 {
        let s = TcpStream::connect(&addr).unwrap();
        if !probe_is_refused(&s) {
            readmitted = Some(s);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        readmitted.is_some(),
        "a closed connection must give its slot back"
    );
}

#[test]
fn keys_and_dbsize() {
    let (addr, _dir) = spawn_server(false, false);
    let mut c = Client::connect(&addr);
    c.cmd(&["MSET", "user:1", "a", "user:2", "b", "post:1", "c"]);
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(3));
    match c.cmd(&["KEYS", "user:*"]) {
        Reply::Array(Some(v)) => {
            let mut got: Vec<String> = v.iter().map(|r| r.as_str()).collect();
            got.sort();
            assert_eq!(got, vec!["user:1".to_string(), "user:2".to_string()]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn transactions_multi_exec_and_watch() {
    let (addr, _dir) = spawn_server(false, false);
    let mut c = Client::connect(&addr);
    c.cmd(&["SET", "src", "10"]);
    c.cmd(&["SET", "dst", "0"]);
    assert_eq!(c.cmd(&["MULTI"]), Reply::Simple("OK".into()));
    assert_eq!(
        c.cmd(&["DECRBY", "src", "3"]),
        Reply::Simple("QUEUED".into())
    );
    assert_eq!(
        c.cmd(&["INCRBY", "dst", "3"]),
        Reply::Simple("QUEUED".into())
    );
    match c.cmd(&["EXEC"]) {
        Reply::Array(Some(v)) => {
            assert_eq!(v[0], Reply::Int(7));
            assert_eq!(v[1], Reply::Int(3));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(c.cmd(&["GET", "src"]).as_str(), "7");
    assert_eq!(c.cmd(&["GET", "dst"]).as_str(), "3");

    // WATCH abort: a second client modifies the watched key.
    assert_eq!(c.cmd(&["WATCH", "src"]), Reply::Simple("OK".into()));
    let mut c2 = Client::connect(&addr);
    c2.cmd(&["SET", "src", "999"]);
    c.cmd(&["MULTI"]);
    c.cmd(&["INCR", "src"]);
    assert_eq!(
        c.cmd(&["EXEC"]),
        Reply::Array(None),
        "aborted by WATCH conflict"
    );
    assert_eq!(c.cmd(&["GET", "src"]).as_str(), "999");

    // DISCARD.
    c.cmd(&["MULTI"]);
    c.cmd(&["SET", "x", "1"]);
    assert_eq!(c.cmd(&["DISCARD"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["EXISTS", "x"]), Reply::Int(0));
}

#[test]
fn large_dataset_spills_to_disk() {
    let (addr, _dir) = spawn_server(true, false);
    let mut c = Client::connect(&addr);
    // Write enough to evict pages to disk, then read back.
    for i in 0..5000 {
        let k = format!("key{i}");
        let v = format!("value-number-{i}");
        assert_eq!(c.cmd(&["SET", &k, &v]), Reply::Simple("OK".into()));
    }
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(5000));
    for i in (0..5000).step_by(311) {
        let k = format!("key{i}");
        assert_eq!(c.cmd(&["GET", &k]).as_str(), format!("value-number-{i}"));
    }
    // Update a cold key (read-copy-update from disk).
    assert_eq!(c.cmd(&["APPEND", "key0", "!"]), Reply::Int(15));
    assert_eq!(c.cmd(&["GET", "key0"]).as_str(), "value-number-0!");
}

/// A record drifts down the log as unrelated traffic arrives: mutable tail →
/// immutable in memory → disk. INCR owes the same answer in all three, so the
/// non-integer and overflow guards cannot live only in `in_place_updater` —
/// the copy paths need them too, or the reply (and the stored value) would
/// depend on how much traffic has passed since the key was written.
#[test]
fn incr_guards_hold_in_every_log_region() {
    fn reply(s: &mut garnet::commands::RespSession, parts: &[&[u8]]) -> Reply {
        common::parse_reply(&s.run(parts))
    }

    /// The answers INCR owes for one region's key set.
    fn assert_answers(s: &mut garnet::commands::RespSession, suffix: &str, region: &str) {
        let (word, num, big) = (
            format!("word{suffix}"),
            format!("num{suffix}"),
            format!("big{suffix}"),
        );
        assert!(
            matches!(reply(s, &[b"INCR", word.as_bytes()]), Reply::Error(_)),
            "INCR on a non-integer must fail ({region})"
        );
        assert_eq!(
            reply(s, &[b"GET", word.as_bytes()]).as_str(),
            "abc",
            "a rejected INCR must leave the value alone ({region})"
        );
        assert!(
            matches!(reply(s, &[b"INCR", big.as_bytes()]), Reply::Error(_)),
            "INCR past i64::MAX must fail ({region})"
        );
        assert_eq!(
            reply(s, &[b"GET", big.as_bytes()]).as_str(),
            "9223372036854775807",
            "a rejected INCR must leave the value alone ({region})"
        );
        assert_eq!(
            reply(s, &[b"INCR", num.as_bytes()]),
            Reply::Int(42),
            "a valid INCR must still work ({region})"
        );
    }

    fn address_of(shared: &garnet::shared::Shared, key: &[u8]) -> u64 {
        shared
            .store
            .index
            .find_tag(tsavorite::hash::hash64(key))
            .expect("key is indexed")
            .load()
            .address()
    }

    /// Append unrelated keys until a watermark has moved past the record.
    fn fill_until(
        s: &mut garnet::commands::RespSession,
        tag: &str,
        mut done: impl FnMut() -> bool,
    ) {
        for i in 0..200_000u32 {
            if done() {
                return;
            }
            let k = format!("filler-{tag}-{i}");
            s.run(&[b"SET", k.as_bytes(), b"0123456789abcdef"]);
        }
        panic!("watermark never advanced past the record");
    }

    let cfg = garnet::shared::Config {
        bind: "127.0.0.1:0".into(),
        dir: std::env::temp_dir().join("mini-garnet-incr-regions"),
        persist: false,
        aof: false,
        aof_commit_wait: false,
        commit_mode: garnet::aof::CommitMode::Never,
        // Small pages, so a few hundred filler writes move the watermarks.
        store: tsavorite::store::StoreSettings {
            index_buckets: 1 << 12,
            log: tsavorite::log::LogSettings {
                page_bits: 12,
                memory_pages: 8,
                mutable_fraction: 0.5,
            },
        },
        maxclients: garnet::server::DEFAULT_MAX_CLIENTS,
    };
    let shared = garnet::shared::Shared::new(cfg).unwrap();
    let mut s = garnet::commands::RespSession::new(shared.clone());

    // One key set per region, all written up front so each set stays together.
    for suffix in ["", "-ro", "-disk"] {
        s.run(&[b"SET", format!("word{suffix}").as_bytes(), b"abc"]);
        s.run(&[b"SET", format!("num{suffix}").as_bytes(), b"41"]);
        s.run(&[
            b"SET",
            format!("big{suffix}").as_bytes(),
            b"9223372036854775807",
        ]);
    }

    // 1. Mutable tail: `in_place_updater` decides.
    assert_answers(&mut s, "", "mutable");

    // 2. Below ReadOnly: the record is immutable, so RMW copies to the tail
    //    and `need_copy_update` decides.
    let ro_target = ["word-ro", "num-ro", "big-ro"]
        .iter()
        .map(|k| address_of(&shared, k.as_bytes()))
        .max()
        .unwrap();
    fill_until(&mut s, "ro", || {
        shared.store.log.read_only_address() > ro_target
    });
    assert_answers(&mut s, "-ro", "immutable");

    // 3. Below Head: the record has been evicted, so RMW reads it back from
    //    the device and copy-updates from the disk image.
    let disk_target = ["word-disk", "num-disk", "big-disk"]
        .iter()
        .map(|k| address_of(&shared, k.as_bytes()))
        .max()
        .unwrap();
    fill_until(&mut s, "disk", || {
        shared.store.log.head_address() > disk_target
    });
    assert_answers(&mut s, "-disk", "on disk");
}

#[test]
fn aof_recovers_state_after_restart() {
    let dir = std::env::temp_dir().join(format!(
        "mini-garnet-aof-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    // Instance 1: drive in-process, then drop everything (a clean shutdown).
    {
        let cfg = persist_cfg(&dir);
        let shared = garnet::shared::Shared::new(cfg).unwrap();
        shared.recover().unwrap();
        let mut s = garnet::commands::RespSession::new(shared.clone());
        s.run(&[b"SET", b"a", b"1"]);
        s.run(&[b"INCRBY", b"a", b"41"]);
        s.run(&[b"SET", b"b", b"hello"]);
        s.run(&[b"MULTI"]);
        s.run(&[b"SET", b"c", b"in-txn"]);
        s.run(&[b"INCR", b"a"]);
        s.run(&[b"EXEC"]);
        s.run(&[b"DEL", b"b"]);
        assert_eq!(common::parse_reply(&s.run(&[b"GET", b"a"])).as_str(), "43");
    }
    // Instance 2: recover from AOF replay (no checkpoint).
    let cfg = persist_cfg(&dir);
    let shared = garnet::shared::Shared::new(cfg).unwrap();
    shared.recover().unwrap();
    let mut s = garnet::commands::RespSession::new(shared.clone());
    assert_eq!(
        common::parse_reply(&s.run(&[b"GET", b"a"])).as_str(),
        "43",
        "INCR replayed deterministically"
    );
    assert_eq!(
        common::parse_reply(&s.run(&[b"GET", b"c"])).as_str(),
        "in-txn",
        "txn replayed"
    );
    assert_eq!(
        common::parse_reply(&s.run(&[b"EXISTS", b"b"])),
        Reply::Int(0),
        "DEL replayed"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn checkpoint_then_recover() {
    let dir = std::env::temp_dir().join(format!(
        "mini-garnet-ckpt-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    {
        let cfg = persist_cfg(&dir);
        let shared = garnet::shared::Shared::new(cfg).unwrap();
        shared.recover().unwrap();
        let mut s = garnet::commands::RespSession::new(shared.clone());
        for i in 0..1000 {
            s.run(&[
                b"SET",
                format!("k{i}").as_bytes(),
                format!("v{i}").as_bytes(),
            ]);
        }
        s.run(&[b"DEL", b"k5"]);
        assert_eq!(
            common::parse_reply(&s.run(&[b"SAVE"])),
            Reply::Simple("OK".into())
        );
        s.run(&[b"SET", b"after", b"1"]); // post-checkpoint write -> new AOF
    }
    let cfg = persist_cfg(&dir);
    let shared = garnet::shared::Shared::new(cfg).unwrap();
    shared.recover().unwrap();
    let mut s = garnet::commands::RespSession::new(shared.clone());
    assert_eq!(common::parse_reply(&s.run(&[b"GET", b"k0"])).as_str(), "v0");
    assert_eq!(
        common::parse_reply(&s.run(&[b"GET", b"k999"])).as_str(),
        "v999"
    );
    assert_eq!(
        common::parse_reply(&s.run(&[b"EXISTS", b"k5"])),
        Reply::Int(0)
    );
    assert_eq!(
        common::parse_reply(&s.run(&[b"GET", b"after"])).as_str(),
        "1",
        "post-checkpoint AOF replayed"
    );
    std::fs::remove_dir_all(&dir).ok();
}
