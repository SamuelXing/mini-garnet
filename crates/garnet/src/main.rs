//! mini-garnet binary entry point.
//!
//! ```text
//!   RESP clients ──TCP──► server (one thread/conn, batch parse + one flush)
//!                              │  RUMDS
//!                       commands (dispatch) ── functions (record logic)
//!                              │
//!                         Tsavorite Store ── AOF (DOL) ── checkpoint
//! ```

use garnet::aof::CommitMode;
use garnet::shared::{Config, Shared};

fn parse_args() -> Config {
    let mut cfg = Config::default();
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--bind" => cfg.bind = it.next().unwrap_or(cfg.bind),
            "--port" => {
                if let Some(p) = it.next() {
                    cfg.bind = format!("127.0.0.1:{p}");
                }
            }
            "--dir" => {
                if let Some(d) = it.next() {
                    cfg.dir = d.into();
                }
            }
            "--maxclients" => {
                if let Some(n) = it.next().and_then(|n| n.parse().ok()) {
                    cfg.maxclients = n;
                }
            }
            "--persist" => cfg.persist = true,
            "--aof" => cfg.aof = true,
            "--aof-commit-wait" => {
                cfg.aof = true;
                cfg.aof_commit_wait = true;
                cfg.commit_mode = CommitMode::Always;
            }
            "--help" | "-h" => {
                eprintln!("mini-garnet [--port N] [--bind ADDR] [--dir PATH] [--maxclients N] [--persist] [--aof] [--aof-commit-wait]");
                std::process::exit(0);
            }
            other => eprintln!("ignoring unknown flag: {other}"),
        }
    }
    cfg
}

fn main() -> std::io::Result<()> {
    let cfg = parse_args();
    let shared = Shared::new(cfg)?;
    shared.recover()?;
    garnet::server::serve(shared)
}
