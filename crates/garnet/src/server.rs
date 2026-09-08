//! The network layer (paper §3.1): one thread per connection, a single
//! receive buffer handed straight to the parser (no thread hop), a batch of
//! pipelined commands processed in place, and one flush of the send buffer per
//! batch (`GarnetServerTcp`, `TcpNetworkHandlerBase`, `RespServerSession`).
//!
//! Partial commands are handled exactly as Garnet does: `TryConsumeMessages`
//! consumes whole commands only; the leftover tail is compacted to the front
//! of the receive buffer (`ShiftNetworkReceiveBuffer`) and the buffer is grown
//! if a single command does not fit (`DoubleNetworkReceiveBuffer`).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use crate::commands::{Action, RespSession};
use crate::resp;
use crate::shared::Shared;

const INITIAL_RECV: usize = 64 * 1024;
const MAX_RECV: usize = 512 * 1024 * 1024;

pub fn serve(shared: Arc<Shared>) -> std::io::Result<()> {
    let listener = TcpListener::bind(&shared.config.bind)?;
    eprintln!("mini-garnet listening on {}", listener.local_addr()?);
    serve_on(listener, shared)
}

/// Accept loop on an already-bound listener. Tests use this to bind an
/// ephemeral port and learn its number before serving.
pub fn serve_on(listener: TcpListener, shared: Arc<Shared>) -> std::io::Result<()> {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let shared = shared.clone();
                // One thread per connection. Everything for a batch — parse,
                // storage ops, reply formatting — happens on this thread, with
                // no handoff to a worker (paper §3.1).
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, shared) {
                        if e.kind() != std::io::ErrorKind::UnexpectedEof {
                            eprintln!("connection error: {e}");
                        }
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}

pub fn handle_connection(mut stream: TcpStream, shared: Arc<Shared>) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    let mut session = RespSession::new(shared);

    let mut recv = vec![0u8; INITIAL_RECV];
    let mut bytes_read = 0usize; // valid bytes in recv
    let mut send = Vec::with_capacity(INITIAL_RECV);

    loop {
        // Grow if the buffer is full (a single command larger than the buffer).
        if bytes_read == recv.len() {
            if recv.len() >= MAX_RECV {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "command too large",
                ));
            }
            recv.resize((recv.len() * 2).min(MAX_RECV), 0);
        }
        let n = stream.read(&mut recv[bytes_read..])?;
        if n == 0 {
            return Ok(());
        }
        bytes_read += n;

        // Process every complete command in the batch. The two scratch
        // buffers below are allocated once per batch, not once per command:
        // with pipelining a batch can hold hundreds of commands, which is the
        // whole reason the protocol is worth batching.
        send.clear();
        let mut read_head = 0usize;
        let mut close = false;
        {
            let buf = &recv[..bytes_read];
            let mut slices: Vec<resp::ArgSlice> = Vec::new();
            let mut argv: Vec<&[u8]> = Vec::new();
            loop {
                match resp::parse_command(buf, read_head, &mut slices) {
                    Ok(Some(next)) => {
                        // `raw` (the whole command) and `argv` (its fields) are
                        // both read-only borrows into the receive buffer, handed
                        // straight to dispatch with no copy (paper §3.1).
                        argv.clear();
                        argv.extend(slices.iter().map(|a| a.bytes(buf)));
                        let action = session.dispatch(&buf[read_head..next], &argv, &mut send);
                        read_head = next;
                        if matches!(action, Action::Close) {
                            close = true;
                            break;
                        }
                    }
                    Ok(None) => break, // incomplete: wait for more bytes
                    Err(_) => {
                        resp::write_error(&mut send, "ERR Protocol error");
                        close = true;
                        break;
                    }
                }
            }
        }

        // One flush per batch.
        if !send.is_empty() {
            stream.write_all(&send)?;
        }
        if close {
            return Ok(());
        }

        // Compact the unconsumed tail to the front (ShiftNetworkReceiveBuffer).
        if read_head > 0 {
            recv.copy_within(read_head..bytes_read, 0);
            bytes_read -= read_head;
        }
    }
}
