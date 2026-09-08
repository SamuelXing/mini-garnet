//! Minimal RESP client used by integration tests (redis-cli is not required).
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

impl Reply {
    pub fn as_str(&self) -> String {
        match self {
            Reply::Simple(s) | Reply::Error(s) => s.clone(),
            Reply::Int(i) => i.to_string(),
            Reply::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Reply::Bulk(None) => "(nil)".into(),
            Reply::Array(_) => format!("{self:?}"),
        }
    }
}

pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    pub fn connect(addr: &str) -> Client {
        let mut last = None;
        for _ in 0..50 {
            match TcpStream::connect(addr) {
                Ok(s) => {
                    s.set_nodelay(true).unwrap();
                    let writer = s.try_clone().unwrap();
                    return Client {
                        reader: BufReader::new(s),
                        writer,
                    };
                }
                Err(e) => {
                    last = Some(e);
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }
        }
        panic!("connect failed: {last:?}");
    }

    pub fn send(&mut self, args: &[&[u8]]) {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
    }

    pub fn read_reply(&mut self) -> Reply {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n").to_string();
        assert!(!line.is_empty(), "connection closed");
        let (prefix, rest) = line.split_at(1);
        match prefix {
            "+" => Reply::Simple(rest.to_string()),
            "-" => Reply::Error(rest.to_string()),
            ":" => Reply::Int(rest.parse().unwrap()),
            "$" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Reply::Bulk(None);
                }
                let mut data = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(n as usize);
                Reply::Bulk(Some(data))
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Reply::Array(None);
                }
                Reply::Array(Some((0..n).map(|_| self.read_reply()).collect()))
            }
            _ => panic!("bad reply line: {line:?}"),
        }
    }

    pub fn cmd(&mut self, args: &[&str]) -> Reply {
        let bytes: Vec<&[u8]> = args.iter().map(|s| s.as_bytes()).collect();
        self.send(&bytes);
        self.read_reply()
    }

    pub fn cmd_bytes(&mut self, args: &[&[u8]]) -> Reply {
        self.send(args);
        self.read_reply()
    }

    /// Pipeline: send everything, then read all replies.
    pub fn pipeline(&mut self, cmds: &[&[&str]]) -> Vec<Reply> {
        for c in cmds {
            let bytes: Vec<&[u8]> = c.iter().map(|s| s.as_bytes()).collect();
            self.send(&bytes);
        }
        (0..cmds.len()).map(|_| self.read_reply()).collect()
    }
}

/// Parse a single RESP reply from a byte buffer (for in-process tests).
pub fn parse_reply(buf: &[u8]) -> Reply {
    fn go(buf: &[u8], pos: &mut usize) -> Reply {
        let line_end = buf[*pos..].windows(2).position(|w| w == b"\r\n").unwrap() + *pos;
        let prefix = buf[*pos];
        let line = &buf[*pos + 1..line_end];
        *pos = line_end + 2;
        match prefix {
            b'+' => Reply::Simple(String::from_utf8_lossy(line).into_owned()),
            b'-' => Reply::Error(String::from_utf8_lossy(line).into_owned()),
            b':' => Reply::Int(std::str::from_utf8(line).unwrap().parse().unwrap()),
            b'$' => {
                let n: i64 = std::str::from_utf8(line).unwrap().parse().unwrap();
                if n < 0 {
                    return Reply::Bulk(None);
                }
                let data = buf[*pos..*pos + n as usize].to_vec();
                *pos += n as usize + 2;
                Reply::Bulk(Some(data))
            }
            b'*' => {
                let n: i64 = std::str::from_utf8(line).unwrap().parse().unwrap();
                if n < 0 {
                    return Reply::Array(None);
                }
                Reply::Array(Some((0..n).map(|_| go(buf, pos)).collect()))
            }
            _ => panic!("bad reply prefix {}", prefix as char),
        }
    }
    let mut pos = 0;
    go(buf, &mut pos)
}
