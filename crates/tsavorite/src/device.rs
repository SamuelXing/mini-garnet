//! `IDevice` equivalent: the on-disk tier of the hybrid log is a
//! log-structured device that only needs *sequential tail writes* and
//! *random reads* (paper §4.4). The file offset **is** the logical address,
//! which keeps address translation trivial.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Mutex;

pub trait Device: Send + Sync {
    fn write_at(&self, addr: u64, data: &[u8]) -> io::Result<()>;
    fn read_at(&self, addr: u64, buf: &mut [u8]) -> io::Result<()>;
    fn sync(&self) -> io::Result<()>;
}

impl<T: Device + ?Sized> Device for std::sync::Arc<T> {
    fn write_at(&self, addr: u64, data: &[u8]) -> io::Result<()> {
        (**self).write_at(addr, data)
    }
    fn read_at(&self, addr: u64, buf: &mut [u8]) -> io::Result<()> {
        (**self).read_at(addr, buf)
    }
    fn sync(&self) -> io::Result<()> {
        (**self).sync()
    }
}

/// A real file. Positioned IO, no seeking, safe for concurrent use.
pub struct FileDevice {
    file: File,
}

impl FileDevice {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(FileDevice { file })
    }
}

impl Device for FileDevice {
    fn write_at(&self, addr: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(data, addr)
    }
    fn read_at(&self, addr: u64, buf: &mut [u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.read_exact_at(buf, addr)
    }
    fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}

/// In-memory device for tests: behaves like a sparse file.
#[derive(Default)]
pub struct MemDevice {
    data: Mutex<Vec<u8>>,
}

impl MemDevice {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Device for MemDevice {
    fn write_at(&self, addr: u64, data: &[u8]) -> io::Result<()> {
        let mut d = self.data.lock().unwrap();
        let end = addr as usize + data.len();
        if d.len() < end {
            d.resize(end, 0);
        }
        d[addr as usize..end].copy_from_slice(data);
        Ok(())
    }
    fn read_at(&self, addr: u64, buf: &mut [u8]) -> io::Result<()> {
        let d = self.data.lock().unwrap();
        let end = addr as usize + buf.len();
        if d.len() < end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of MemDevice",
            ));
        }
        buf.copy_from_slice(&d[addr as usize..end]);
        Ok(())
    }
    fn sync(&self) -> io::Result<()> {
        Ok(())
    }
}
