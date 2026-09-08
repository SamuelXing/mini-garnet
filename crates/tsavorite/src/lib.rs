//! Mini Tsavorite: the storage engine underneath mini Garnet.
//!
//! Layout (each module header explains the design it mirrors):
//! * [`epoch`]  – epoch protection (`LightEpoch`), the basis for EPSM.
//! * [`hash`]   – 64-bit key hash.
//! * [`record`] – record header bits and on-log record layout.
//! * [`index`]  – cache-line hash buckets, tentative insert protocol, bucket latches.
//! * [`device`] – the disk tier (`IDevice`).
//! * [`log`]    – the hybrid log allocator: circular page buffer + address watermarks.
//! * [`store`]  – the RUMDS interface: Read/Upsert/RMW/Delete/Scan + callbacks,
//!   region decision logic, RCU with sealing, transactions, CPR versioning.
pub mod checkpoint;
pub mod device;
pub mod epoch;
pub mod hash;
pub mod index;
pub mod log;
pub mod record;
pub mod store;
