//! mini-garnet library: a RESP cache-store on top of mini Tsavorite.
//! See `main.rs` for the binary entry point and the module headers for the
//! design each layer mirrors.

pub mod aof;
pub mod commands;
pub mod functions;
pub mod resp;
pub mod server;
pub mod shared;

/// Small store settings used by tests and the default in-memory server.
pub fn store_settings_small() -> tsavorite::store::StoreSettings {
    tsavorite::store::StoreSettings {
        index_buckets: 1 << 12,
        log: tsavorite::log::LogSettings {
            page_bits: 16,
            memory_pages: 8,
            mutable_fraction: 0.9,
        },
    }
}
