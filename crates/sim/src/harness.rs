//! Registers VFS instances with the bundled SQLite and opens connections
//! through them. Each [`Client`] stands for one process: its own VFS, so its
//! own sessions, leases and caches.

use std::ffi::c_int;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use grpsqlite::cache::CacheSettings;
use grpsqlite::{Backend, Config, Core};
use rusqlite::{Connection, OpenFlags};

static NEXT_VFS: AtomicU64 = AtomicU64::new(0);

/// Prints log records to stderr, for `GRPSQLITE_LOG=debug` runs.
struct Stderr;

impl log::Log for Stderr {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        eprintln!("    {} {}", record.level(), record.args());
    }

    fn flush(&self) {}
}

fn install_logger() {
    let level = std::env::var("GRPSQLITE_LOG")
        .ok()
        .and_then(|l| l.parse().ok())
        .unwrap_or(log::LevelFilter::Off);
    if level != log::LevelFilter::Off && log::set_logger(&Stderr).is_ok() {
        log::set_max_level(level);
    }
}

pub struct Client {
    pub name: String,
    pub core: Arc<Core>,
}

impl Client {
    pub fn new(backend: Arc<dyn Backend>, cache: Option<CacheSettings>) -> Client {
        Client::with_config(backend, Ok(Config::default()), cache)
    }

    pub fn with_config(
        backend: Arc<dyn Backend>,
        config: Result<Config, String>,
        cache: Option<CacheSettings>,
    ) -> Client {
        let name = format!("grpsqlite-sim-{}", NEXT_VFS.fetch_add(1, Ordering::Relaxed));
        let core = Arc::new(Core::with_cache(backend, config, cache));
        install_logger();
        if let Err(code) =
            grpsqlite::register_static(&name, core.clone(), false, log::LevelFilter::Off)
        {
            panic!("registering {name} failed with SQLite code {code}");
        }
        Client { name, core }
    }

    pub fn open(&self, path: &str) -> rusqlite::Result<Connection> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        Connection::open_with_flags_and_vfs(path, flags, self.name.as_str())
    }

    pub fn open_readonly(&self, path: &str) -> rusqlite::Result<Connection> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        Connection::open_with_flags_and_vfs(path, flags, self.name.as_str())
    }
}

/// Seeds SQLite's PRNG, which picks journal nonces and temporary names.
pub fn seed_sqlite(seed: u64) {
    unsafe {
        rusqlite::ffi::sqlite3_test_control(
            rusqlite::ffi::SQLITE_TESTCTRL_PRNG_SEED,
            seed as c_int,
            std::ptr::null_mut::<rusqlite::ffi::sqlite3>(),
        );
    }
}

/// The SQLite primary error code of `err`, if it has one.
pub fn sqlite_code(err: &rusqlite::Error) -> Option<rusqlite::ErrorCode> {
    match err {
        rusqlite::Error::SqliteFailure(e, _) => Some(e.code),
        _ => None,
    }
}
