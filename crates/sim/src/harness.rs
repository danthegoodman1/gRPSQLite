//! Registers VFS instances with the bundled SQLite and opens connections
//! through them. Each [`Client`] stands for one process: its own VFS, so its
//! own sessions, leases and caches.

use std::ffi::c_int;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};

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

/// Turns off SQLite's memory statistics, which take a global mutex on every
/// allocation and so serialize seeds running on parallel threads. SQLite
/// accepts the setting only before it initializes.
fn configure_sqlite() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| unsafe {
        rusqlite::ffi::sqlite3_config(rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS, 0 as c_int);
    });
}

pub struct Client {
    pub name: String,
    pub core: Arc<Core>,
}

impl Drop for Client {
    /// Frees the VFS once no connection uses it. A client dropped while one
    /// of its connections is open leaves its VFS registered.
    fn drop(&mut self) {
        if self.core.is_idle() {
            // SAFETY: an idle core has no open files, so no connection uses
            // the VFS.
            let _ = unsafe { grpsqlite::unregister_static(&self.name) };
        }
    }
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
        configure_sqlite();
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
    configure_sqlite();
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
