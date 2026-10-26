//! gRPSQLite: a SQLite VFS that stores databases on a gRPC server.
//!
//! [`register_static`] registers a VFS built from a [`Core`] with a SQLite
//! linked into the same program. Builds for the SQLite shell link the static
//! library and call [`grpsqlite_extra_init`] through `SQLITE_EXTRA_INIT`; the
//! `dynamic` feature provides `sqlite3_grpsqlite_init` for `.load`.

use std::ffi::{CString, c_char, c_int};
use std::sync::Arc;

pub mod backend;
pub mod cache;
pub mod config;
mod grpc;
mod locks;
mod logging;
mod overlay;
mod vfs;

pub use backend::Backend;
pub use config::{CacheConfig, Config, EnvConfig, GrpcConfig};
pub use grpc::GrpcBackend;
pub use grpsqlite_proto as proto;
pub use vfs::{Core, GrpsqliteVfs, Handle};

/// The name the VFS registers under.
pub const VFS_NAME: &str = "grpsqlite";

/// A backend for a VFS whose configuration failed: every request fails.
struct Unconfigured;

impl Backend for Unconfigured {
    fn open(&self, _: proto::OpenRequest) -> Result<proto::OpenResponse, tonic::Status> {
        Err(tonic::Status::unavailable("grpsqlite is not configured"))
    }
    fn close(&self, _: proto::CloseRequest) -> Result<(), tonic::Status> {
        Ok(())
    }
    fn read(&self, _: proto::ReadRequest) -> Result<proto::ReadResponse, tonic::Status> {
        Err(tonic::Status::unavailable("grpsqlite is not configured"))
    }
    fn write(&self, _: proto::WriteRequest) -> Result<(), tonic::Status> {
        Err(tonic::Status::unavailable("grpsqlite is not configured"))
    }
    fn commit(&self, _: proto::CommitRequest) -> Result<proto::CommitResponse, tonic::Status> {
        Err(tonic::Status::unavailable("grpsqlite is not configured"))
    }
    fn get_file_size(
        &self,
        _: proto::GetFileSizeRequest,
    ) -> Result<proto::GetFileSizeResponse, tonic::Status> {
        Err(tonic::Status::unavailable("grpsqlite is not configured"))
    }
    fn truncate(&self, _: proto::TruncateRequest) -> Result<(), tonic::Status> {
        Err(tonic::Status::unavailable("grpsqlite is not configured"))
    }
    fn delete(&self, _: proto::DeleteRequest) -> Result<(), tonic::Status> {
        Err(tonic::Status::unavailable("grpsqlite is not configured"))
    }
    fn pragma(&self, _: proto::PragmaRequest) -> Result<proto::PragmaResponse, tonic::Status> {
        Err(tonic::Status::unavailable("grpsqlite is not configured"))
    }
    fn changed_pages(
        &self,
        _: proto::ChangedPagesRequest,
    ) -> Result<proto::ChangedPagesResponse, tonic::Status> {
        Err(tonic::Status::unavailable("grpsqlite is not configured"))
    }
    fn lease_valid(&self, _: &str) -> bool {
        false
    }
}

/// Builds the VFS the environment describes. Configuration errors are kept,
/// so registration succeeds and every open fails with `SQLITE_CANTOPEN` and a
/// logged reason.
pub fn core_from_env() -> (Arc<Core>, log::LevelFilter) {
    let env = EnvConfig::from_env();
    let level = env.as_ref().map_or(log::LevelFilter::Warn, |e| e.log_level);
    let core = match env {
        Ok(env) => match GrpcBackend::new(&env.grpc) {
            Ok(backend) => Core::new(Arc::new(backend), Ok(env.vfs)),
            Err(e) => Core::new(Arc::new(Unconfigured), Err(e)),
        },
        Err(e) => Core::new(Arc::new(Unconfigured), Err(e)),
    };
    (Arc::new(core), level)
}

fn vfs_name(name: &str) -> Result<CString, c_int> {
    CString::new(name).map_err(|_| sqlite_plugin::vars::SQLITE_MISUSE)
}

/// Registers `core` with the SQLite this program links, under `name`.
#[cfg(feature = "static")]
pub fn register_static(
    name: &str,
    core: Arc<Core>,
    make_default: bool,
    log_level: log::LevelFilter,
) -> Result<(), c_int> {
    let logger = sqlite_plugin::vfs::register_static(
        vfs_name(name)?,
        GrpsqliteVfs(core),
        sqlite_plugin::vfs::RegisterOpts { make_default },
    )?;
    logging::install(logger, log_level);
    Ok(())
}

/// Unregisters a VFS that [`register_static`] registered and frees its
/// `Core`.
///
/// # Safety
///
/// No SQLite connection may still use the VFS.
#[cfg(feature = "static")]
pub unsafe fn unregister_static(name: &str) -> Result<(), c_int> {
    let name = vfs_name(name)?;
    unsafe { sqlite_plugin::vfs::unregister_static::<GrpsqliteVfs>(&name) }
}

/// Registers the VFS from the environment as SQLite's default. Pass
/// `-DSQLITE_EXTRA_INIT=grpsqlite_extra_init` when compiling SQLite, which
/// then calls this at the end of `sqlite3_initialize`.
#[cfg(feature = "static")]
#[unsafe(no_mangle)]
pub extern "C" fn grpsqlite_extra_init(_unused: *const c_char) -> c_int {
    let (core, level) = core_from_env();
    match register_static(VFS_NAME, core, true, level) {
        Ok(()) => sqlite_plugin::vars::SQLITE_OK,
        Err(code) => code,
    }
}

/// SQLite's entry point for `.load libgrpsqlite`: registers the VFS from the
/// environment as the default.
///
/// # Safety
/// SQLite's extension loader must call this with a valid `p_api`.
#[cfg(feature = "dynamic")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_grpsqlite_init(
    _db: *mut std::ffi::c_void,
    _err: *mut *mut c_char,
    p_api: *mut sqlite_plugin::sqlite3_api_routines,
) -> c_int {
    let (core, level) = core_from_env();
    let name = match vfs_name(VFS_NAME) {
        Ok(n) => n,
        Err(code) => return code,
    };
    let registered = unsafe {
        sqlite_plugin::vfs::register_dynamic(
            p_api,
            name,
            GrpsqliteVfs(core),
            sqlite_plugin::vfs::RegisterOpts { make_default: true },
        )
    };
    match registered {
        Ok(logger) => {
            logging::install(logger, level);
            sqlite_plugin::vars::SQLITE_OK_LOAD_PERMANENTLY
        }
        Err(code) => code,
    }
}
