//! The VFS: SQLite's file operations, mapped onto protocol requests.
//!
//! Each open database has one [`Db`], shared by every connection to it in
//! this process. A read-write `Db` holds the database's lease. Main-database
//! handles keep their connection's uncommitted transaction and, for read-only
//! connections, the version their read transaction observes. Temporary files,
//! and rollback journals on servers with atomic commits, live in memory.

use std::collections::BTreeMap;
use std::ffi::{c_int, c_void};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use grpsqlite_proto as proto;
use parking_lot::Mutex;
use proto::FileKind;
use sqlite_plugin::flags::{AccessFlags, LockLevel, OpenKind, OpenOpts};
use sqlite_plugin::vars;
use sqlite_plugin::vfs::{Pragma, PragmaErr, VfsHandle, VfsResult};
use tonic::{Code, Status};
use xxhash_rust::xxh3::xxh3_64;

use crate::backend::{Backend, is_lease_error};
use crate::cache::{CacheSettings, PageCache};
use crate::config::Config;
use crate::locks::LockTable;
use crate::overlay::Overlay;

const PRAGMA_PREFIX: &str = "grpsqlite_";

/// The VFS state shared by every handle.
pub struct Core {
    backend: Arc<dyn Backend>,
    config: Result<Config, String>,
    cache: Option<CacheSettings>,
    /// Open databases by path and read-only flag.
    dbs: Mutex<BTreeMap<(String, bool), Arc<Db>>>,
    /// Named in-memory files: rollback and super journals on servers with
    /// atomic commits.
    local: Mutex<BTreeMap<String, LocalFile>>,
    next_id: AtomicU64,
}

/// One database open in this process.
struct Db {
    path: String,
    readonly: bool,
    session: String,
    lease: String,
    atomic: bool,
    sector_size: i32,
    state: Mutex<DbState>,
}

#[derive(Default)]
struct DbState {
    main_handles: usize,
    locks: LockTable,
    /// Read-write only: committed bytes at the start of the main file.
    header: Option<Vec<u8>>,
    /// Read-write only: the main file's committed size.
    size: Option<u64>,
    cache: Option<PageCache>,
    lost: bool,
}

impl DbState {
    /// Records a committed write to the main file.
    fn written(&mut self, offset: u64, data: &Bytes) {
        if let Some(h) = &mut self.header {
            if offset == 0 && data.len() > h.len() {
                *h = data.to_vec();
            } else if (offset as usize) < h.len() {
                let start = offset as usize;
                let n = (h.len() - start).min(data.len());
                h[start..start + n].copy_from_slice(&data[..n]);
            }
        }
        if let Some(size) = &mut self.size {
            *size = (*size).max(offset + data.len() as u64);
        }
        if let Some(cache) = &mut self.cache {
            cache.put_committed(offset, data);
        }
    }

    /// Records a committed truncation of the main file.
    fn truncated(&mut self, size: u64) {
        if let Some(h) = &mut self.header {
            h.truncate(size as usize);
        }
        self.size = Some(size);
        if let Some(cache) = &mut self.cache {
            cache.truncate(size);
        }
    }

    /// Forgets committed state after a change whose outcome is unknown.
    fn forget(&mut self) {
        self.header = None;
        self.size = None;
        if let Some(cache) = &mut self.cache {
            cache.clear();
        }
    }
}

impl Db {
    fn file(&self, kind: FileKind) -> Option<proto::FileRef> {
        Some(proto::FileRef {
            session: self.session.clone(),
            lease: self.lease.clone(),
            database: self.path.clone(),
            kind: kind as i32,
        })
    }
}

/// An open file.
pub enum Handle {
    Main(MainFile),
    /// A rollback journal or WAL stored on a server without atomic commits.
    Remote(RemoteFile),
    Local(LocalHandle),
}

pub struct MainFile {
    db: Arc<Db>,
    id: u64,
    /// Read-only: the version this read transaction observes, or empty.
    snapshot: Bytes,
    /// Writes since the transaction began, sent at sync.
    txn: Overlay,
}

pub struct RemoteFile {
    db: Arc<Db>,
    kind: FileKind,
}

pub struct LocalHandle {
    file: LocalFile,
    name: Option<String>,
    delete_on_close: bool,
    readonly: bool,
}

#[derive(Clone, Default)]
pub struct LocalFile(Arc<Mutex<Vec<u8>>>);

impl VfsHandle for Handle {
    fn readonly(&self) -> bool {
        match self {
            Handle::Main(f) => f.db.readonly,
            Handle::Remote(f) => f.db.readonly,
            Handle::Local(f) => f.readonly,
        }
    }

    fn in_memory(&self) -> bool {
        false
    }
}

fn is_ioerr(code: c_int) -> bool {
    code & 0xff == vars::SQLITE_IOERR
}

/// The database a journal, WAL or super-journal path belongs to.
fn sidecar(path: &str) -> Option<(&str, FileKind)> {
    if let Some(db) = path.strip_suffix("-journal") {
        return Some((db, FileKind::Journal));
    }
    if let Some(db) = path.strip_suffix("-wal") {
        return Some((db, FileKind::Wal));
    }
    None
}

fn super_journal_db(path: &str) -> Option<&str> {
    path.rfind("-mj").map(|i| &path[..i])
}

impl Core {
    pub fn new(backend: Arc<dyn Backend>, config: Result<Config, String>) -> Core {
        let cache = config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .map(CacheSettings::from_config);
        Core::with_cache(backend, config, cache)
    }

    /// Like [`Core::new`], with caches built from `cache` instead of the
    /// configuration.
    pub fn with_cache(
        backend: Arc<dyn Backend>,
        config: Result<Config, String>,
        cache: Option<CacheSettings>,
    ) -> Core {
        Core {
            backend,
            config,
            cache,
            dbs: Mutex::new(BTreeMap::new()),
            local: Mutex::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Whether no database or in-memory file is open.
    pub fn is_idle(&self) -> bool {
        self.dbs.lock().is_empty() && self.local.lock().is_empty()
    }

    fn find_db(&self, path: &str) -> Option<Arc<Db>> {
        let dbs = self.dbs.lock();
        dbs.get(&(path.to_string(), false))
            .or_else(|| dbs.get(&(path.to_string(), true)))
            .cloned()
    }

    /// Fails when the database's lease is gone.
    fn check(&self, db: &Db) -> VfsResult<()> {
        if db.readonly {
            return Ok(());
        }
        let mut st = db.state.lock();
        if st.lost {
            return Err(vars::SQLITE_IOERR);
        }
        if !self.backend.lease_valid(&db.session) {
            mark_lost(db, &mut st);
            return Err(vars::SQLITE_IOERR);
        }
        Ok(())
    }

    /// Logs a failed request and returns `code`. A rejected lease marks the
    /// database lost.
    fn fail(&self, db: &Db, op: &str, status: &Status, code: c_int) -> c_int {
        if is_lease_error(status) && !db.readonly {
            mark_lost(db, &mut db.state.lock());
        } else {
            log::error!("{op} {}: {status}", db.path);
        }
        code
    }

    pub fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Handle> {
        log::debug!("open {path:?} {opts:?}");
        let config = match &self.config {
            Ok(c) => c,
            Err(e) => {
                log::error!("misconfigured: {e}");
                return Err(vars::SQLITE_CANTOPEN);
            }
        };
        let readonly = opts.mode().is_readonly();
        let delete_on_close = opts.delete_on_close();
        match (opts.kind(), path) {
            (OpenKind::MainDb, Some(path)) => self.open_main(config, path, readonly),
            (OpenKind::MainJournal | OpenKind::Wal, Some(path)) => {
                self.open_sidecar(path, readonly, delete_on_close)
            }
            (OpenKind::SuperJournal, Some(path)) => {
                let atomic = super_journal_db(path)
                    .and_then(|db| self.find_db(db))
                    .is_some_and(|db| db.atomic);
                if !atomic {
                    log::error!(
                        "a transaction that writes several databases needs a server with atomic commits"
                    );
                    return Err(vars::SQLITE_CANTOPEN);
                }
                Ok(self.open_local_named(path, readonly, delete_on_close))
            }
            _ => Ok(Handle::Local(LocalHandle {
                file: LocalFile::default(),
                name: None,
                delete_on_close: true,
                readonly,
            })),
        }
    }

    fn open_main(&self, config: &Config, path: &str, readonly: bool) -> VfsResult<Handle> {
        let key = (path.to_string(), readonly);
        let mut dbs = self.dbs.lock();
        let db = match dbs.get(&key) {
            Some(db) if !db.state.lock().lost => db.clone(),
            _ => {
                let resp = self
                    .backend
                    .open(proto::OpenRequest {
                        client_token: config.client_token.clone(),
                        database: path.to_string(),
                        readonly,
                    })
                    .map_err(|s| {
                        log::error!("opening {path}: {s}");
                        vars::SQLITE_CANTOPEN
                    })?;
                if readonly && !(resp.point_in_time_reads && resp.atomic_commit) {
                    log::error!(
                        "opening {path} read-only needs a server with point-in-time reads and atomic commits"
                    );
                    let _ = self.backend.close(proto::CloseRequest {
                        session: resp.session,
                        lease: resp.lease,
                        database: path.to_string(),
                    });
                    return Err(vars::SQLITE_CANTOPEN);
                }
                let cache = self
                    .cache
                    .as_ref()
                    .and_then(|c| PageCache::open(c, path, readonly));
                let db = Arc::new(Db {
                    path: path.to_string(),
                    readonly,
                    session: resp.session,
                    lease: resp.lease,
                    atomic: resp.atomic_commit,
                    sector_size: match resp.sector_size {
                        0 => 4096,
                        s => s as i32,
                    },
                    state: Mutex::new(DbState {
                        cache,
                        ..DbState::default()
                    }),
                });
                dbs.insert(key, db.clone());
                db
            }
        };
        db.state.lock().main_handles += 1;
        Ok(Handle::Main(MainFile {
            db,
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            snapshot: Bytes::new(),
            txn: Overlay::default(),
        }))
    }

    fn open_sidecar(&self, path: &str, readonly: bool, delete_on_close: bool) -> VfsResult<Handle> {
        let Some((db_path, kind)) = sidecar(path) else {
            log::error!("{path} does not name a journal or WAL");
            return Err(vars::SQLITE_CANTOPEN);
        };
        let Some(db) = self.find_db(db_path) else {
            log::error!("{path} belongs to no open database");
            return Err(vars::SQLITE_CANTOPEN);
        };
        if !db.atomic {
            return Ok(Handle::Remote(RemoteFile { db, kind }));
        }
        if kind == FileKind::Wal {
            log::error!(
                "{path}: WAL mode needs a server without atomic commits; use journal_mode=memory or delete"
            );
            return Err(vars::SQLITE_CANTOPEN);
        }
        // The server never holds part of a transaction, so a journal never
        // needs to outlive this process.
        Ok(self.open_local_named(path, readonly, delete_on_close))
    }

    fn open_local_named(&self, path: &str, readonly: bool, delete_on_close: bool) -> Handle {
        let file = self
            .local
            .lock()
            .entry(path.to_string())
            .or_default()
            .clone();
        Handle::Local(LocalHandle {
            file,
            name: Some(path.to_string()),
            delete_on_close,
            readonly,
        })
    }

    pub fn close(&self, handle: Handle) -> VfsResult<()> {
        match handle {
            Handle::Main(f) => self.close_main(f),
            Handle::Remote(_) => {}
            Handle::Local(f) => {
                if let (true, Some(name)) = (f.delete_on_close, f.name) {
                    self.local.lock().remove(&name);
                }
            }
        }
        Ok(())
    }

    /// Releases the handle's locks; the last handle of a database also ends
    /// its session. Local state is released even when the server is
    /// unreachable.
    fn close_main(&self, f: MainFile) {
        log::debug!("close {}", f.db.path);
        if !f.txn.is_empty() {
            log::debug!("discarding uncommitted writes to {} at close", f.db.path);
        }
        let last = {
            let mut st = f.db.state.lock();
            st.locks.unlock(f.id, LockLevel::Unlocked);
            st.main_handles -= 1;
            st.main_handles == 0
        };
        if !last {
            return;
        }
        let db = &f.db;
        let mut dbs = self.dbs.lock();
        let key = (db.path.clone(), db.readonly);
        if dbs.get(&key).is_some_and(|d| Arc::ptr_eq(d, db)) {
            dbs.remove(&key);
        }
        if !db.readonly {
            let journal = format!("{}-journal", db.path);
            let super_prefix = format!("{}-mj", db.path);
            self.local
                .lock()
                .retain(|name, _| *name != journal && !name.starts_with(&super_prefix));
            if let Some(cache) = db.state.lock().cache.take() {
                cache.close();
            }
        }
        // Holding `dbs` makes a concurrent open of this database wait until
        // the lease is released.
        if let Err(s) = self.backend.close(proto::CloseRequest {
            session: db.session.clone(),
            lease: db.lease.clone(),
            database: db.path.clone(),
        }) {
            log::warn!("closing {}: {s}", db.path);
        }
    }

    pub fn access(&self, path: &str, _flags: AccessFlags) -> VfsResult<bool> {
        if let Some(f) = self.local.lock().get(path) {
            return Ok(!f.0.lock().is_empty());
        }
        let Some((db_path, kind)) = sidecar(path) else {
            return Ok(false);
        };
        let Some(db) = self.find_db(db_path) else {
            return Ok(false);
        };
        if db.atomic {
            return Ok(false);
        }
        self.check(&db)?;
        // Like the unix VFS, an empty file does not exist.
        let resp = self
            .backend
            .get_file_size(proto::GetFileSizeRequest {
                file: db.file(kind),
                version: Bytes::new(),
            })
            .map_err(|s| self.fail(&db, "access", &s, vars::SQLITE_IOERR_ACCESS))?;
        Ok(resp.size > 0)
    }

    pub fn delete(&self, path: &str) -> VfsResult<()> {
        log::debug!("delete {path}");
        if self.local.lock().remove(path).is_some() {
            return Ok(());
        }
        let Some((db_path, kind)) = sidecar(path) else {
            return Ok(());
        };
        let Some(db) = self.find_db(db_path) else {
            return Ok(());
        };
        if db.atomic {
            return Ok(());
        }
        self.check(&db)?;
        self.backend
            .delete(proto::DeleteRequest {
                file: db.file(kind),
            })
            .map_err(|s| self.fail(&db, "delete", &s, vars::SQLITE_IOERR_DELETE))
    }

    pub fn file_size(&self, handle: &mut Handle) -> VfsResult<usize> {
        let size = match handle {
            Handle::Main(f) => self.main_size(f),
            Handle::Remote(f) => {
                self.check(&f.db)?;
                self.backend
                    .get_file_size(proto::GetFileSizeRequest {
                        file: f.db.file(f.kind),
                        version: Bytes::new(),
                    })
                    .map(|r| r.size)
                    .map_err(|s| self.fail(&f.db, "size", &s, vars::SQLITE_IOERR_FSTAT))
            }
            Handle::Local(f) => Ok(f.file.0.lock().len() as u64),
        }?;
        usize::try_from(size).map_err(|_| vars::SQLITE_IOERR_FSTAT)
    }

    fn main_size(&self, f: &mut MainFile) -> VfsResult<u64> {
        self.check(&f.db)?;
        let known = if f.db.readonly {
            None
        } else {
            f.db.state.lock().size
        };
        let committed = match known {
            Some(size) => size,
            None => {
                let resp = self
                    .backend
                    .get_file_size(proto::GetFileSizeRequest {
                        file: f.db.file(FileKind::Main),
                        version: f.snapshot.clone(),
                    })
                    .map_err(|s| self.fail(&f.db, "size", &s, vars::SQLITE_IOERR_FSTAT))?;
                if f.db.readonly {
                    if f.snapshot.is_empty() {
                        f.snapshot = resp.version;
                    }
                } else {
                    f.db.state.lock().size = Some(resp.size);
                }
                resp.size
            }
        };
        Ok(f.txn.size(committed))
    }

    pub fn read(&self, handle: &mut Handle, offset: usize, buf: &mut [u8]) -> VfsResult<usize> {
        let offset = offset as u64;
        match handle {
            Handle::Main(f) => self.main_read(f, offset, buf),
            Handle::Remote(f) => {
                self.check(&f.db)?;
                let resp = self
                    .backend
                    .read(proto::ReadRequest {
                        file: f.db.file(f.kind),
                        offset,
                        length: buf.len() as u64,
                        version: Bytes::new(),
                        checksum: 0,
                    })
                    .map_err(|s| self.fail(&f.db, "read", &s, vars::SQLITE_IOERR_READ))?;
                let n = resp.data.len().min(buf.len());
                buf[..n].copy_from_slice(&resp.data[..n]);
                Ok(n)
            }
            Handle::Local(f) => {
                let data = f.file.0.lock();
                let start = (offset as usize).min(data.len());
                let n = (data.len() - start).min(buf.len());
                buf[..n].copy_from_slice(&data[start..start + n]);
                Ok(n)
            }
        }
    }

    fn main_read(&self, f: &mut MainFile, offset: u64, buf: &mut [u8]) -> VfsResult<usize> {
        self.check(&f.db)?;
        if f.txn.is_empty() {
            return self.read_committed(f, offset, buf);
        }
        let committed = if f.txn.covers(offset, buf.len() as u64) {
            0
        } else {
            self.read_committed(f, offset, buf)?
        };
        buf[committed..].fill(0);
        Ok(f.txn.apply(offset, buf, committed))
    }

    /// Reads committed bytes: from the header or page cache when this process
    /// holds the lease, otherwise from the server.
    fn read_committed(&self, f: &mut MainFile, offset: u64, buf: &mut [u8]) -> VfsResult<usize> {
        let len = buf.len() as u64;
        let db = f.db.clone();
        let mut checksum = 0;
        if db.readonly {
            if let Some(cache) = &mut db.state.lock().cache {
                checksum = cache.checksum(offset, len);
            }
        } else {
            let mut st = db.state.lock();
            if let Some(h) = &st.header {
                if offset + len <= h.len() as u64 {
                    let start = offset as usize;
                    buf.copy_from_slice(&h[start..start + buf.len()]);
                    return Ok(buf.len());
                }
            }
            if st.size.is_some_and(|size| offset >= size) {
                return Ok(0);
            }
            if let Some(cache) = &mut st.cache {
                if let Some(n) = cache.read(offset, buf) {
                    return Ok(n);
                }
            }
        }
        let resp = self
            .backend
            .read(proto::ReadRequest {
                file: db.file(FileKind::Main),
                offset,
                length: len,
                version: f.snapshot.clone(),
                checksum,
            })
            .map_err(|s| self.fail(&db, "read", &s, vars::SQLITE_IOERR_READ))?;
        if db.readonly && f.snapshot.is_empty() {
            f.snapshot = resp.version.clone();
        }
        if resp.checksum_matched {
            let mut st = db.state.lock();
            if let Some(n) = st
                .cache
                .as_mut()
                .and_then(|c| c.read_verified(offset, buf, checksum))
            {
                return Ok(n);
            }
            drop(st);
            // The cached copy failed verification: ask again for the bytes.
            return self.read_uncached(f, offset, buf);
        }
        let n = resp.data.len().min(buf.len());
        buf[..n].copy_from_slice(&resp.data[..n]);
        let mut st = db.state.lock();
        if !db.readonly && offset == 0 && st.header.as_ref().is_none_or(|h| h.len() < n) {
            st.header = Some(resp.data[..n].to_vec());
        }
        if let Some(cache) = &mut st.cache {
            cache.put_read(offset, len, resp.data.slice(..n));
        }
        Ok(n)
    }

    fn read_uncached(&self, f: &mut MainFile, offset: u64, buf: &mut [u8]) -> VfsResult<usize> {
        let resp = self
            .backend
            .read(proto::ReadRequest {
                file: f.db.file(FileKind::Main),
                offset,
                length: buf.len() as u64,
                version: f.snapshot.clone(),
                checksum: 0,
            })
            .map_err(|s| self.fail(&f.db, "read", &s, vars::SQLITE_IOERR_READ))?;
        let n = resp.data.len().min(buf.len());
        buf[..n].copy_from_slice(&resp.data[..n]);
        if let Some(cache) = &mut f.db.state.lock().cache {
            cache.put_read(offset, buf.len() as u64, resp.data.slice(..n));
        }
        Ok(n)
    }

    pub fn write(&self, handle: &mut Handle, offset: usize, data: &[u8]) -> VfsResult<usize> {
        let offset = offset as u64;
        match handle {
            Handle::Main(f) => self.main_write(f, offset, data)?,
            Handle::Remote(f) => {
                self.check(&f.db)?;
                self.backend
                    .write(proto::WriteRequest {
                        file: f.db.file(f.kind),
                        offset,
                        data: Bytes::copy_from_slice(data),
                        checksum: xxh3_64(data),
                    })
                    .map_err(|s| self.fail(&f.db, "write", &s, vars::SQLITE_IOERR_WRITE))?;
            }
            Handle::Local(f) => {
                let mut file = f.file.0.lock();
                let start = offset as usize;
                let end = start + data.len();
                if file.len() < end {
                    file.resize(end, 0);
                }
                file[start..end].copy_from_slice(data);
            }
        }
        Ok(data.len())
    }

    fn main_write(&self, f: &mut MainFile, offset: u64, data: &[u8]) -> VfsResult<()> {
        if f.db.readonly {
            return Err(vars::SQLITE_READONLY);
        }
        self.check(&f.db)?;
        if f.db.atomic {
            f.txn.write(offset, data);
            return Ok(());
        }
        let data = Bytes::copy_from_slice(data);
        let result = self.backend.write(proto::WriteRequest {
            file: f.db.file(FileKind::Main),
            offset,
            checksum: xxh3_64(&data),
            data: data.clone(),
        });
        let mut st = f.db.state.lock();
        match result {
            Ok(()) => {
                st.written(offset, &data);
                Ok(())
            }
            Err(s) => {
                st.forget();
                drop(st);
                Err(self.fail(&f.db, "write", &s, vars::SQLITE_IOERR_WRITE))
            }
        }
    }

    pub fn truncate(&self, handle: &mut Handle, size: usize) -> VfsResult<()> {
        let size = size as u64;
        match handle {
            Handle::Main(f) => {
                if f.db.readonly {
                    return Err(vars::SQLITE_READONLY);
                }
                self.check(&f.db)?;
                if f.db.atomic {
                    f.txn.truncate(size);
                    return Ok(());
                }
                let result = self.backend.truncate(proto::TruncateRequest {
                    file: f.db.file(FileKind::Main),
                    size,
                });
                let mut st = f.db.state.lock();
                match result {
                    Ok(()) => {
                        st.truncated(size);
                        Ok(())
                    }
                    Err(s) => {
                        st.forget();
                        drop(st);
                        Err(self.fail(&f.db, "truncate", &s, vars::SQLITE_IOERR_TRUNCATE))
                    }
                }
            }
            Handle::Remote(f) => {
                self.check(&f.db)?;
                self.backend
                    .truncate(proto::TruncateRequest {
                        file: f.db.file(f.kind),
                        size,
                    })
                    .map_err(|s| self.fail(&f.db, "truncate", &s, vars::SQLITE_IOERR_TRUNCATE))
            }
            Handle::Local(f) => {
                f.file.0.lock().resize(size as usize, 0);
                Ok(())
            }
        }
    }

    /// Sends the handle's transaction as one atomic commit.
    pub fn sync(&self, handle: &mut Handle) -> VfsResult<()> {
        let Handle::Main(f) = handle else {
            return Ok(());
        };
        if f.txn.is_empty() {
            return Ok(());
        }
        self.check(&f.db)?;
        let txn = std::mem::take(&mut f.txn);
        let committed_size = f.db.state.lock().size;
        let final_size = committed_size.map(|s| txn.size(s));
        let (cut, extents) = txn.into_commit_parts();
        log::debug!("commit {} writes to {}", extents.len(), f.db.path);
        let result = self.backend.commit(proto::CommitRequest {
            file: f.db.file(FileKind::Main),
            writes: extents
                .iter()
                .map(|e| proto::PageWrite {
                    offset: e.offset,
                    checksum: xxh3_64(&e.data),
                    data: e.data.clone(),
                })
                .collect(),
            truncate_to: cut,
        });
        let mut st = f.db.state.lock();
        match result {
            Ok(_) => {
                if let Some(cut) = cut {
                    st.truncated(cut);
                }
                for e in &extents {
                    st.written(e.offset, &e.data);
                }
                st.size = final_size;
                Ok(())
            }
            Err(s) => {
                st.forget();
                drop(st);
                Err(self.fail(&f.db, "commit", &s, vars::SQLITE_IOERR_WRITE))
            }
        }
    }

    pub fn lock(&self, handle: &mut Handle, level: LockLevel) -> VfsResult<()> {
        let Handle::Main(f) = handle else {
            return Ok(());
        };
        f.db.state
            .lock()
            .locks
            .lock(f.id, level)
            .map_err(|_| vars::SQLITE_BUSY)
    }

    pub fn unlock(&self, handle: &mut Handle, level: LockLevel) -> VfsResult<()> {
        let Handle::Main(f) = handle else {
            return Ok(());
        };
        f.db.state.lock().locks.unlock(f.id, level);
        if level < LockLevel::Reserved && !f.txn.is_empty() {
            log::debug!(
                "discarding writes of an unfinished transaction on {}",
                f.db.path
            );
            f.txn.clear();
        }
        if level == LockLevel::Unlocked {
            f.snapshot = Bytes::new();
        }
        Ok(())
    }

    pub fn check_reserved_lock(&self, handle: &mut Handle) -> VfsResult<bool> {
        Ok(match handle {
            Handle::Main(f) => f.db.state.lock().locks.reserved(),
            _ => false,
        })
    }

    pub fn file_control(&self, handle: &mut Handle, op: c_int, _arg: *mut c_void) -> VfsResult<()> {
        if op == vars::SQLITE_FCNTL_SYNC {
            return self.sync(handle);
        }
        Err(vars::SQLITE_NOTFOUND)
    }

    pub fn pragma(
        &self,
        handle: &mut Handle,
        pragma: Pragma<'_>,
    ) -> Result<Option<String>, PragmaErr> {
        let Handle::Main(f) = handle else {
            return Err(PragmaErr::NotFound);
        };
        let name = pragma.name.to_ascii_lowercase();
        if name == "locking_mode"
            && f.db.atomic
            && pragma
                .arg
                .is_some_and(|a| a.eq_ignore_ascii_case("exclusive"))
        {
            // In exclusive mode SQLite stops updating the header's change
            // counter after the first commit, and replicas rely on it to
            // notice commits. The lease already makes this process the only
            // writer, so exclusive mode gains nothing here.
            return Err(PragmaErr::Fail(
                vars::SQLITE_ERROR,
                Some("grpsqlite: locking_mode=exclusive is unsupported on servers with atomic commits".to_string()),
            ));
        }
        if !name.starts_with(PRAGMA_PREFIX) {
            return Err(PragmaErr::NotFound);
        }
        match self.backend.pragma(proto::PragmaRequest {
            file: f.db.file(FileKind::Main),
            name,
            value: pragma.arg.unwrap_or_default().to_string(),
        }) {
            Ok(r) if r.response.is_empty() => Ok(None),
            Ok(r) => Ok(Some(r.response)),
            Err(s) if s.code() == Code::NotFound => Err(PragmaErr::NotFound),
            Err(s) => Err(PragmaErr::Fail(
                vars::SQLITE_ERROR,
                Some(s.message().to_string()),
            )),
        }
    }

    pub fn sector_size(&self, handle: &mut Handle) -> i32 {
        match handle {
            Handle::Main(f) => f.db.sector_size,
            Handle::Remote(f) => f.db.sector_size,
            Handle::Local(_) => 4096,
        }
    }

    /// Discards the transaction of a main-database handle whose operation
    /// failed with an I/O error: SQLite now rolls the transaction back.
    fn after(&self, handle: &mut Handle, result: VfsResult<()>) -> VfsResult<()> {
        if let (Err(code), Handle::Main(f)) = (&result, handle) {
            if is_ioerr(*code) {
                f.txn.clear();
            }
        }
        result
    }
}

fn mark_lost(db: &Db, st: &mut DbState) {
    if !st.lost {
        log::error!(
            "lost the lease for {}; reopen the database to continue",
            db.path
        );
    }
    st.lost = true;
    st.forget();
}

/// The type registered with SQLite.
pub struct GrpsqliteVfs(pub Arc<Core>);

impl sqlite_plugin::vfs::Vfs for GrpsqliteVfs {
    type Handle = Handle;

    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Handle> {
        self.0.open(path, opts)
    }

    fn delete(&self, path: &str) -> VfsResult<()> {
        self.0.delete(path)
    }

    fn access(&self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        self.0.access(path, flags)
    }

    fn file_size(&self, handle: &mut Handle) -> VfsResult<usize> {
        let result = self.0.file_size(handle);
        let size = result.as_ref().map(|_| ()).map_err(|e| *e);
        self.0.after(handle, size)?;
        result
    }

    fn truncate(&self, handle: &mut Handle, size: usize) -> VfsResult<()> {
        let result = self.0.truncate(handle, size);
        self.0.after(handle, result)
    }

    fn write(&self, handle: &mut Handle, offset: usize, data: &[u8]) -> VfsResult<usize> {
        let result = self.0.write(handle, offset, data);
        let r = result.as_ref().map(|_| ()).map_err(|e| *e);
        self.0.after(handle, r)?;
        result
    }

    fn read(&self, handle: &mut Handle, offset: usize, data: &mut [u8]) -> VfsResult<usize> {
        let result = self.0.read(handle, offset, data);
        let r = result.as_ref().map(|_| ()).map_err(|e| *e);
        self.0.after(handle, r)?;
        result
    }

    fn lock(&self, handle: &mut Handle, level: LockLevel) -> VfsResult<()> {
        self.0.lock(handle, level)
    }

    fn unlock(&self, handle: &mut Handle, level: LockLevel) -> VfsResult<()> {
        self.0.unlock(handle, level)
    }

    fn check_reserved_lock(&self, handle: &mut Handle) -> VfsResult<bool> {
        self.0.check_reserved_lock(handle)
    }

    fn sync(&self, handle: &mut Handle) -> VfsResult<()> {
        let result = self.0.sync(handle);
        self.0.after(handle, result)
    }

    fn close(&self, handle: Handle) -> VfsResult<()> {
        self.0.close(handle)
    }

    fn pragma(&self, handle: &mut Handle, pragma: Pragma<'_>) -> Result<Option<String>, PragmaErr> {
        self.0.pragma(handle, pragma)
    }

    fn sector_size(&self, handle: &mut Handle) -> VfsResult<i32> {
        Ok(self.0.sector_size(handle))
    }

    fn file_control(&self, handle: &mut Handle, op: c_int, arg: *mut c_void) -> VfsResult<()> {
        let result = self.0.file_control(handle, op, arg);
        if result == Err(vars::SQLITE_NOTFOUND) {
            return result;
        }
        self.0.after(handle, result)
    }
}
