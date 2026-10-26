//! An in-memory gRPSQLite server and the reference implementation of
//! `proto/grpsqlite.proto`.
//!
//! [`Store`] holds every database and applies requests synchronously, taking
//! the current time as an argument, so the simulator can drive it directly and
//! replay a seed exactly. [`Service`] serves a `Store` over gRPC.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use grpsqlite_proto as proto;
use parking_lot::Mutex;
use proto::FileKind;
use tonic::{Request, Response, Status};
use xxhash_rust::xxh3::xxh3_64;

/// What the server supports and how long leases last.
#[derive(Clone, Debug)]
pub struct Options {
    pub atomic_commit: bool,
    pub point_in_time_reads: bool,
    pub heartbeat_interval_ms: u64,
    pub lease_ttl_ms: u64,
    /// With point-in-time reads, how many versions before the latest stay
    /// readable.
    pub retained_versions: u64,
    /// With point-in-time reads, answer ChangedPages.
    pub changed_pages: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            atomic_commit: true,
            point_in_time_reads: true,
            heartbeat_interval_ms: 1000,
            lease_ttl_ms: 5000,
            retained_versions: 10_000,
            changed_pages: true,
        }
    }
}

/// Every database the server holds, plus open sessions.
#[derive(Debug)]
pub struct Store {
    opts: Options,
    databases: BTreeMap<String, Database>,
    next_id: u64,
}

#[derive(Debug, Default)]
struct Database {
    lease: Option<Lease>,
    /// Increments with every change to the main file.
    version: u64,
    main: VersionedFile,
    journal: Vec<u8>,
    wal: Vec<u8>,
}

#[derive(Debug)]
struct Lease {
    id: String,
    session: String,
    expires_at: u64,
}

impl Store {
    pub fn new(opts: Options) -> Store {
        Store {
            opts,
            databases: BTreeMap::new(),
            next_id: 0,
        }
    }

    pub fn options(&self) -> &Options {
        &self.opts
    }

    fn next_id(&mut self, prefix: &str) -> String {
        self.next_id += 1;
        format!("{prefix}{}", self.next_id)
    }

    pub fn open(
        &mut self,
        req: proto::OpenRequest,
        now: u64,
    ) -> Result<proto::OpenResponse, Status> {
        if req.readonly && !(self.opts.point_in_time_reads && self.opts.atomic_commit) {
            return Err(Status::failed_precondition(
                "read-only sessions need point-in-time reads and atomic commits",
            ));
        }
        let session = self.next_id("session-");
        let lease = if req.readonly {
            String::new()
        } else {
            let id = self.next_id("lease-");
            let ttl = self.opts.lease_ttl_ms;
            let db = self.databases.entry(req.database).or_default();
            if let Some(held) = &db.lease
                && now < held.expires_at
            {
                return Err(Status::failed_precondition(format!(
                    "database is leased by {}",
                    held.session
                )));
            }
            db.lease = Some(Lease {
                id: id.clone(),
                session: session.clone(),
                expires_at: now + ttl,
            });
            id
        };
        Ok(proto::OpenResponse {
            session,
            lease,
            atomic_commit: self.opts.atomic_commit,
            point_in_time_reads: self.opts.point_in_time_reads,
            sector_size: 4096,
            heartbeat_interval_millis: self.opts.heartbeat_interval_ms,
            lease_ttl_millis: self.opts.lease_ttl_ms,
            changed_pages: self.lists_changed_pages(),
        })
    }

    pub fn close(&mut self, req: proto::CloseRequest) -> Result<proto::CloseResponse, Status> {
        if let Some(db) = self.databases.get_mut(&req.database)
            && db.lease.as_ref().is_some_and(|l| l.id == req.lease)
        {
            db.lease = None;
        }
        Ok(proto::CloseResponse {})
    }

    pub fn heartbeat(
        &mut self,
        req: proto::HeartbeatRequest,
        now: u64,
    ) -> Result<proto::HeartbeatResponse, Status> {
        let ttl = self.opts.lease_ttl_ms;
        let db = self.databases.get_mut(&req.database);
        match db.and_then(|db| db.lease.as_mut()) {
            Some(lease) if lease.id == req.lease && now < lease.expires_at => {
                lease.expires_at = now + ttl;
                Ok(proto::HeartbeatResponse {})
            }
            _ => Err(Status::failed_precondition("lease expired or replaced")),
        }
    }

    pub fn read(&mut self, req: proto::ReadRequest) -> Result<proto::ReadResponse, Status> {
        let file = req.file.unwrap_or_default();
        let kind = file_kind(&file)?;
        let Some(db) = self.databases.get(&file.database) else {
            // A database nobody has written is empty at version 0.
            return Ok(proto::ReadResponse {
                version: self.encode_version(0),
                size: (kind == FileKind::Main).then_some(0),
                ..proto::ReadResponse::default()
            });
        };
        let version = self.read_version(db, &file, &req.version)?;
        let (data, size) = match kind {
            FileKind::Main => (
                db.main.read(version, req.offset, req.length),
                Some(db.main.size_at(version)),
            ),
            FileKind::Journal => (read_flat(&db.journal, req.offset, req.length), None),
            FileKind::Wal => (read_flat(&db.wal, req.offset, req.length), None),
        };
        let version = self.encode_version(version);
        if req.checksum != 0 && xxh3_64(&data) == req.checksum {
            return Ok(proto::ReadResponse {
                data: Bytes::new(),
                version,
                checksum_matched: true,
                size,
            });
        }
        Ok(proto::ReadResponse {
            data: data.into(),
            version,
            checksum_matched: false,
            size,
        })
    }

    pub fn changed_pages(
        &mut self,
        req: proto::ChangedPagesRequest,
    ) -> Result<proto::ChangedPagesResponse, Status> {
        if !self.lists_changed_pages() {
            return Err(Status::unimplemented("ChangedPages is off"));
        }
        let file = req.file.unwrap_or_default();
        if file_kind(&file)? != FileKind::Main {
            return Err(Status::invalid_argument(
                "ChangedPages applies to the main file",
            ));
        }
        let Some(db) = self.databases.get(&file.database) else {
            return Ok(proto::ChangedPagesResponse::default());
        };
        let since = self.read_version(db, &file, &req.since)?;
        let until = self.read_version(db, &file, &req.until)?;
        let ranges = db
            .main
            .changed(since, until)
            .into_iter()
            .map(|(offset, length)| proto::ByteRange { offset, length })
            .collect();
        Ok(proto::ChangedPagesResponse { ranges })
    }

    pub fn get_file_size(
        &mut self,
        req: proto::GetFileSizeRequest,
    ) -> Result<proto::GetFileSizeResponse, Status> {
        let file = req.file.unwrap_or_default();
        let kind = file_kind(&file)?;
        let Some(db) = self.databases.get(&file.database) else {
            return Ok(proto::GetFileSizeResponse {
                size: 0,
                version: self.encode_version(0),
            });
        };
        let version = self.read_version(db, &file, &req.version)?;
        let size = match kind {
            FileKind::Main => db.main.size_at(version),
            FileKind::Journal => db.journal.len() as u64,
            FileKind::Wal => db.wal.len() as u64,
        };
        Ok(proto::GetFileSizeResponse {
            size,
            version: self.encode_version(version),
        })
    }

    pub fn write(
        &mut self,
        req: proto::WriteRequest,
        now: u64,
    ) -> Result<proto::WriteResponse, Status> {
        if self.opts.atomic_commit {
            return Err(Status::unimplemented("this server takes Commit, not Write"));
        }
        let file = req.file.unwrap_or_default();
        let kind = file_kind(&file)?;
        let retain = self.retain_from_latest();
        let db = self.leased(&file, now)?;
        match kind {
            FileKind::Main => {
                db.version += 1;
                let version = db.version;
                db.main.write(version, req.offset, &req.data);
                db.main.prune(version.saturating_sub(retain));
            }
            FileKind::Journal => write_flat(&mut db.journal, req.offset, &req.data),
            FileKind::Wal => write_flat(&mut db.wal, req.offset, &req.data),
        }
        Ok(proto::WriteResponse {})
    }

    pub fn commit(
        &mut self,
        req: proto::CommitRequest,
        now: u64,
    ) -> Result<proto::CommitResponse, Status> {
        if !self.opts.atomic_commit {
            return Err(Status::unimplemented("this server takes Write, not Commit"));
        }
        let file = req.file.unwrap_or_default();
        if file_kind(&file)? != FileKind::Main {
            return Err(Status::invalid_argument("Commit applies to the main file"));
        }
        let retain = self.retain_from_latest();
        let db = self.leased(&file, now)?;
        db.version += 1;
        let version = db.version;
        if let Some(size) = req.truncate_to {
            db.main.truncate(version, size);
        }
        for w in &req.writes {
            db.main.write(version, w.offset, &w.data);
        }
        db.main.prune(version.saturating_sub(retain));
        Ok(proto::CommitResponse {
            version: self.encode_version(version),
        })
    }

    pub fn truncate(
        &mut self,
        req: proto::TruncateRequest,
        now: u64,
    ) -> Result<proto::TruncateResponse, Status> {
        let file = req.file.unwrap_or_default();
        let kind = file_kind(&file)?;
        if self.opts.atomic_commit && kind == FileKind::Main {
            return Err(Status::invalid_argument(
                "this server truncates the main file only inside Commit",
            ));
        }
        let retain = self.retain_from_latest();
        let db = self.leased(&file, now)?;
        match kind {
            FileKind::Main => {
                db.version += 1;
                let version = db.version;
                db.main.truncate(version, req.size);
                db.main.prune(version.saturating_sub(retain));
            }
            FileKind::Journal => db.journal.resize(req.size as usize, 0),
            FileKind::Wal => db.wal.resize(req.size as usize, 0),
        }
        Ok(proto::TruncateResponse {})
    }

    pub fn delete(
        &mut self,
        req: proto::DeleteRequest,
        now: u64,
    ) -> Result<proto::DeleteResponse, Status> {
        let file = req.file.unwrap_or_default();
        let kind = file_kind(&file)?;
        let retain = self.retain_from_latest();
        let db = self.leased(&file, now)?;
        match kind {
            FileKind::Main => {
                db.version += 1;
                let version = db.version;
                db.main.truncate(version, 0);
                db.main.prune(version.saturating_sub(retain));
            }
            FileKind::Journal => db.journal.clear(),
            FileKind::Wal => db.wal.clear(),
        }
        Ok(proto::DeleteResponse {})
    }

    pub fn pragma(&mut self, req: proto::PragmaRequest) -> Result<proto::PragmaResponse, Status> {
        let file = req.file.unwrap_or_default();
        match req.name.as_str() {
            "grpsqlite_server" => Ok(proto::PragmaResponse {
                response: "memory".to_string(),
            }),
            "grpsqlite_version" => {
                let version = self
                    .databases
                    .get(&file.database)
                    .map_or(0, |db| db.version);
                Ok(proto::PragmaResponse {
                    response: version.to_string(),
                })
            }
            _ => Err(Status::not_found("unknown pragma")),
        }
    }

    /// The database's main-file version, for tests.
    pub fn version(&self, database: &str) -> u64 {
        self.databases.get(database).map_or(0, |db| db.version)
    }

    /// The latest bytes of a database file, for tests.
    pub fn file(&self, database: &str, kind: FileKind) -> Vec<u8> {
        let Some(db) = self.databases.get(database) else {
            return Vec::new();
        };
        match kind {
            FileKind::Main => db.main.read(db.version, 0, db.main.size_at(db.version)),
            FileKind::Journal => db.journal.clone(),
            FileKind::Wal => db.wal.clone(),
        }
    }

    /// Whether `lease` is the database's current, unexpired lease.
    pub fn lease_held(&self, database: &str, lease: &str, now: u64) -> bool {
        self.databases
            .get(database)
            .and_then(|db| db.lease.as_ref())
            .is_some_and(|l| l.id == lease && now < l.expires_at)
    }

    fn retain_from_latest(&self) -> u64 {
        if self.opts.point_in_time_reads {
            self.opts.retained_versions
        } else {
            0
        }
    }

    fn leased(&mut self, file: &proto::FileRef, now: u64) -> Result<&mut Database, Status> {
        let db = self.databases.entry(file.database.clone()).or_default();
        match &db.lease {
            Some(lease) if lease.id == file.lease && now < lease.expires_at => Ok(db),
            _ => Err(Status::failed_precondition("lease expired or replaced")),
        }
    }

    /// The version a read observes: the requested one for read-only sessions
    /// on a point-in-time server, otherwise the latest.
    fn lists_changed_pages(&self) -> bool {
        self.opts.point_in_time_reads && self.opts.changed_pages
    }

    fn read_version(
        &self,
        db: &Database,
        file: &proto::FileRef,
        version: &[u8],
    ) -> Result<u64, Status> {
        if !file.lease.is_empty() && !version.is_empty() {
            return Err(Status::invalid_argument(
                "a request with a lease reads the latest version",
            ));
        }
        if !self.opts.point_in_time_reads || version.is_empty() {
            return Ok(db.version);
        }
        let bytes: [u8; 8] = version
            .try_into()
            .map_err(|_| Status::invalid_argument("malformed version"))?;
        let requested = u64::from_be_bytes(bytes);
        if requested > db.version {
            return Err(Status::invalid_argument("version from the future"));
        }
        if requested < db.version.saturating_sub(self.opts.retained_versions) {
            return Err(Status::out_of_range("version no longer retained"));
        }
        Ok(requested)
    }

    fn encode_version(&self, version: u64) -> Bytes {
        if self.opts.point_in_time_reads {
            Bytes::copy_from_slice(&version.to_be_bytes())
        } else {
            Bytes::new()
        }
    }
}

fn file_kind(file: &proto::FileRef) -> Result<FileKind, Status> {
    FileKind::try_from(file.kind).map_err(|_| Status::invalid_argument("unknown file kind"))
}

fn read_flat(data: &[u8], offset: u64, length: u64) -> Vec<u8> {
    let start = (offset as usize).min(data.len());
    let end = (offset.saturating_add(length) as usize).min(data.len());
    data[start..end].to_vec()
}

fn write_flat(data: &mut Vec<u8>, offset: u64, bytes: &[u8]) {
    let start = offset as usize;
    let end = start + bytes.len();
    if data.len() < end {
        data.resize(end, 0);
    }
    data[start..end].copy_from_slice(bytes);
}

const CHUNK: u64 = 4096;

/// One chunk's contents from a version on; `None` means zeros.
type ChunkHistory = Vec<(u64, Option<Arc<[u8]>>)>;

/// A byte file that keeps old versions readable, as a history per 4 KiB
/// chunk.
#[derive(Debug, Default)]
struct VersionedFile {
    chunks: BTreeMap<u64, ChunkHistory>,
    sizes: Vec<(u64, u64)>,
}

impl VersionedFile {
    fn size_at(&self, version: u64) -> u64 {
        at_version(&self.sizes, version).copied().unwrap_or(0)
    }

    fn chunk_at(&self, index: u64, version: u64) -> Option<&Arc<[u8]>> {
        self.chunks
            .get(&index)
            .and_then(|history| at_version(history, version))
            .and_then(|c| c.as_ref())
    }

    fn read(&self, version: u64, offset: u64, length: u64) -> Vec<u8> {
        let end = offset.saturating_add(length).min(self.size_at(version));
        if offset >= end {
            return Vec::new();
        }
        let mut out = vec![0u8; (end - offset) as usize];
        let mut pos = offset;
        while pos < end {
            let index = pos / CHUNK;
            let within = (pos % CHUNK) as usize;
            let n = ((CHUNK - pos % CHUNK).min(end - pos)) as usize;
            if let Some(chunk) = self.chunk_at(index, version) {
                let dst = (pos - offset) as usize;
                out[dst..dst + n].copy_from_slice(&chunk[within..within + n]);
            }
            pos += n as u64;
        }
        out
    }

    fn write(&mut self, version: u64, offset: u64, data: &[u8]) {
        let end = offset + data.len() as u64;
        let mut pos = offset;
        while pos < end {
            let index = pos / CHUNK;
            let within = (pos % CHUNK) as usize;
            let n = ((CHUNK - pos % CHUNK).min(end - pos)) as usize;
            let mut chunk = match self.chunk_at(index, version) {
                Some(c) => c.to_vec(),
                None => vec![0u8; CHUNK as usize],
            };
            let src = (pos - offset) as usize;
            chunk[within..within + n].copy_from_slice(&data[src..src + n]);
            self.set_chunk(index, version, Some(chunk.into()));
            pos += n as u64;
        }
        let size = self.size_at(version);
        if end > size {
            self.set_size(version, end);
        }
    }

    fn truncate(&mut self, version: u64, size: u64) {
        let old = self.size_at(version);
        if size < old {
            // Zero the tail of the boundary chunk and drop whole chunks past
            // the new end, so a later extension reads zeros.
            if !size.is_multiple_of(CHUNK) {
                let index = size / CHUNK;
                if let Some(c) = self.chunk_at(index, version) {
                    let mut chunk = c.to_vec();
                    chunk[(size % CHUNK) as usize..].fill(0);
                    self.set_chunk(index, version, Some(chunk.into()));
                }
            }
            let first_dropped = size.div_ceil(CHUNK);
            let dropped: Vec<u64> = self
                .chunks
                .range(first_dropped..)
                .map(|(i, _)| *i)
                .collect();
            for index in dropped {
                if self.chunk_at(index, version).is_some() {
                    self.set_chunk(index, version, None);
                }
            }
        }
        if size != old {
            self.set_size(version, size);
        }
    }

    fn set_chunk(&mut self, index: u64, version: u64, data: Option<Arc<[u8]>>) {
        set_at_version(self.chunks.entry(index).or_default(), version, data);
    }

    fn set_size(&mut self, version: u64, size: u64) {
        set_at_version(&mut self.sizes, version, size);
    }

    /// The byte ranges that differ between versions `since` and `until`.
    fn changed(&self, since: u64, until: u64) -> Vec<(u64, u64)> {
        let (since, until) = (since.min(until), since.max(until));
        let mut ranges: Vec<(u64, u64)> = self
            .chunks
            .iter()
            .filter(|(_, h)| h.iter().any(|(v, _)| *v > since && *v <= until))
            .map(|(index, _)| (index * CHUNK, CHUNK))
            .collect();
        let (a, b) = (self.size_at(since), self.size_at(until));
        if a != b {
            ranges.push((a.min(b), a.max(b) - a.min(b)));
            ranges.sort_unstable();
        }
        ranges
    }

    /// Forgets history older than `oldest`, keeping the state as of `oldest`.
    fn prune(&mut self, oldest: u64) {
        for history in self.chunks.values_mut() {
            prune_history(history, oldest);
        }
        self.chunks
            .retain(|_, h| !(h.len() == 1 && h[0].1.is_none()));
        prune_history(&mut self.sizes, oldest);
    }
}

fn at_version<T>(history: &[(u64, T)], version: u64) -> Option<&T> {
    let i = history.partition_point(|(v, _)| *v <= version);
    i.checked_sub(1).map(|i| &history[i].1)
}

fn set_at_version<T>(history: &mut Vec<(u64, T)>, version: u64, value: T) {
    match history.last_mut() {
        Some((v, slot)) if *v == version => *slot = value,
        _ => history.push((version, value)),
    }
}

fn prune_history<T>(history: &mut Vec<(u64, T)>, oldest: u64) {
    let keep_from = history
        .partition_point(|(v, _)| *v <= oldest)
        .saturating_sub(1);
    history.drain(..keep_from);
}

/// Serves a [`Store`] over gRPC.
#[derive(Clone)]
pub struct Service {
    store: Arc<Mutex<Store>>,
    start: Instant,
    latency: Duration,
}

impl Service {
    pub fn new(store: Arc<Mutex<Store>>) -> Service {
        Service {
            store,
            start: Instant::now(),
            latency: Duration::ZERO,
        }
    }

    /// Delays every response, to model a network round trip.
    pub fn with_latency(mut self, latency: Duration) -> Service {
        self.latency = latency;
        self
    }

    /// A Commit carries a whole transaction, so the server accepts messages
    /// of any size instead of gRPC's default 4 MiB.
    pub fn into_server(self) -> proto::grpsqlite_server::GrpsqliteServer<Service> {
        proto::grpsqlite_server::GrpsqliteServer::new(self).max_decoding_message_size(usize::MAX)
    }

    async fn call<T>(
        &self,
        f: impl FnOnce(&mut Store, u64) -> Result<T, Status>,
    ) -> Result<Response<T>, Status> {
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
        let now = self.start.elapsed().as_millis() as u64;
        f(&mut self.store.lock(), now).map(Response::new)
    }
}

#[tonic::async_trait]
impl proto::grpsqlite_server::Grpsqlite for Service {
    async fn open(
        &self,
        r: Request<proto::OpenRequest>,
    ) -> Result<Response<proto::OpenResponse>, Status> {
        self.call(|s, now| s.open(r.into_inner(), now)).await
    }

    async fn close(
        &self,
        r: Request<proto::CloseRequest>,
    ) -> Result<Response<proto::CloseResponse>, Status> {
        self.call(|s, _| s.close(r.into_inner())).await
    }

    async fn heartbeat(
        &self,
        r: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::HeartbeatResponse>, Status> {
        self.call(|s, now| s.heartbeat(r.into_inner(), now)).await
    }

    async fn read(
        &self,
        r: Request<proto::ReadRequest>,
    ) -> Result<Response<proto::ReadResponse>, Status> {
        self.call(|s, _| s.read(r.into_inner())).await
    }

    async fn write(
        &self,
        r: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::WriteResponse>, Status> {
        self.call(|s, now| s.write(r.into_inner(), now)).await
    }

    async fn commit(
        &self,
        r: Request<proto::CommitRequest>,
    ) -> Result<Response<proto::CommitResponse>, Status> {
        self.call(|s, now| s.commit(r.into_inner(), now)).await
    }

    async fn get_file_size(
        &self,
        r: Request<proto::GetFileSizeRequest>,
    ) -> Result<Response<proto::GetFileSizeResponse>, Status> {
        self.call(|s, _| s.get_file_size(r.into_inner())).await
    }

    async fn truncate(
        &self,
        r: Request<proto::TruncateRequest>,
    ) -> Result<Response<proto::TruncateResponse>, Status> {
        self.call(|s, now| s.truncate(r.into_inner(), now)).await
    }

    async fn delete(
        &self,
        r: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::DeleteResponse>, Status> {
        self.call(|s, now| s.delete(r.into_inner(), now)).await
    }

    async fn pragma(
        &self,
        r: Request<proto::PragmaRequest>,
    ) -> Result<Response<proto::PragmaResponse>, Status> {
        self.call(|s, _| s.pragma(r.into_inner())).await
    }

    async fn changed_pages(
        &self,
        r: Request<proto::ChangedPagesRequest>,
    ) -> Result<Response<proto::ChangedPagesResponse>, Status> {
        self.call(|s, _| s.changed_pages(r.into_inner())).await
    }
}

/// Serves `service` on `listener` until the future is dropped. Every request
/// is one small round trip, so connections disable Nagle's algorithm.
pub async fn serve(
    listener: tokio::net::TcpListener,
    service: Service,
) -> Result<(), tonic::transport::Error> {
    let incoming = tonic::transport::server::TcpIncoming::from(listener).with_nodelay(Some(true));
    tonic::transport::Server::builder()
        .add_service(service.into_server())
        .serve_with_incoming(incoming)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn main_ref(database: &str, lease: &str) -> Option<proto::FileRef> {
        Some(proto::FileRef {
            session: String::new(),
            lease: lease.to_string(),
            database: database.to_string(),
            kind: FileKind::Main as i32,
        })
    }

    fn open(store: &mut Store, database: &str, now: u64) -> Result<proto::OpenResponse, Status> {
        store.open(
            proto::OpenRequest {
                client_token: String::new(),
                database: database.to_string(),
                readonly: false,
            },
            now,
        )
    }

    fn commit(
        store: &mut Store,
        lease: &str,
        offset: u64,
        data: &[u8],
        now: u64,
    ) -> Result<proto::CommitResponse, Status> {
        store.commit(
            proto::CommitRequest {
                file: main_ref("db", lease),
                writes: vec![proto::PageWrite {
                    offset,
                    data: Bytes::copy_from_slice(data),
                    checksum: 0,
                }],
                truncate_to: None,
            },
            now,
        )
    }

    fn read(store: &mut Store, version: Bytes, offset: u64, length: u64) -> Vec<u8> {
        store
            .read(proto::ReadRequest {
                file: main_ref("db", ""),
                offset,
                length,
                version,
                checksum: 0,
            })
            .map(|r| r.data.to_vec())
            .unwrap_or_default()
    }

    #[test]
    fn stale_lease_cannot_write() {
        let mut store = Store::new(Options::default());
        let a = open(&mut store, "db", 0).unwrap();
        assert!(open(&mut store, "db", 1).is_err(), "lease is held");
        // A's lease expires; B takes over and A's writes fail.
        let b = open(&mut store, "db", 10_000).unwrap();
        let err = commit(&mut store, &a.lease, 0, b"a", 10_001).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        commit(&mut store, &b.lease, 0, b"b", 10_002).unwrap();
    }

    #[test]
    fn a_missing_database_reads_as_empty_at_version_zero() {
        let mut store = Store::new(Options::default());
        let resp = store
            .read(proto::ReadRequest {
                file: main_ref("none", ""),
                offset: 0,
                length: 100,
                version: Bytes::new(),
                checksum: 0,
            })
            .unwrap_or_else(|e| unreachable!("{e}"));
        assert!(resp.data.is_empty());
        assert_eq!(resp.size, Some(0));
        assert_eq!(resp.version, Bytes::copy_from_slice(&0u64.to_be_bytes()));
        let size = store
            .get_file_size(proto::GetFileSizeRequest {
                file: main_ref("none", ""),
                version: Bytes::new(),
            })
            .unwrap_or_else(|e| unreachable!("{e}"));
        assert_eq!(size.version, resp.version);
    }

    #[test]
    fn reads_at_old_versions() {
        let mut store = Store::new(Options::default());
        let a = open(&mut store, "db", 0).unwrap();
        let v1 = commit(&mut store, &a.lease, 0, &[1; 5000], 1)
            .unwrap()
            .version;
        let v2 = commit(&mut store, &a.lease, 4096, &[2; 10], 2)
            .unwrap()
            .version;
        assert_eq!(read(&mut store, v1.clone(), 4096, 10), vec![1; 10]);
        assert_eq!(read(&mut store, v2, 4096, 10), vec![2; 10]);
        assert_eq!(read(&mut store, v1, 0, 10_000).len(), 5000);
    }

    #[test]
    fn truncate_then_extend_reads_zeros() {
        let mut store = Store::new(Options::default());
        let a = open(&mut store, "db", 0).unwrap();
        commit(&mut store, &a.lease, 0, &[7; 9000], 1).unwrap();
        store
            .commit(
                proto::CommitRequest {
                    file: main_ref("db", &a.lease),
                    writes: vec![proto::PageWrite {
                        offset: 12_000,
                        data: Bytes::from(vec![9; 10]),
                        checksum: 0,
                    }],
                    truncate_to: Some(100),
                },
                2,
            )
            .unwrap();
        let data = read(&mut store, Bytes::new(), 0, 20_000);
        assert_eq!(data.len(), 12_010);
        assert!(data[..100].iter().all(|b| *b == 7));
        assert!(data[100..12_000].iter().all(|b| *b == 0));
        assert!(data[12_000..].iter().all(|b| *b == 9));
    }

    #[test]
    fn checksum_match_skips_data() {
        let mut store = Store::new(Options::default());
        let a = open(&mut store, "db", 0).unwrap();
        commit(&mut store, &a.lease, 0, &[3; 4096], 1).unwrap();
        let resp = store
            .read(proto::ReadRequest {
                file: main_ref("db", ""),
                offset: 0,
                length: 4096,
                version: Bytes::new(),
                checksum: xxh3_64(&[3; 4096]),
            })
            .unwrap();
        assert!(resp.checksum_matched);
        assert!(resp.data.is_empty());
    }
}
