//! The production [`Backend`]: protocol requests over gRPC.
//!
//! One multi-thread runtime with a single worker owns one channel and every
//! heartbeat task. The channel's background work runs on that worker between
//! SQLite calls, so idle periods leave the connection healthy. The runtime
//! starts with the first request, so a host that loads SQLite and then forks
//! starts its threads in the child.

#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use grpsqlite_proto as proto;
use parking_lot::Mutex;
use proto::grpsqlite_client::GrpsqliteClient;
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

use crate::backend::{Backend, is_lease_error};
use crate::config::GrpcConfig;

pub struct GrpcBackend {
    endpoint: Endpoint,
    connection: OnceLock<Result<Connection, String>>,
    leases: Mutex<BTreeMap<String, Arc<LeaseState>>>,
    start: Instant,
}

struct Connection {
    runtime: tokio::runtime::Runtime,
    client: GrpsqliteClient<Channel>,
}

/// What the heartbeat task knows about one lease.
struct LeaseState {
    lost: AtomicBool,
    /// When the last accepted heartbeat (or the Open) was sent, in
    /// milliseconds since the backend started.
    last_ok_sent: AtomicU64,
    ttl_ms: u64,
    task: tokio::task::JoinHandle<()>,
}

const DEFAULT_HEARTBEAT_MS: u64 = 5000;

impl GrpcBackend {
    /// Checks the configuration. The runtime and channel start with the first
    /// request.
    pub fn new(config: &GrpcConfig) -> Result<GrpcBackend, String> {
        let endpoint = Endpoint::from_shared(config.url.clone())
            .map_err(|e| format!("GRPSQLITE_URL={:?}: {e}", config.url))?
            .connect_timeout(config.connect_timeout)
            .timeout(config.rpc_timeout)
            .http2_keep_alive_interval(config.keepalive_interval)
            .keep_alive_timeout(config.keepalive_timeout)
            .keep_alive_while_idle(true);
        Ok(GrpcBackend {
            endpoint,
            connection: OnceLock::new(),
            leases: Mutex::new(BTreeMap::new()),
            start: Instant::now(),
        })
    }

    fn connection(&self) -> Result<&Connection, Status> {
        self.connection
            .get_or_init(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("grpsqlite")
                    .enable_all()
                    .build()
                    .map_err(|e| format!("starting the gRPC runtime: {e}"))?;
                let channel = {
                    let _guard = runtime.enter();
                    self.endpoint.connect_lazy()
                };
                Ok(Connection {
                    runtime,
                    client: GrpsqliteClient::new(channel),
                })
            })
            .as_ref()
            .map_err(|e| Status::unavailable(e.clone()))
    }

    fn client(&self) -> Result<GrpsqliteClient<Channel>, Status> {
        Ok(self.connection()?.client.clone())
    }

    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Runs `fut` on the runtime and waits for it. A std channel carries the
    /// result, so this works even when the caller is itself inside a runtime.
    fn call<T, F>(&self, fut: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: Future<Output = Result<tonic::Response<T>, Status>> + Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.connection()?.runtime.spawn(async move {
            let _ = tx.send(fut.await.map(tonic::Response::into_inner));
        });
        rx.recv()
            .unwrap_or_else(|_| Err(Status::internal("the gRPC runtime stopped")))
    }

    fn start_heartbeat(
        &self,
        session: &str,
        database: &str,
        resp: &proto::OpenResponse,
        sent_at: u64,
    ) -> Result<(), Status> {
        let connection = self.connection()?;
        let interval = match resp.heartbeat_interval_millis {
            0 => DEFAULT_HEARTBEAT_MS,
            ms => ms,
        };
        let ttl_ms = match resp.lease_ttl_millis {
            0 => 3 * interval,
            ms => ms,
        };
        let req = proto::HeartbeatRequest {
            session: session.to_string(),
            lease: resp.lease.clone(),
            database: database.to_string(),
        };
        let state = Arc::new_cyclic(|weak: &std::sync::Weak<LeaseState>| {
            let weak = weak.clone();
            let mut client = connection.client.clone();
            let start = self.start;
            let task = connection.runtime.spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(interval));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let sent = start.elapsed().as_millis() as u64;
                    let result = client.heartbeat(req.clone()).await;
                    let Some(state) = weak.upgrade() else { return };
                    match result {
                        Ok(_) => state.last_ok_sent.store(sent, Ordering::Release),
                        Err(status) if is_lease_error(&status) => {
                            log::error!("lease for {} lost: {}", req.database, status.message());
                            state.lost.store(true, Ordering::Release);
                            return;
                        }
                        Err(status) => {
                            log::warn!("heartbeat for {} failed: {status}", req.database)
                        }
                    }
                }
            });
            LeaseState {
                lost: AtomicBool::new(false),
                last_ok_sent: AtomicU64::new(sent_at),
                ttl_ms,
                task,
            }
        });
        self.leases.lock().insert(session.to_string(), state);
        Ok(())
    }
}

impl Backend for GrpcBackend {
    fn open(&self, req: proto::OpenRequest) -> Result<proto::OpenResponse, Status> {
        let database = req.database.clone();
        let sent_at = self.now_ms();
        let mut c = self.client()?;
        let resp = self.call(async move { c.open(req).await })?;
        if !resp.lease.is_empty() {
            self.start_heartbeat(&resp.session, &database, &resp, sent_at)?;
        }
        Ok(resp)
    }

    fn close(&self, req: proto::CloseRequest) -> Result<(), Status> {
        if let Some(state) = self.leases.lock().remove(&req.session) {
            state.task.abort();
        }
        let mut c = self.client()?;
        self.call(async move { c.close(req).await }).map(drop)
    }

    fn read(&self, req: proto::ReadRequest) -> Result<proto::ReadResponse, Status> {
        let mut c = self.client()?;
        self.call(async move { c.read(req).await })
    }

    fn write(&self, req: proto::WriteRequest) -> Result<(), Status> {
        let mut c = self.client()?;
        self.call(async move { c.write(req).await }).map(drop)
    }

    fn commit(&self, req: proto::CommitRequest) -> Result<proto::CommitResponse, Status> {
        let mut c = self.client()?;
        self.call(async move { c.commit(req).await })
    }

    fn get_file_size(
        &self,
        req: proto::GetFileSizeRequest,
    ) -> Result<proto::GetFileSizeResponse, Status> {
        let mut c = self.client()?;
        self.call(async move { c.get_file_size(req).await })
    }

    fn truncate(&self, req: proto::TruncateRequest) -> Result<(), Status> {
        let mut c = self.client()?;
        self.call(async move { c.truncate(req).await }).map(drop)
    }

    fn delete(&self, req: proto::DeleteRequest) -> Result<(), Status> {
        let mut c = self.client()?;
        self.call(async move { c.delete(req).await }).map(drop)
    }

    fn pragma(&self, req: proto::PragmaRequest) -> Result<proto::PragmaResponse, Status> {
        let mut c = self.client()?;
        self.call(async move { c.pragma(req).await })
    }

    fn lease_valid(&self, session: &str) -> bool {
        let Some(state) = self.leases.lock().get(session).cloned() else {
            return true;
        };
        !state.lost.load(Ordering::Acquire)
            && self
                .now_ms()
                .saturating_sub(state.last_ok_sent.load(Ordering::Acquire))
                < state.ttl_ms
    }
}
