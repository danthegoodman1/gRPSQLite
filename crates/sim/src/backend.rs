//! A [`Backend`] that executes requests against an in-process [`Store`] in
//! simulated time, with injected faults.

use std::collections::BTreeMap;

use grpsqlite::Backend;
use grpsqlite::proto::{self, FileKind};
use grpsqlite_memory::{Options, Store};
use parking_lot::Mutex;
use tonic::Status;

use crate::prng::Prng;

/// Fault rates, in requests per thousand.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// The request fails before the server sees it.
    pub fail_before: u64,
    /// The server applies a change, then the response is lost.
    pub fail_after: u64,
    /// A heartbeat fails before the server sees it.
    pub heartbeat_loss: u64,
}

struct LeaseTrack {
    lease: String,
    database: String,
    interval: u64,
    ttl: u64,
    last_ok_sent: u64,
    next_due: u64,
    lost: bool,
    /// The simulator suppresses this session's heartbeats.
    stalled: bool,
}

pub struct SimState {
    pub store: Store,
    /// Simulated milliseconds.
    pub now: u64,
    pub faults: Faults,
    /// Faults apply only while no [`SimBackend::quietly`] call is running.
    quiet: u32,
    rng: Prng,
    counts: BTreeMap<String, u64>,
    leases: BTreeMap<String, LeaseTrack>,
    /// Every request fails while set.
    partitioned: bool,
    /// Partition after this many more requests.
    partition_after: Option<u64>,
}

pub struct SimBackend {
    state: Mutex<SimState>,
}

fn kind_name(file: &Option<proto::FileRef>) -> &'static str {
    match file.as_ref().map(|f| FileKind::try_from(f.kind)) {
        Some(Ok(FileKind::Main)) => "main",
        Some(Ok(FileKind::Journal)) => "journal",
        Some(Ok(FileKind::Wal)) => "wal",
        _ => "unknown",
    }
}

impl SimBackend {
    pub fn new(opts: Options, faults: Faults, rng: Prng) -> SimBackend {
        SimBackend {
            state: Mutex::new(SimState {
                store: Store::new(opts),
                now: 0,
                faults,
                quiet: 0,
                rng,
                counts: BTreeMap::new(),
                leases: BTreeMap::new(),
                partitioned: false,
                partition_after: None,
            }),
        }
    }

    /// A backend with default options and no faults.
    pub fn simple(opts: Options) -> SimBackend {
        SimBackend::new(opts, Faults::default(), Prng::new(0))
    }

    pub fn with_state<T>(&self, f: impl FnOnce(&mut SimState) -> T) -> T {
        f(&mut self.state.lock())
    }

    /// How many requests named `name` (such as `commit` or `write.main`)
    /// the backend has received.
    pub fn count(&self, name: &str) -> u64 {
        self.state.lock().counts.get(name).copied().unwrap_or(0)
    }

    pub fn counts(&self) -> BTreeMap<String, u64> {
        self.state.lock().counts.clone()
    }

    pub fn reset_counts(&self) {
        self.state.lock().counts.clear();
    }

    /// Runs `f` with faults off, for the simulator's own checks.
    pub fn quietly<T>(&self, f: impl FnOnce() -> T) -> T {
        self.state.lock().quiet += 1;
        let out = f();
        self.state.lock().quiet -= 1;
        out
    }

    /// Fails every request after `requests` more, until [`SimBackend::heal`].
    pub fn partition_after(&self, requests: u64) {
        self.state.lock().partition_after = Some(requests);
    }

    pub fn heal(&self) {
        let mut st = self.state.lock();
        st.partitioned = false;
        st.partition_after = None;
    }

    /// Stops heartbeats for every lease of `database`, so it expires.
    pub fn stall_heartbeats(&self, database: &str) {
        for track in self.state.lock().leases.values_mut() {
            if track.database == database {
                track.stalled = true;
            }
        }
    }

    /// Advances simulated time, sending each heartbeat that falls due.
    pub fn advance(&self, ms: u64) {
        let mut st = self.state.lock();
        let target = st.now + ms;
        let sessions: Vec<String> = st.leases.keys().cloned().collect();
        for session in sessions {
            loop {
                let Some(track) = st.leases.get(&session) else {
                    break;
                };
                if track.lost || track.next_due > target {
                    break;
                }
                let due = track.next_due;
                let stalled = track.stalled;
                let req = proto::HeartbeatRequest {
                    session: session.clone(),
                    lease: track.lease.clone(),
                    database: track.database.clone(),
                };
                let interval = track.interval;
                let loss = st.faults.heartbeat_loss;
                let lost_in_transit =
                    stalled || st.partitioned || (st.quiet == 0 && st.rng.per_mille(loss));
                let result = if lost_in_transit {
                    None
                } else {
                    Some(st.store.heartbeat(req, due))
                };
                *st.counts.entry("heartbeat".to_string()).or_default() += 1;
                let Some(track) = st.leases.get_mut(&session) else {
                    break;
                };
                match result {
                    Some(Ok(_)) => track.last_ok_sent = due,
                    Some(Err(_)) => track.lost = true,
                    None => {}
                }
                track.next_due = due + interval;
            }
        }
        st.now = target;
    }

    pub fn now(&self) -> u64 {
        self.state.lock().now
    }

    pub fn lease_ttl(&self) -> u64 {
        self.state.lock().store.options().lease_ttl_ms
    }

    /// Admits a request: counts it and applies partitions and early faults.
    fn admit(st: &mut SimState, name: &str) -> Result<(), Status> {
        *st.counts.entry(name.to_string()).or_default() += 1;
        if let Some(n) = st.partition_after {
            if n == 0 {
                st.partitioned = true;
                st.partition_after = None;
            } else {
                st.partition_after = Some(n - 1);
            }
        }
        if st.partitioned {
            return Err(Status::unavailable("partitioned"));
        }
        let rate = st.faults.fail_before;
        if st.quiet == 0 && st.rng.per_mille(rate) {
            return Err(Status::unavailable("injected failure before the request"));
        }
        Ok(())
    }

    /// Loses the response of an applied change.
    fn lose_response(st: &mut SimState) -> bool {
        let rate = st.faults.fail_after;
        st.quiet == 0 && st.rng.per_mille(rate)
    }
}

impl Backend for SimBackend {
    fn open(&self, req: proto::OpenRequest) -> Result<proto::OpenResponse, Status> {
        let mut st = self.state.lock();
        Self::admit(&mut st, "open")?;
        let database = req.database.clone();
        let now = st.now;
        let resp = st.store.open(req, now)?;
        if Self::lose_response(&mut st) {
            return Err(Status::unavailable("injected failure after open"));
        }
        if !resp.lease.is_empty() {
            st.leases.insert(
                resp.session.clone(),
                LeaseTrack {
                    lease: resp.lease.clone(),
                    database,
                    interval: resp.heartbeat_interval_millis,
                    ttl: resp.lease_ttl_millis,
                    last_ok_sent: now,
                    next_due: now + resp.heartbeat_interval_millis,
                    lost: false,
                    stalled: false,
                },
            );
        }
        Ok(resp)
    }

    fn close(&self, req: proto::CloseRequest) -> Result<(), Status> {
        let mut st = self.state.lock();
        st.leases.remove(&req.session);
        Self::admit(&mut st, "close")?;
        st.store.close(req).map(drop)
    }

    fn read(&self, req: proto::ReadRequest) -> Result<proto::ReadResponse, Status> {
        let mut st = self.state.lock();
        Self::admit(&mut st, &format!("read.{}", kind_name(&req.file)))?;
        st.store.read(req)
    }

    fn write(&self, req: proto::WriteRequest) -> Result<(), Status> {
        let mut st = self.state.lock();
        Self::admit(&mut st, &format!("write.{}", kind_name(&req.file)))?;
        let now = st.now;
        st.store.write(req, now)?;
        if Self::lose_response(&mut st) {
            return Err(Status::unavailable("injected failure after write"));
        }
        Ok(())
    }

    fn commit(&self, req: proto::CommitRequest) -> Result<proto::CommitResponse, Status> {
        let mut st = self.state.lock();
        Self::admit(&mut st, "commit")?;
        let now = st.now;
        let resp = st.store.commit(req, now)?;
        if Self::lose_response(&mut st) {
            return Err(Status::unavailable("injected failure after commit"));
        }
        Ok(resp)
    }

    fn get_file_size(
        &self,
        req: proto::GetFileSizeRequest,
    ) -> Result<proto::GetFileSizeResponse, Status> {
        let mut st = self.state.lock();
        Self::admit(&mut st, &format!("size.{}", kind_name(&req.file)))?;
        st.store.get_file_size(req)
    }

    fn truncate(&self, req: proto::TruncateRequest) -> Result<(), Status> {
        let mut st = self.state.lock();
        Self::admit(&mut st, &format!("truncate.{}", kind_name(&req.file)))?;
        let now = st.now;
        st.store.truncate(req, now)?;
        if Self::lose_response(&mut st) {
            return Err(Status::unavailable("injected failure after truncate"));
        }
        Ok(())
    }

    fn delete(&self, req: proto::DeleteRequest) -> Result<(), Status> {
        let mut st = self.state.lock();
        Self::admit(&mut st, &format!("delete.{}", kind_name(&req.file)))?;
        let now = st.now;
        st.store.delete(req, now)?;
        if Self::lose_response(&mut st) {
            return Err(Status::unavailable("injected failure after delete"));
        }
        Ok(())
    }

    fn pragma(&self, req: proto::PragmaRequest) -> Result<proto::PragmaResponse, Status> {
        let mut st = self.state.lock();
        Self::admit(&mut st, "pragma")?;
        st.store.pragma(req)
    }

    fn lease_valid(&self, session: &str) -> bool {
        let st = self.state.lock();
        match st.leases.get(session) {
            None => true,
            Some(t) => !t.lost && st.now.saturating_sub(t.last_ok_sent) < t.ttl,
        }
    }
}
