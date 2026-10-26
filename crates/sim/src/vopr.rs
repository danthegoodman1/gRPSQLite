//! VOPR-style simulation: one seed drives a randomly configured server and
//! VFS through random transactions, reopens, replicas, crashes, lease
//! takeovers and faults, checking every answer against a model.
//!
//! Everything runs on one thread against [`SimBackend`], in simulated time,
//! so a seed replays the same run exactly.
//!
//! What the model checks:
//! - the writer reads exactly the model's committed rows;
//! - a commit that fails leaves either the state before it or the state after
//!   it, never a mix (G1, G2);
//! - after a crash at any request, the database holds the last acknowledged
//!   state, or the in-flight transaction's state if its commit was sent;
//! - a client that lost its lease commits nothing, and in normal locking
//!   mode reads nothing (G3);
//! - every read-only transaction sees one committed state (G4);
//! - with a damaged page cache, every answer still matches (G6);
//! - `PRAGMA integrity_check` passes and every VFS ends idle.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use grpsqlite::cache::CacheSettings;
use grpsqlite_memory::Options;
use rusqlite::{Connection, params};
use xxhash_rust::xxh3::Xxh3;

use crate::backend::{Faults, SimBackend};
use crate::cache::{CacheFaults, SimCacheStores};
use crate::harness::Client;
use crate::prng::Prng;

const DB: &str = "vopr.db";

/// The simulated deployment, drawn from the seed.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub atomic: bool,
    pub point_in_time: bool,
    pub page_size: u32,
    pub cache_size: i32,
    pub journal_mode: &'static str,
    pub exclusive: bool,
    pub synchronous: &'static str,
    pub cache: Option<CacheFaults>,
    pub faults: Faults,
    pub crashes: bool,
    pub takeovers: bool,
    pub key_space: u64,
    pub max_value: usize,
    pub steps: u64,
    pub retained_versions: u64,
    /// Ask for exclusive locking on a server with atomic commits, which the
    /// VFS must refuse.
    pub try_exclusive: bool,
}

impl SimConfig {
    pub fn draw(seed: u64) -> SimConfig {
        let mut rng = Prng::stream(seed, "config");
        let atomic = rng.percent(75);
        let point_in_time = atomic && rng.percent(60);
        let journal_mode = if atomic {
            *rng.pick(&["memory", "memory", "delete", "truncate", "persist"])
        } else {
            *rng.pick(&["delete", "truncate", "persist", "wal"])
        };
        // Servers with atomic commits reject exclusive locking.
        let exclusive = journal_mode == "wal" || (!atomic && rng.percent(30));
        let faulty = rng.percent(50);
        let faults = if faulty {
            Faults {
                fail_before: rng.range(0..=20),
                fail_after: rng.range(0..=10),
                heartbeat_loss: rng.range(0..=300),
            }
        } else {
            Faults::default()
        };
        let cache = rng.percent(40).then(|| {
            if rng.percent(50) {
                CacheFaults {
                    bit_flip: rng.range(0..=50),
                    lost: rng.range(0..=50),
                    failed_write: rng.range(0..=50),
                }
            } else {
                CacheFaults::default()
            }
        });
        SimConfig {
            atomic,
            point_in_time,
            page_size: *rng.pick(&[512, 1024, 2048, 4096, 4096, 8192, 16384, 32768, 65536]),
            cache_size: *rng.pick(&[10, 10, 50, 500]),
            journal_mode,
            exclusive,
            synchronous: rng.pick(&["OFF", "NORMAL", "FULL"]),
            cache,
            faults,
            crashes: rng.percent(50),
            takeovers: rng.percent(30),
            key_space: rng.range(20..=2000),
            max_value: *rng.pick(&[16, 200, 2000, 20_000]),
            steps: rng.range(40..=200),
            retained_versions: *rng.pick(&[3, 50, 10_000]),
            try_exclusive: atomic && Prng::stream(seed, "config-exclusive").percent(20),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub steps: u64,
    pub commits: u64,
    pub rollbacks: u64,
    pub failed_transactions: u64,
    pub ambiguous_commits: u64,
    pub crashes: u64,
    pub takeovers: u64,
    pub reopens: u64,
    pub vacuums: u64,
    pub reader_checks: u64,
    pub writer_checks: u64,
    /// A hash of every observable result, equal across replays of a seed.
    pub trace: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    v: Vec<u8>,
    tag: i64,
}

type Rows = BTreeMap<i64, Row>;

fn digest_rows(rows: &Rows) -> u64 {
    let mut h = Xxh3::new();
    for (k, r) in rows {
        h.update(&k.to_le_bytes());
        h.update(&(r.v.len() as u64).to_le_bytes());
        h.update(&r.v);
        h.update(&r.tag.to_le_bytes());
    }
    h.digest()
}

fn digest_db(conn: &Connection) -> rusqlite::Result<u64> {
    let mut stmt = conn.prepare_cached("SELECT k, v, tag FROM kv ORDER BY k")?;
    let mut rows = stmt.query([])?;
    let mut h = Xxh3::new();
    while let Some(row) = rows.next()? {
        let k: i64 = row.get(0)?;
        let v: Vec<u8> = row.get(1)?;
        let tag: i64 = row.get(2)?;
        h.update(&k.to_le_bytes());
        h.update(&(v.len() as u64).to_le_bytes());
        h.update(&v);
        h.update(&tag.to_le_bytes());
    }
    Ok(h.digest())
}

#[derive(Clone, Debug)]
enum Op {
    Put(i64, Vec<u8>, i64),
    Delete(i64),
    DeleteRange(i64, i64),
    Retag(i64, i64, i64),
}

impl Op {
    fn apply(&self, rows: &mut Rows) {
        match self {
            Op::Put(k, v, tag) => {
                rows.insert(
                    *k,
                    Row {
                        v: v.clone(),
                        tag: *tag,
                    },
                );
            }
            Op::Delete(k) => {
                rows.remove(k);
            }
            Op::DeleteRange(lo, hi) => {
                let doomed: Vec<i64> = rows.range(*lo..=*hi).map(|(k, _)| *k).collect();
                for k in doomed {
                    rows.remove(&k);
                }
            }
            Op::Retag(lo, hi, tag) => {
                for (_, r) in rows.range_mut(*lo..=*hi) {
                    r.tag = *tag;
                }
            }
        }
    }

    fn run(&self, conn: &Connection) -> rusqlite::Result<()> {
        match self {
            Op::Put(k, v, tag) => conn
                .prepare_cached("INSERT OR REPLACE INTO kv(k, v, tag) VALUES (?1, ?2, ?3)")?
                .execute(params![k, v, tag])
                .map(drop),
            Op::Delete(k) => conn
                .prepare_cached("DELETE FROM kv WHERE k = ?1")?
                .execute(params![k])
                .map(drop),
            Op::DeleteRange(lo, hi) => conn
                .prepare_cached("DELETE FROM kv WHERE k BETWEEN ?1 AND ?2")?
                .execute(params![lo, hi])
                .map(drop),
            Op::Retag(lo, hi, tag) => conn
                .prepare_cached("UPDATE kv SET tag = ?3 WHERE k BETWEEN ?1 AND ?2")?
                .execute(params![lo, hi, tag])
                .map(drop),
        }
    }
}

/// What happened to a transaction the workload ran.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Committed,
    RolledBack,
    /// Failed before COMMIT was sent.
    Failed,
    /// COMMIT was sent and failed: the outcome is unknown.
    CommitFailed,
}

struct Sim {
    seed: u64,
    cfg: SimConfig,
    rng: Prng,
    /// Decides actions added after the original workload, so adding them
    /// leaves the original draws unchanged.
    extra: Prng,
    backend: Arc<SimBackend>,
    cache: Option<CacheSettings>,
    writer: Client,
    conn: Option<Connection>,
    reader: Option<(Client, Option<Connection>)>,
    model: Rows,
    history: BTreeSet<u64>,
    report: Report,
    verbose: bool,
    step: u64,
}

pub fn run(seed: u64, verbose: bool) -> Result<Report, String> {
    run_with(seed, SimConfig::draw(seed), verbose)
}

pub fn run_with(seed: u64, cfg: SimConfig, verbose: bool) -> Result<Report, String> {
    let opts = Options {
        atomic_commit: cfg.atomic,
        point_in_time_reads: cfg.point_in_time,
        heartbeat_interval_ms: 1000,
        lease_ttl_ms: 5000,
        retained_versions: cfg.retained_versions,
    };
    let backend = Arc::new(SimBackend::new(
        opts,
        cfg.faults.clone(),
        Prng::stream(seed, "faults"),
    ));
    let cache = cfg.cache.clone().map(|faults| {
        let max_bytes = *Prng::stream(seed, "cache-size").pick(&[64 * 1024, 1 << 20, 64 << 20]);
        SimCacheStores::settings(faults, Prng::stream(seed, "cache"), max_bytes)
    });
    if verbose {
        eprintln!("seed {seed}: {cfg:?}");
    }
    let writer = Client::new(backend.clone(), cache.clone());
    let mut sim = Sim {
        seed,
        rng: Prng::stream(seed, "workload"),
        extra: Prng::stream(seed, "workload-extra"),
        cfg,
        backend,
        cache,
        writer,
        conn: None,
        reader: None,
        model: Rows::new(),
        history: BTreeSet::new(),
        report: Report::default(),
        verbose,
        step: 0,
    };
    let result = sim.run();
    sim.shutdown();
    result.map(|()| sim.report)
}

impl Sim {
    fn fail(&self, msg: impl std::fmt::Display) -> String {
        format!("seed {} step {}: {msg}", self.seed, self.step)
    }

    fn note(&mut self, event: &str, value: u64) {
        let mut h = Xxh3::new();
        h.update(&self.report.trace.to_le_bytes());
        h.update(event.as_bytes());
        h.update(&value.to_le_bytes());
        self.report.trace = h.digest();
        if self.verbose {
            eprintln!(
                "  [{}] t={} {event} {value:#x}",
                self.step,
                self.backend.now()
            );
        }
    }

    fn run(&mut self) -> Result<(), String> {
        self.open_writer()?;
        self.quiet_setup()?;
        self.history.insert(digest_rows(&self.model));
        for step in 0..self.cfg.steps {
            self.step = step;
            self.report.steps += 1;
            let dt = self.rng.range(0..=1500);
            self.backend.advance(dt);
            if self.cfg.point_in_time && self.extra.percent(5) {
                self.long_reader()?;
            }
            let roll = self.rng.below(100);
            match roll {
                0..=49 => self.transaction(false)?,
                50..=59 => self.check_writer()?,
                60..=64 => self.integrity()?,
                65..=69 => self.reopen()?,
                70..=73 => self.vacuum()?,
                74..=85 if self.cfg.point_in_time => self.check_reader()?,
                86..=91 if self.cfg.crashes => self.crash()?,
                92..=94 if self.cfg.takeovers => self.takeover()?,
                _ => self.transaction(true)?,
            }
        }
        self.final_check()
    }

    fn conn(&self) -> &Connection {
        match &self.conn {
            Some(c) => c,
            None => unreachable!("the writer connection is open between steps"),
        }
    }

    /// Opens the writer, retrying while faults or a stale lease interfere.
    fn open_writer(&mut self) -> Result<(), String> {
        for attempt in 0..40 {
            let opened = self.writer.open(DB).and_then(|c| {
                self.configure(&c)?;
                Ok(c)
            });
            match opened {
                Ok(c) => {
                    self.conn = Some(c);
                    self.note("open", attempt);
                    return Ok(());
                }
                Err(e) => {
                    self.note("open-retry", attempt);
                    if self.verbose {
                        eprintln!("    open failed: {e}");
                    }
                    let ttl = self.backend.lease_ttl();
                    self.backend.advance(ttl / 4 + 1);
                }
            }
        }
        // Give up on faults, and let any lease a failed close left behind
        // expire; the open must then succeed.
        self.backend.advance(self.backend.lease_ttl() + 1);
        let c = self
            .backend
            .quietly(|| {
                let c = self.writer.open(DB)?;
                self.configure(&c)?;
                Ok::<_, rusqlite::Error>(c)
            })
            .map_err(|e| self.fail(format!("open without faults failed: {e}")))?;
        self.conn = Some(c);
        Ok(())
    }

    /// Applies the configuration's pragmas. `locking_mode` comes first: WAL
    /// without shared memory needs exclusive locking before the first read.
    /// These use plain SQL because rusqlite's pragma helpers read the
    /// database before running the pragma.
    fn configure(&self, c: &Connection) -> rusqlite::Result<()> {
        c.busy_timeout(std::time::Duration::ZERO)?;
        if self.cfg.exclusive {
            c.execute_batch("PRAGMA locking_mode=exclusive")?;
        } else if self.cfg.try_exclusive {
            let _ = c.execute_batch("PRAGMA locking_mode=exclusive");
        }
        c.execute_batch(&format!(
            "PRAGMA page_size={}; PRAGMA cache_size={}; PRAGMA synchronous={};",
            self.cfg.page_size, self.cfg.cache_size, self.cfg.synchronous
        ))?;
        let mode: String = c.query_row(
            &format!("PRAGMA journal_mode={}", self.cfg.journal_mode),
            [],
            |r| r.get(0),
        )?;
        if !mode.eq_ignore_ascii_case(self.cfg.journal_mode) {
            return Err(rusqlite::Error::InvalidParameterName(format!(
                "journal_mode {mode} instead of {}",
                self.cfg.journal_mode
            )));
        }
        Ok(())
    }

    fn quiet_setup(&mut self) -> Result<(), String> {
        let backend = self.backend.clone();
        backend
            .quietly(|| {
                self.conn().execute_batch(
                    "CREATE TABLE IF NOT EXISTS kv(k INTEGER PRIMARY KEY, v BLOB NOT NULL, tag INTEGER NOT NULL);
                     CREATE INDEX IF NOT EXISTS kv_tag ON kv(tag);",
                )
            })
            .map_err(|e| self.fail(format!("creating the schema: {e}")))
    }

    fn random_op(&mut self) -> Op {
        let k = self.rng.range(1..=self.cfg.key_space) as i64;
        match self.rng.below(10) {
            0..=5 => {
                let len = if self.rng.percent(10) {
                    self.rng.index(self.cfg.max_value + 1)
                } else {
                    self.rng.index(self.cfg.max_value.min(200) + 1)
                };
                let v = self.rng.bytes(len);
                Op::Put(k, v, self.rng.below(16) as i64)
            }
            6 | 7 => Op::Delete(k),
            8 => Op::DeleteRange(k, k + self.rng.below(20) as i64),
            _ => Op::Retag(k, k + self.rng.below(50) as i64, self.rng.below(16) as i64),
        }
    }

    /// Runs one random transaction against the writer. Returns its outcome
    /// and the rows it would commit, or a violation.
    fn run_transaction(&mut self, big: bool) -> Result<(Outcome, Rows), String> {
        let n = if big {
            self.rng.range(50..=400)
        } else {
            self.rng.range(1..=20)
        };
        let ops: Vec<Op> = (0..n).map(|_| self.random_op()).collect();
        let savepoint = self
            .rng
            .percent(30)
            .then(|| (self.rng.index(ops.len()), self.rng.percent(50)));
        let rollback = self.rng.percent(10);
        let reader_mid = self.cfg.point_in_time && self.rng.percent(15);
        let immediate = self.rng.percent(50);
        let mut scratch = self.model.clone();
        let body = (|| -> rusqlite::Result<()> {
            self.conn().execute_batch(if immediate {
                "BEGIN IMMEDIATE"
            } else {
                "BEGIN"
            })?;
            let mut saved = None;
            for (i, op) in ops.iter().enumerate() {
                if savepoint.is_some_and(|(start, _)| start == i) {
                    self.conn().execute_batch("SAVEPOINT sp")?;
                    saved = Some(scratch.clone());
                }
                op.run(self.conn())?;
                op.apply(&mut scratch);
            }
            if let (Some((_, undo)), Some(before)) = (savepoint, saved) {
                if undo {
                    self.conn().execute_batch("ROLLBACK TO sp; RELEASE sp")?;
                    scratch = before;
                } else {
                    self.conn().execute_batch("RELEASE sp")?;
                }
            }
            Ok(())
        })();
        let outcome = match body {
            Err(e) => self.abandon(e, Outcome::Failed),
            Ok(()) => {
                if reader_mid {
                    // A replica reading mid-transaction must see committed state.
                    self.check_reader()?;
                }
                if rollback {
                    match self.conn().execute_batch("ROLLBACK") {
                        Ok(()) => Outcome::RolledBack,
                        Err(e) => self.abandon(e, Outcome::Failed),
                    }
                } else {
                    match self.conn().execute_batch("COMMIT") {
                        Ok(()) => Outcome::Committed,
                        Err(e) => self.abandon(e, Outcome::CommitFailed),
                    }
                }
            }
        };
        Ok((outcome, scratch))
    }

    /// Ends a transaction after a statement failed.
    fn abandon(&self, err: rusqlite::Error, outcome: Outcome) -> Outcome {
        if self.verbose {
            eprintln!("    transaction failed: {err}");
        }
        let _ = self.conn().execute_batch("ROLLBACK");
        outcome
    }

    fn transaction(&mut self, big: bool) -> Result<(), String> {
        let (outcome, after) = self.run_transaction(big)?;
        self.note("txn", outcome as u64);
        match outcome {
            Outcome::Committed => {
                self.model = after;
                self.history.insert(digest_rows(&self.model));
                self.report.commits += 1;
            }
            Outcome::RolledBack => self.report.rollbacks += 1,
            Outcome::Failed => {
                self.report.failed_transactions += 1;
                self.resolve(None)?;
            }
            Outcome::CommitFailed => {
                self.report.failed_transactions += 1;
                self.report.ambiguous_commits += 1;
                self.resolve(Some(after))?;
            }
        }
        Ok(())
    }

    /// After a failed transaction, reads the database without faults and
    /// adopts whichever acceptable state it holds.
    fn resolve(&mut self, after: Option<Rows>) -> Result<(), String> {
        let digest = self.quiet_digest()?;
        let before = digest_rows(&self.model);
        self.note("resolve", digest);
        if digest == before {
            return Ok(());
        }
        if let Some(after) = after {
            if digest == digest_rows(&after) {
                self.model = after;
                self.history.insert(digest);
                return Ok(());
            }
        }
        Err(self.fail("a failed transaction left a state that is neither before nor after it"))
    }

    /// The writer's digest with faults off. A writer that lost its lease is
    /// reopened once the lease expires.
    fn quiet_digest(&mut self) -> Result<u64, String> {
        let backend = self.backend.clone();
        if let Ok(d) = backend.quietly(|| digest_db(self.conn())) {
            return Ok(d);
        }
        self.conn = None;
        self.backend.advance(self.backend.lease_ttl() + 1);
        backend.quietly(|| self.open_writer())?;
        backend
            .quietly(|| digest_db(self.conn()))
            .map_err(|e| self.fail(format!("reading without faults failed: {e}")))
    }

    fn check_writer(&mut self) -> Result<(), String> {
        let result = digest_db(self.conn());
        if let Ok(d) = result {
            self.report.writer_checks += 1;
            self.note("check", d);
            if d != digest_rows(&self.model) {
                return Err(self.fail("the writer's rows differ from the model"));
            }
        }
        Ok(())
    }

    /// Runs `integrity_check` with faults off: under faults it reports failed
    /// page reads in its output.
    fn integrity(&mut self) -> Result<(), String> {
        let backend = self.backend.clone();
        let result: rusqlite::Result<String> = backend.quietly(|| {
            self.conn()
                .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        });
        if let Ok(msg) = result {
            if msg != "ok" {
                return Err(self.fail(format!("integrity_check: {msg}")));
            }
        }
        Ok(())
    }

    fn reopen(&mut self) -> Result<(), String> {
        self.conn = None;
        if !self.writer.core.is_idle() {
            return Err(self.fail("the writer's VFS holds state after its last connection closed"));
        }
        self.report.reopens += 1;
        self.open_writer()
    }

    fn vacuum(&mut self) -> Result<(), String> {
        if self.rng.percent(30) && self.cfg.journal_mode != "wal" {
            let size = *self.rng.pick(&[512u32, 1024, 4096, 16384, 65536]);
            let _ = self
                .conn()
                .execute_batch(&format!("PRAGMA page_size={size}"));
        }
        let result = self.conn().execute_batch("VACUUM");
        self.note("vacuum", result.is_ok() as u64);
        self.report.vacuums += 1;
        if result.is_err() {
            let _ = self.conn().execute_batch("ROLLBACK");
            self.resolve(None)?;
        }
        Ok(())
    }

    /// The replica's connection, opened on first use.
    fn reader_conn(&mut self) -> Option<&Connection> {
        if self.reader.is_none() {
            let client = Client::new(self.backend.clone(), self.cache.clone());
            self.reader = Some((client, None));
        }
        let (client, conn) = self.reader.as_mut()?;
        if conn.is_none() {
            let c = client.open_readonly(DB).ok()?;
            let _ = c.execute_batch(&format!("PRAGMA cache_size={}", self.cfg.cache_size));
            *conn = Some(c);
        }
        conn.as_ref()
    }

    /// Holds a read-only transaction open while the writer commits: every
    /// read in it must see the version its first read saw.
    fn long_reader(&mut self) -> Result<(), String> {
        let Some(c) = self.reader_conn() else {
            return Ok(());
        };
        if c.execute_batch("BEGIN").is_err() {
            return Ok(());
        }
        let first = digest_db(c);
        let Some((_, conn)) = &mut self.reader else {
            return Ok(());
        };
        let conn = conn.take();
        let mut result = Ok(());
        if let (Ok(d), Some(c)) = (&first, &conn) {
            self.note("long-reader", *d);
            if !self.history.contains(d) {
                result = Err(self.fail("a read-only transaction saw a state that was never committed"));
            } else {
                for _ in 0..self.extra.range(1..=3) {
                    self.transaction(false)?;
                }
                if let Ok(again) = digest_db(c) {
                    self.report.reader_checks += 1;
                    if again != *d {
                        result = Err(self.fail("a read-only transaction's view changed while it ran"));
                    }
                }
            }
        }
        if let Some(c) = &conn {
            let _ = c.execute_batch("ROLLBACK");
        }
        if let Some((_, slot)) = &mut self.reader {
            *slot = conn;
        }
        result
    }

    fn check_reader(&mut self) -> Result<(), String> {
        let Some(c) = self.reader_conn() else {
            return Ok(());
        };
        let result = digest_db(c);
        if self.rng.percent(20) {
            if let Some((_, conn)) = &mut self.reader {
                *conn = None;
            }
        }
        if let Ok(d) = result {
            self.report.reader_checks += 1;
            self.note("reader", d);
            if !self.history.contains(&d) {
                return Err(
                    self.fail("a read-only transaction saw a state that was never committed")
                );
            }
        }
        Ok(())
    }

    /// Partitions the client after a random number of requests, runs a
    /// transaction, drops every connection as a crash would, and recovers.
    fn crash(&mut self) -> Result<(), String> {
        let after = self.rng.range(0..=60);
        self.backend.partition_after(after);
        let big = self.rng.percent(50);
        let (outcome, state) = self.run_transaction(big)?;
        self.note("crash", outcome as u64);
        self.conn = None;
        if let Some((_, conn)) = &mut self.reader {
            *conn = None;
        }
        self.backend.heal();
        if !self.writer.core.is_idle() {
            return Err(self.fail("the writer's VFS holds state after a crash"));
        }
        self.backend.advance(self.backend.lease_ttl() + 1);
        self.report.crashes += 1;
        self.open_writer()?;
        let digest = self.quiet_digest()?;
        let after_digest = digest_rows(&state);
        let acceptable = match outcome {
            Outcome::Committed => digest == after_digest,
            Outcome::RolledBack | Outcome::Failed => digest == digest_rows(&self.model),
            Outcome::CommitFailed => digest == after_digest || digest == digest_rows(&self.model),
        };
        if !acceptable {
            return Err(self.fail(format!(
                "after a crash ({outcome:?}) the database holds an unexpected state"
            )));
        }
        if digest == after_digest && outcome != Outcome::RolledBack && outcome != Outcome::Failed {
            self.model = state;
            self.history.insert(digest);
        }
        if outcome == Outcome::Committed {
            self.report.commits += 1;
        }
        Ok(())
    }

    /// Lets the writer's lease expire, has a second client take the
    /// database over, and checks the first can no longer use it.
    fn takeover(&mut self) -> Result<(), String> {
        self.backend.stall_heartbeats(DB);
        self.backend.advance(self.backend.lease_ttl() + 1);
        let next = Client::new(self.backend.clone(), self.cache.clone());
        let old = std::mem::replace(&mut self.writer, next);
        let old_conn = self.conn.take();
        self.open_writer()?;
        self.report.takeovers += 1;
        if let Some(c) = &old_conn {
            let read = c.query_row("SELECT count(*) FROM kv", [], |r| r.get::<_, i64>(0));
            self.note("stale-read", read.is_ok() as u64);
            if read.is_ok() && !self.cfg.exclusive {
                return Err(self.fail("a client read after losing its lease"));
            }
            let write = c.execute_batch(
                "BEGIN; INSERT OR REPLACE INTO kv(k, v, tag) VALUES (-1, x'00', 0); COMMIT;",
            );
            self.note("stale-write", write.is_ok() as u64);
            if write.is_ok() {
                return Err(self.fail("a client committed after losing its lease"));
            }
            let _ = c.execute_batch("ROLLBACK");
        }
        drop(old_conn);
        if !old.core.is_idle() {
            return Err(self.fail("a VFS that lost its lease holds state after closing"));
        }
        let digest = self.quiet_digest()?;
        if digest != digest_rows(&self.model) {
            return Err(self.fail("after a takeover the database differs from the model"));
        }
        Ok(())
    }

    fn final_check(&mut self) -> Result<(), String> {
        let digest = self.quiet_digest()?;
        if digest != digest_rows(&self.model) {
            return Err(self.fail("the final database differs from the model"));
        }
        let backend = self.backend.clone();
        let msg: String = backend
            .quietly(|| {
                self.conn()
                    .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            })
            .map_err(|e| self.fail(format!("final integrity_check failed: {e}")))?;
        if msg != "ok" {
            return Err(self.fail(format!("final integrity_check: {msg}")));
        }
        self.note("final", digest);
        // The server, read through a new client, must hold the model too.
        backend.quietly(|| {
            self.conn = None;
            if let Some((_, conn)) = &mut self.reader {
                *conn = None;
            }
        });
        if !self.writer.core.is_idle() {
            return Err(self.fail("the writer's VFS holds state after its last connection closed"));
        }
        let fresh = Client::new(self.backend.clone(), None);
        let digest = backend
            .quietly(|| {
                let c = fresh.open(DB)?;
                self.configure(&c)?;
                digest_db(&c)
            })
            .map_err(|e| self.fail(format!("reading through a new client failed: {e}")))?;
        if digest != digest_rows(&self.model) {
            return Err(self.fail("the server's database differs from the model"));
        }
        if !fresh.core.is_idle() {
            return Err(self.fail("a VFS holds state after its last connection closed"));
        }
        Ok(())
    }

    fn shutdown(&mut self) {
        self.conn = None;
        self.reader = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_ops_match_sql() {
        let conn = Connection::open_in_memory().unwrap_or_else(|e| unreachable!("{e}"));
        conn.execute_batch(
            "CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB NOT NULL, tag INTEGER NOT NULL)",
        )
        .unwrap_or_else(|e| unreachable!("{e}"));
        let mut rng = Prng::new(3);
        let mut rows = Rows::new();
        for _ in 0..500 {
            let k = rng.range(1..=50) as i64;
            let op = match rng.below(4) {
                0 => {
                    let len = rng.index(30);
                    Op::Put(k, rng.bytes(len), rng.below(5) as i64)
                }
                1 => Op::Delete(k),
                2 => Op::DeleteRange(k, k + 5),
                _ => Op::Retag(k, k + 10, rng.below(5) as i64),
            };
            op.run(&conn).unwrap_or_else(|e| unreachable!("{e}"));
            op.apply(&mut rows);
            assert_eq!(digest_db(&conn).ok(), Some(digest_rows(&rows)));
        }
    }
}
