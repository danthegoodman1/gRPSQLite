//! Measures what each workload costs in requests, and in wall time over gRPC
//! with injected latency.
//!
//! ```text
//! bench [--latency-ms N]
//! ```
//!
//! Request counts come from the simulated server, so they are exact and
//! repeat run to run. With `--latency-ms`, the workloads also run over gRPC
//! against an in-process memory server that delays every response by N ms.

// The timed runs measure real time, unlike the simulator.
#![allow(clippy::disallowed_types)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use grpsqlite::cache::CacheSettings;
use grpsqlite::{Backend, GrpcBackend, GrpcConfig};
use grpsqlite_memory::{Options, Service, Store};
use grpsqlite_sim::backend::SimBackend;
use grpsqlite_sim::cache::{CacheFaults, SimCacheStores};
use grpsqlite_sim::harness::Client;
use grpsqlite_sim::prng::Prng;
use parking_lot::Mutex;
use rusqlite::{Connection, params};

const ROWS: u64 = 20_000;
const LOOKUPS: usize = 500;
const SCANS: usize = 50;
const PAGE_CACHE: &str = "PRAGMA cache_size=50";

fn fill(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(&format!(
        "{PAGE_CACHE};
         CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB NOT NULL);
         WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < {ROWS})
         INSERT INTO kv SELECT x, printf('%0200d', x) FROM s;"
    ))
}

fn lookups(conn: &Connection) -> rusqlite::Result<()> {
    let mut rng = Prng::new(1);
    let mut stmt = conn.prepare_cached("SELECT length(v) FROM kv WHERE k = ?1")?;
    for _ in 0..LOOKUPS {
        let k = rng.range(1..=ROWS) as i64;
        let _: i64 = stmt.query_row(params![k], |r| r.get(0))?;
    }
    Ok(())
}

fn scans(conn: &Connection) -> rusqlite::Result<()> {
    let mut rng = Prng::new(2);
    let mut stmt =
        conn.prepare_cached("SELECT sum(length(v)) FROM kv WHERE k BETWEEN ?1 AND ?1 + 1000")?;
    for _ in 0..SCANS {
        let k = rng.range(1..=ROWS) as i64;
        let _: i64 = stmt.query_row(params![k], |r| r.get(0))?;
    }
    Ok(())
}

fn full_scan(conn: &Connection) -> rusqlite::Result<()> {
    let _: i64 = conn.query_row("SELECT sum(length(v)) FROM kv", [], |r| r.get(0))?;
    Ok(())
}

/// Point lookups on `reader` while `writer` updates one row after every
/// tenth. Each `round` writes new values: SQLite skips writing a row whose
/// bytes stay the same.
fn lookups_under_writes(
    writer: &Connection,
    reader: &Connection,
    round: i64,
) -> rusqlite::Result<()> {
    let mut rng = Prng::new(4);
    let mut get = reader.prepare_cached("SELECT length(v) FROM kv WHERE k = ?1")?;
    let mut put = writer.prepare_cached("UPDATE kv SET v = printf('%0200d', ?2) WHERE k = ?1")?;
    for i in 0..LOOKUPS {
        let k = rng.range(1..=ROWS) as i64;
        let _: i64 = get.query_row(params![k], |r| r.get(0))?;
        if i % 10 == 9 {
            let k = rng.range(1..=ROWS) as i64;
            put.execute(params![k, round * LOOKUPS as i64 + i as i64])?;
        }
    }
    Ok(())
}

type Workload = (&'static str, fn(&Connection) -> rusqlite::Result<()>);

const WORKLOADS: [Workload; 3] = [
    ("500 point lookups", lookups),
    ("50 range scans of 1,000 rows", scans),
    ("full scan", full_scan),
];

fn cache() -> Option<CacheSettings> {
    Some(SimCacheStores::settings(
        CacheFaults::default(),
        Prng::new(3),
        256 << 20,
    ))
}

fn requests(sim: &SimBackend) -> u64 {
    sim.counts()
        .iter()
        .filter(|(k, _)| k.as_str() != "heartbeat")
        .map(|(_, v)| v)
        .sum()
}

fn row(sim: &SimBackend, client: &str, workload: &str) {
    println!(
        "| {client} | {workload} | {} | {} | {} |",
        requests(sim),
        sim.count("read.main"),
        sim.count("commit")
    );
    sim.reset_counts();
}

/// Runs each workload once to warm the caches, then counts a second run.
fn warm_rows(sim: &SimBackend, conn: &Connection, client: &str) -> rusqlite::Result<()> {
    for (name, f) in WORKLOADS {
        f(conn)?;
        sim.reset_counts();
        f(conn)?;
        row(sim, client, name);
    }
    Ok(())
}

fn count_requests() -> rusqlite::Result<()> {
    println!(
        "Requests per workload: {ROWS} rows of 200 bytes, a 50-page SQLite cache, warm runs.\n"
    );
    println!("| Client | Workload | Requests | Reads | Commits |");
    println!("| --- | --- | ---: | ---: | ---: |");
    let sim = Arc::new(SimBackend::simple(Options::default()));
    let backend: Arc<dyn Backend> = sim.clone();

    let plain = Client::new(backend.clone(), None);
    let conn = plain.open("bench.db")?;
    sim.reset_counts();
    fill(&conn)?;
    row(&sim, "writer", "insert 20,000 rows in one transaction");
    warm_rows(&sim, &conn, "writer")?;
    drop(conn);

    let cached = Client::new(backend.clone(), cache());
    let conn = cached.open("bench.db")?;
    conn.execute_batch(PAGE_CACHE)?;
    warm_rows(&sim, &conn, "writer, page cache")?;
    drop(conn);
    // Reopen: the cache's pages from the last session are confirmed by one
    // header read and reused.
    sim.reset_counts();
    let conn = cached.open("bench.db")?;
    conn.execute_batch(PAGE_CACHE)?;
    full_scan(&conn)?;
    row(&sim, "writer, page cache, reopened", "full scan");
    drop(conn);

    drop(sim);
    replica_rows(true)?;
    replica_rows(false)
}

/// Replicas on a server with or without ChangedPages. The writer has a page
/// cache, so the requests during its updates are the replica's plus the
/// writer's commits, which the table leaves out.
fn replica_rows(changed_pages: bool) -> rusqlite::Result<()> {
    let sim = Arc::new(SimBackend::simple(Options {
        changed_pages,
        ..Options::default()
    }));
    let backend: Arc<dyn Backend> = sim.clone();
    let writer = Client::new(backend.clone(), cache());
    let wconn = writer.open("bench.db")?;
    fill(&wconn)?;
    let clients = if changed_pages {
        vec![("replica", None), ("replica, page cache", cache())]
    } else {
        vec![("replica, page cache, no ChangedPages", cache())]
    };
    for (label, cache) in clients {
        let reader = Client::new(backend.clone(), cache);
        let conn = reader.open_readonly("bench.db")?;
        conn.execute_batch(PAGE_CACHE)?;
        warm_rows(&sim, &conn, label)?;
        lookups_under_writes(&wconn, &conn, 0)?;
        sim.reset_counts();
        lookups_under_writes(&wconn, &conn, 1)?;
        let commits = sim.count("commit");
        println!(
            "| {label} | 500 point lookups while a writer commits {commits} times | {} | {} | 0 |",
            requests(&sim) - commits,
            sim.count("read.main"),
        );
        sim.reset_counts();
    }
    Ok(())
}

fn timed(latency: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let listener = runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
    let addr = listener.local_addr()?;
    let service =
        Service::new(Arc::new(Mutex::new(Store::new(Options::default())))).with_latency(latency);
    runtime.spawn(async move {
        let _ = grpsqlite_memory::serve(listener, service).await;
    });
    let config = GrpcConfig {
        url: format!("http://{addr}"),
        ..GrpcConfig::default()
    };
    let backend: Arc<dyn Backend> = Arc::new(GrpcBackend::new(&config)?);
    println!(
        "\nWall time over gRPC, {} ms added to every response. Clients without a page cache start cold; clients with one run each workload once to warm it.\n",
        latency.as_millis()
    );
    println!("| Client | Workload | Seconds |");
    println!("| --- | --- | ---: |");
    let writer = Client::new(backend.clone(), None);
    let conn = writer.open("timed.db")?;
    let start = Instant::now();
    fill(&conn)?;
    println!(
        "| writer | insert 20,000 rows in one transaction | {:.3} |",
        start.elapsed().as_secs_f64()
    );
    drop(conn);
    for (label, readonly, cached) in [
        ("writer", false, false),
        ("writer, page cache", false, true),
        ("replica", true, false),
        ("replica, page cache", true, true),
    ] {
        let client = Client::new(backend.clone(), if cached { cache() } else { None });
        for (name, f) in WORKLOADS {
            let conn = if readonly {
                client.open_readonly("timed.db")?
            } else {
                client.open("timed.db")?
            };
            conn.execute_batch(PAGE_CACHE)?;
            if cached {
                f(&conn)?;
            }
            let start = Instant::now();
            f(&conn)?;
            println!(
                "| {label} | {name} | {:.3} |",
                start.elapsed().as_secs_f64()
            );
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut latency = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--latency-ms" => {
                let ms = args.next().ok_or("--latency-ms needs a value")?.parse()?;
                latency = Some(Duration::from_millis(ms));
            }
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    count_requests()?;
    if let Some(latency) = latency {
        timed(latency)?;
    }
    Ok(())
}
