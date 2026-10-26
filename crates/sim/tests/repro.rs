//! The bugs found in the review of `e2a2d84` (R1–R11 in `PLAN.md`) and by
//! the VOPR since, each replayed against the simulated server.

use std::sync::Arc;

use grpsqlite::EnvConfig;
use grpsqlite_memory::Options;
use grpsqlite_sim::backend::SimBackend;
use grpsqlite_sim::cache::{CacheFaults, SimCacheStores};
use grpsqlite_sim::harness::{Client, sqlite_code};
use grpsqlite_sim::prng::Prng;
use rusqlite::{Connection, ErrorCode};

fn atomic() -> Options {
    Options::default()
}

fn non_atomic() -> Options {
    Options {
        atomic_commit: false,
        point_in_time_reads: false,
        ..Options::default()
    }
}

fn setup(opts: Options) -> (Arc<SimBackend>, Client) {
    let backend = Arc::new(SimBackend::simple(opts));
    let client = Client::new(backend.clone(), None);
    (backend, client)
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).expect(sql)
}

fn integrity(conn: &Connection) -> String {
    conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("integrity_check")
}

/// Crashes the client: every request fails, so dropping the connection
/// releases nothing on the server; then waits out the lease.
fn crash(backend: &SimBackend, conn: Connection) {
    backend.partition_after(0);
    drop(conn);
    backend.heal();
    backend.advance(backend.lease_ttl() + 1);
}

const FILL: &str = "CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
    WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 5000)
    INSERT INTO t1 SELECT x, 'old' || printf('%097d', x) FROM s;";

#[test]
fn r1_crash_before_commit_leaves_no_trace() {
    let (backend, client) = setup(atomic());
    let conn = client.open("r1.db").unwrap();
    conn.execute_batch("PRAGMA journal_mode=memory; PRAGMA cache_size=10;")
        .unwrap();
    conn.execute_batch(FILL).unwrap();
    conn.execute_batch("BEGIN; UPDATE t1 SET b = 'new' || printf('%097d', a);")
        .unwrap();
    assert_eq!(
        backend.count("write.main"),
        0,
        "nothing reaches the server before COMMIT"
    );
    crash(&backend, conn);
    let conn = client.open("r1.db").unwrap();
    assert_eq!(
        count(&conn, "SELECT count(*) FROM t1 WHERE b LIKE 'new%'"),
        0
    );
    assert_eq!(
        count(&conn, "SELECT count(*) FROM t1 WHERE b LIKE 'old%'"),
        5000
    );
    assert_eq!(integrity(&conn), "ok");
}

#[test]
fn r2_spilled_pages_commit_atomically() {
    let (backend, client) = setup(atomic());
    let conn = client.open("r2.db").unwrap();
    conn.execute_batch("PRAGMA journal_mode=memory; PRAGMA cache_size=10; CREATE TABLE t1(a, b);")
        .unwrap();
    backend.reset_counts();
    conn.execute_batch(
        "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 20000)
         INSERT INTO t1 SELECT x, printf('%0100d', x) FROM s;",
    )
    .unwrap();
    assert_eq!(backend.count("write.main"), 0);
    assert_eq!(backend.count("commit"), 1, "one transaction, one commit");
    assert_eq!(count(&conn, "SELECT count(*) FROM t1"), 20000);
}

#[test]
fn r3_crash_mid_transaction_rolls_back_from_the_journal() {
    let (backend, client) = setup(non_atomic());
    let conn = client.open("r3.db").unwrap();
    conn.execute_batch("PRAGMA journal_mode=delete;").unwrap();
    conn.execute_batch(FILL).unwrap();
    backend.reset_counts();
    conn.execute_batch(
        "PRAGMA cache_size=10; BEGIN;
         DELETE FROM t1 WHERE a % 2 = 0;
         INSERT INTO t1 SELECT a + 100000, printf('%0300d', a) FROM t1;",
    )
    .unwrap();
    assert!(
        backend.count("write.main") > 0,
        "spilled pages reached the server"
    );
    crash(&backend, conn);
    let conn = client.open("r3.db").unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM t1"), 5000);
    assert_eq!(
        count(&conn, "SELECT count(*) FROM t1 WHERE b LIKE 'old%'"),
        5000
    );
    assert_eq!(integrity(&conn), "ok");
}

#[test]
fn r4_every_page_size_works() {
    for opts in [atomic(), non_atomic()] {
        for page_size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let (_backend, client) = setup(opts.clone());
            let path = format!("r4-{page_size}.db");
            let conn = client.open(&path).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA page_size={page_size}; CREATE TABLE t1(a, b);
                 WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 2000)
                 INSERT INTO t1 SELECT x, printf('%050d', x) FROM s;"
            ))
            .unwrap();
            assert_eq!(count(&conn, "PRAGMA page_size"), page_size);
            drop(conn);
            let conn = client.open(&path).unwrap();
            assert_eq!(
                count(&conn, "SELECT count(*) FROM t1"),
                2000,
                "page size {page_size}"
            );
            assert_eq!(integrity(&conn), "ok", "page size {page_size}");
        }
    }
}

#[test]
fn r5_reopening_another_database_keeps_them_apart() {
    let (_backend, client) = setup(atomic());
    let b = client.open("b.db").unwrap();
    b.execute_batch("CREATE TABLE b_tbl(y); INSERT INTO b_tbl VALUES (10);")
        .unwrap();
    drop(b);
    let a = client.open("a.db").unwrap();
    a.execute_batch(
        "CREATE TABLE a_tbl(x); CREATE TABLE a_tbl2(x); INSERT INTO a_tbl VALUES (1), (2), (3);",
    )
    .unwrap();
    drop(a);
    let b = client.open("b.db").unwrap();
    let names: Vec<String> = b
        .prepare("SELECT name FROM sqlite_schema")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, ["b_tbl"]);
    assert_eq!(count(&b, "SELECT y FROM b_tbl"), 10);
    // Both open at once, through ATTACH.
    b.execute_batch(
        "ATTACH 'a.db' AS a; INSERT INTO a.a_tbl VALUES (4); INSERT INTO b_tbl VALUES (11);",
    )
    .unwrap();
    assert_eq!(count(&b, "SELECT count(*) FROM a.a_tbl"), 4);
    assert_eq!(count(&b, "SELECT count(*) FROM b_tbl"), 2);
    assert_eq!(integrity(&b), "ok");
}

#[test]
fn r6_writer_reads_its_latest_commit() {
    let (backend, client) = setup(atomic());
    let conn = client.open("r6.db").unwrap();
    conn.execute_batch(
        "CREATE TABLE t1(a, b);
         WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 1000)
         INSERT INTO t1 SELECT x, printf('%0100d', x) FROM s;",
    )
    .unwrap();
    drop(conn);
    let conn = client.open("r6.db").unwrap();
    conn.execute_batch("PRAGMA cache_size=10;").unwrap();
    for _ in 0..2 {
        backend.advance(1000);
        conn.execute_batch(
            "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 3000)
             INSERT INTO t1 SELECT x, printf('%0100d', x) FROM s;",
        )
        .unwrap();
    }
    assert_eq!(count(&conn, "SELECT count(*) FROM t1"), 7000);
    assert_eq!(integrity(&conn), "ok");
}

#[test]
fn r6_exclusive_locking_is_rejected_with_atomic_commits() {
    let (_backend, client) = setup(atomic());
    let conn = client.open("r6x.db").unwrap();
    let err = conn
        .execute_batch("PRAGMA locking_mode=exclusive")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("locking_mode=exclusive is unsupported"),
        "{err}"
    );
}

#[test]
fn r7_temporary_files_stay_local() {
    let (backend, client) = setup(atomic());
    let conn = client.open("r7.db").unwrap();
    conn.execute_batch(
        "PRAGMA cache_size=10; CREATE TABLE t1(a, b);
         WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 50000)
         INSERT INTO t1 SELECT x, printf('%050d', x * 7919 % 50000) FROM s;",
    )
    .unwrap();
    backend.reset_counts();
    conn.execute_batch("CREATE INDEX t1b ON t1(b); VACUUM;")
        .unwrap();
    let counts = backend.counts();
    for name in counts.keys() {
        assert!(
            ["commit", "read.main", "size.main", "heartbeat"].contains(&name.as_str()),
            "unexpected request {name}: {counts:?}"
        );
    }
    assert_eq!(
        count(&conn, "SELECT count(*) FROM t1 INDEXED BY t1b WHERE b > ''"),
        50000
    );
}

#[test]
fn r8_paths_name_distinct_databases() {
    let (_backend, client) = setup(atomic());
    for (path, rows) in [("/tmp/abs/main.db", 1), ("/a/x.db", 2), ("/b/x.db", 3)] {
        let conn = client.open(path).unwrap();
        conn.execute_batch("CREATE TABLE t(x)").unwrap();
        for i in 0..rows {
            conn.execute("INSERT INTO t VALUES (?1)", [i]).unwrap();
        }
    }
    for (path, rows) in [("/tmp/abs/main.db", 1), ("/a/x.db", 2), ("/b/x.db", 3)] {
        let conn = client.open(path).unwrap();
        assert_eq!(count(&conn, "SELECT count(*) FROM t"), rows, "{path}");
    }
}

#[test]
fn r9_cache_settings_are_validated_not_panicked_on() {
    let half = EnvConfig::from_lookup(|name| {
        (name == "GRPSQLITE_CACHE_DIR").then(|| "/tmp/c".to_string())
    });
    let err = half.unwrap_err();
    assert!(err.contains("together"), "{err}");
    // A VFS built from a bad configuration fails each open instead.
    let backend = Arc::new(SimBackend::simple(atomic()));
    let client = Client::with_config(backend, Err(err), None);
    let err = client.open("r9.db").unwrap_err();
    assert_eq!(sqlite_code(&err), Some(ErrorCode::CannotOpen));
}

#[test]
fn r10_cached_pages_follow_commits() {
    let backend = Arc::new(SimBackend::simple(atomic()));
    let cache = SimCacheStores::settings(CacheFaults::default(), Prng::new(1), 64 << 20);
    let client = Client::new(backend.clone(), Some(cache));
    let conn = client.open("r10.db").unwrap();
    conn.execute_batch(
        "PRAGMA cache_size=10; CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
         WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 3000)
         INSERT INTO t1 SELECT x, 'old' || printf('%097d', x) FROM s;",
    )
    .unwrap();
    conn.execute_batch("UPDATE t1 SET b = 'new' || printf('%097d', a) WHERE a % 3 = 0;")
        .unwrap();
    backend.reset_counts();
    assert_eq!(
        count(&conn, "SELECT count(*) FROM t1 WHERE b LIKE 'new%'"),
        1000
    );
    assert_eq!(
        count(&conn, "SELECT count(*) FROM t1 WHERE b LIKE 'old%'"),
        2000
    );
    assert_eq!(
        backend.count("read.main"),
        0,
        "the lease holder reads cached pages locally"
    );
}

/// On a server without atomic commits, a WAL write can reach the server and
/// lose its response. If it was a commit's last write, the server holds the
/// commit while SQLite believes it failed. The writer must stop reading until
/// it reopens and recovers the WAL from the server.
#[test]
fn wal_write_with_a_lost_response_breaks_the_writer() {
    let (backend, client) = setup(non_atomic());
    let wal = "PRAGMA locking_mode=exclusive; PRAGMA journal_mode=wal;";
    let insert = "INSERT INTO t1 VALUES (2)";
    // A dry run counts the WAL writes of one insert.
    let conn = client.open("wal-dry.db").unwrap();
    conn.execute_batch(wal).unwrap();
    conn.execute_batch("CREATE TABLE t1(a); INSERT INTO t1 VALUES (1);")
        .unwrap();
    let before = backend.count("write.wal");
    conn.execute_batch(insert).unwrap();
    let writes = backend.count("write.wal") - before;
    drop(conn);

    let conn = client.open("wal.db").unwrap();
    conn.execute_batch(wal).unwrap();
    conn.execute_batch("CREATE TABLE t1(a); INSERT INTO t1 VALUES (1);")
        .unwrap();
    backend.lose_response_of("write.wal", writes);
    assert!(conn.execute_batch(insert).is_err());
    let read = conn.query_row("SELECT count(*) FROM t1", [], |r| r.get::<_, i64>(0));
    assert!(
        read.is_err(),
        "the writer read {read:?} after a failed WAL write"
    );
    drop(conn);

    let conn = client.open("wal.db").unwrap();
    conn.execute_batch(wal).unwrap();
    assert_eq!(
        count(&conn, "SELECT count(*) FROM t1"),
        2,
        "the commit reached the server"
    );
    assert_eq!(integrity(&conn), "ok");
}

/// Without shared memory, SQLite keeps a normal-locking connection out of
/// WAL instead of switching and then failing every read.
#[test]
fn wal_needs_exclusive_locking() {
    let (_backend, client) = setup(non_atomic());
    let conn = client.open("wal-normal.db").unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode=wal", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
    conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
        .unwrap();
    drop(conn);

    let conn = client.open("wal-normal.db").unwrap();
    conn.execute_batch("PRAGMA locking_mode=exclusive").unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode=wal", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    conn.execute_batch("INSERT INTO t VALUES (2)").unwrap();
    drop(conn);

    let conn = client.open("wal-normal.db").unwrap();
    let err = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
        .unwrap_err();
    assert_eq!(sqlite_code(&err), Some(ErrorCode::CannotOpen));
    drop(conn);
    let conn = client.open("wal-normal.db").unwrap();
    conn.execute_batch("PRAGMA locking_mode=exclusive").unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM t"), 2);
}

/// A replica in the same process as a writer mid-transaction must not take
/// the writer's in-memory journal for a hot one.
#[test]
fn a_replica_ignores_a_live_writer_journal_in_its_process() {
    let (_backend, client) = setup(atomic());
    let writer = client.open("live.db").unwrap();
    writer
        .execute_batch("PRAGMA journal_mode=delete; CREATE TABLE t(x); INSERT INTO t VALUES (1);")
        .unwrap();
    writer
        .execute_batch("BEGIN; INSERT INTO t VALUES (2);")
        .unwrap();
    let replica = client.open_readonly("live.db").unwrap();
    assert_eq!(count(&replica, "SELECT count(*) FROM t"), 1);
    writer.execute_batch("COMMIT").unwrap();
    assert_eq!(count(&replica, "SELECT count(*) FROM t"), 2);
}

/// A connection that lost its lease mid-transaction leaves an in-memory
/// journal behind. After another process takes over and commits, a new
/// connection in the first process must read the server's state, not play
/// back that stale journal over it.
#[test]
fn a_reopened_database_ignores_the_broken_connection_journal() {
    let (backend, client) = setup(atomic());
    let old = client.open("stale.db").unwrap();
    old.execute_batch(
        "PRAGMA journal_mode=delete; PRAGMA cache_size=10;
         CREATE TABLE t(a INTEGER PRIMARY KEY, b);
         WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 500)
         INSERT INTO t SELECT x, 'old' FROM s;",
    )
    .unwrap();
    old.execute_batch("BEGIN; UPDATE t SET b = 'mine';")
        .unwrap();
    backend.stall_heartbeats("stale.db");
    backend.advance(backend.lease_ttl() + 1);

    let other = Client::new(backend.clone(), None);
    let conn = other.open("stale.db").unwrap();
    conn.execute_batch("UPDATE t SET b = 'theirs'").unwrap();
    drop(conn);
    drop(other);

    let new = client.open("stale.db").unwrap();
    new.execute_batch("PRAGMA journal_mode=delete").unwrap();
    assert_eq!(
        count(&new, "SELECT count(*) FROM t WHERE b = 'theirs'"),
        500
    );
    new.execute_batch("UPDATE t SET b = 'next' WHERE a <= 10")
        .unwrap();
    drop(old);
    assert_eq!(count(&new, "SELECT count(*) FROM t WHERE b = 'next'"), 10);
    assert_eq!(integrity(&new), "ok");
}
