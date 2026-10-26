//! The VFS over real gRPC against the in-memory server.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use grpsqlite::{GrpcBackend, GrpcConfig};
use grpsqlite_memory::{Options, Service, Store};
use grpsqlite_sim::harness::{Client, sqlite_code};
use parking_lot::Mutex;
use rusqlite::ErrorCode;

struct Server {
    addr: SocketAddr,
    runtime: tokio::runtime::Runtime,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    fn start(opts: Options) -> Server {
        Server::start_at("127.0.0.1:0".parse().unwrap(), opts)
    }

    fn start_at(addr: SocketAddr, opts: Options) -> Server {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind(addr))
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let service = Service::new(Arc::new(Mutex::new(Store::new(opts))));
        let task = runtime.spawn(async move {
            let _ = grpsqlite_memory::serve(listener, service).await;
        });
        Server {
            addr,
            runtime,
            task,
        }
    }

    fn client(&self) -> Client {
        let config = GrpcConfig {
            url: format!("http://{}", self.addr),
            connect_timeout: Duration::from_secs(2),
            rpc_timeout: Duration::from_secs(5),
            ..GrpcConfig::default()
        };
        Client::new(Arc::new(GrpcBackend::new(&config).unwrap()), None)
    }

    fn stop(self) -> SocketAddr {
        self.task.abort();
        self.runtime.shutdown_timeout(Duration::from_secs(1));
        self.addr
    }
}

fn count(conn: &rusqlite::Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).expect(sql)
}

#[test]
fn persists_across_connections_and_clients() {
    let server = Server::start(Options::default());
    let client = server.client();
    let conn = client.open("e2e.db").unwrap();
    conn.execute_batch(
        "CREATE TABLE users(id INTEGER PRIMARY KEY, name);
         INSERT INTO users(name) VALUES ('Alice'), ('Bob');",
    )
    .unwrap();
    drop(conn);
    let conn = server.client().open("e2e.db").unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM users"), 2);
    let name: String = conn
        .query_row("PRAGMA grpsqlite_server", [], |r| r.get(0))
        .unwrap();
    assert_eq!(name, "memory");
}

#[test]
fn replica_reads_committed_state_while_the_writer_works() {
    let server = Server::start(Options::default());
    let writer = server.client().open("rep.db").unwrap();
    writer
        .execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1), (2), (3);")
        .unwrap();
    let replica = server.client().open_readonly("rep.db").unwrap();
    assert_eq!(count(&replica, "SELECT count(*) FROM t"), 3);
    writer
        .execute_batch("BEGIN; INSERT INTO t VALUES (4);")
        .unwrap();
    assert_eq!(
        count(&replica, "SELECT count(*) FROM t"),
        3,
        "uncommitted rows stay invisible"
    );
    writer.execute_batch("COMMIT").unwrap();
    assert_eq!(count(&replica, "SELECT count(*) FROM t"), 4);
    let err = replica
        .execute_batch("INSERT INTO t VALUES (5)")
        .unwrap_err();
    assert_eq!(sqlite_code(&err), Some(ErrorCode::ReadOnly));
}

#[test]
fn heartbeats_keep_the_lease_past_its_ttl() {
    let server = Server::start(Options {
        heartbeat_interval_ms: 50,
        lease_ttl_ms: 300,
        ..Options::default()
    });
    let client = server.client();
    let conn = client.open("hb.db").unwrap();
    conn.execute_batch("CREATE TABLE t(x)").unwrap();
    std::thread::sleep(Duration::from_millis(1000));
    conn.execute_batch("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM t"), 1);
}

#[test]
fn a_restarted_server_ends_the_old_lease() {
    let server = Server::start(Options {
        heartbeat_interval_ms: 50,
        lease_ttl_ms: 300,
        ..Options::default()
    });
    let client = server.client();
    let conn = client.open("restart.db").unwrap();
    conn.execute_batch("CREATE TABLE t(x)").unwrap();
    let addr = server.stop();
    let _server = Server::start_at(addr, Options::default());
    // The new server knows no lease, so the old session can no longer write.
    std::thread::sleep(Duration::from_millis(500));
    assert!(conn.execute_batch("INSERT INTO t VALUES (1)").is_err());
    drop(conn);
    // Reopening starts a new session with a new lease.
    let conn = client.open("restart.db").unwrap();
    conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
        .unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM t"), 1);
}

#[test]
fn r11_no_server_fails_the_open() {
    let config = GrpcConfig {
        url: "http://127.0.0.1:1".to_string(),
        connect_timeout: Duration::from_millis(500),
        ..GrpcConfig::default()
    };
    let client = Client::new(Arc::new(GrpcBackend::new(&config).unwrap()), None);
    let err = client.open("nowhere.db").unwrap_err();
    assert_eq!(sqlite_code(&err), Some(ErrorCode::CannotOpen));
}
