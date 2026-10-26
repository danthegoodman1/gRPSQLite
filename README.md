# gRPSQLite <!-- omit in toc -->

**A SQLite VFS that stores databases on a gRPC server: turn any datastore into a SQLite backend.**

gRPSQLite lets you build multitenant, distributed SQLite databases on whatever storage you run. Give every user, human or AI, their own SQLite database.

- **Real SQLite**: works with existing SQLite tools, extensions and applications, loaded at runtime or linked in.
- **Any storage**: implement a small gRPC service over a database, an object store or a version-control system.
- **Atomic commits**: each SQLite transaction reaches the server as one `Commit`, however large.
- **Read replicas**: any number of read-only clients, each reading one committed version, when the server supports point-in-time reads.
- **Any language**: the server is a plain gRPC service ([`proto/grpsqlite.proto`](proto/grpsqlite.proto)).

> [!WARNING]
> This is early software. The guarantees below are checked by a deterministic simulator, but gRPSQLite has not run in production.

## Table of Contents <!-- omit in toc -->

- [Quick start](#quick-start)
- [Guarantees](#guarantees)
- [How it works](#how-it-works)
- [Choosing server capabilities](#choosing-server-capabilities)
  - [Atomic commits](#atomic-commits)
  - [Read-only replicas](#read-only-replicas)
  - [Servers without atomic commits](#servers-without-atomic-commits)
- [Using the VFS](#using-the-vfs)
  - [Linked into SQLite](#linked-into-sqlite)
  - [Loaded at runtime](#loaded-at-runtime)
  - [From Rust](#from-rust)
  - [Configuration](#configuration)
- [Local page cache](#local-page-cache)
- [Writing a server](#writing-a-server)
- [Testing](#testing)
- [Contributing](#contributing)

## Quick start

On Linux with a Rust toolchain and a C compiler:

```bash
# Terminal 1: the in-memory reference server
cargo run --release -p grpsqlite-memory

# Terminal 2: build a SQLite 3.54.1 shell with gRPSQLite as its default VFS
scripts/build-sqlite
target/sqlite/sqlite3-grpsqlite
```

In the shell:

```sql
.open main.db
CREATE TABLE users(id, name);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
```

Quit and reopen `main.db`: the rows are still there, on the server. `examples/` holds longer scripts; `scripts/smoke` runs them all.

## Guarantees

- **Atomic commits.** A transaction reaches the server whole or not at all. A client crash or failure at any point leaves the server at the last committed state. Atomicity is per database; an `ATTACH` transaction commits each database separately.
- **Durable commits.** `COMMIT` returns success only after the server acknowledges every page of the transaction. A failed `COMMIT` has an unknown outcome; the next transaction reads whichever state the server holds.
- **Single writer.** At most one client holds a database's write lease. The server rejects writes without the current lease. A client that loses its lease stops writing and stops trusting its caches.
- **Consistent replicas.** On a server with point-in-time reads, every read-only transaction reads exactly one committed version.
- **Real SQLite.** Stock SQLite works, loaded at runtime or linked in, with page sizes 512–65536, rollback-journal modes, `ATTACH`, and several databases and connections per process.
- **Invisible caches.** Local caching changes latency, never results.

## How it works

```
┌──────────────────┐   gRPC    ┌──────────────────┐            ┌─────────────┐
│ SQLite           │ ────────► │ your server      │ ─────────► │ FoundationDB│
│ + gRPSQLite VFS  │ ◄──────── │ (any language)   │ ◄───────── │ Postgres, S3│
└──────────────────┘           └──────────────────┘            └─────────────┘
```

SQLite reads and writes its files through a VFS. The gRPSQLite VFS turns those calls into protocol requests:

- **Open** starts a session per database. A read-write open acquires the database's lease, which the VFS heartbeats in the background.
- **Read** and **GetFileSize** fetch bytes. The lease holder always reads the latest version; a replica pins the version its first read returned until its read transaction ends.
- **Commit** carries every page a transaction wrote. The VFS buffers writes in memory until SQLite syncs the database at commit, so cache spills and SQLite's own retries never reach the server half-done.
- **Write**, **Truncate** and **Delete** serve servers without atomic commits, where SQLite's rollback journal or WAL lives on the server.

Temporary files, statement journals and, on servers with atomic commits, rollback journals stay in client memory. Only the files that must survive a crash travel to the server.

## Choosing server capabilities

A server declares what it supports in its `Open` response.

### Atomic commits

Set `atomic_commit` if your store can apply many writes in one transaction (FoundationDB, Postgres, DynamoDB, most databases). This is the recommended mode:

- every transaction is one round trip at commit;
- the server stores only the main database file;
- any rollback-journal mode works (`memory`, `delete`, `truncate`, `persist`); the journal stays in client memory, so `memory` is the cheapest.

Two settings are refused with servers that have atomic commits:

- `PRAGMA journal_mode=wal`: WAL needs a server without atomic commits.
- `PRAGMA locking_mode=exclusive`: in exclusive mode SQLite stops updating the header's change counter after the first commit, and replicas rely on that counter to notice commits. The lease already makes the client the only writer, so exclusive mode gains nothing.

### Read-only replicas

Set `point_in_time_reads` (with `atomic_commit`) if your store can read as of a version: FoundationDB, CockroachDB (`AS OF SYSTEM TIME`), RocksDB with user-defined timestamps, BadgerDB, or an object store behind a versioned index. Each response carries an opaque `version`; the replica sends it back to read the same version for the rest of its read transaction.

Open a database read-only to get a replica:

```sql
.open --readonly main.db
```

Stores that cannot read a past version (Postgres, MySQL, Redis, plain filesystems) cannot serve replicas on their own.

### Servers without atomic commits

Without `atomic_commit`, SQLite's own crash recovery protects transactions: the rollback journal or WAL lives on the server, and after a crash the next writer replays the journal. Use `journal_mode=delete`, `truncate` or `persist`, or WAL with exclusive locking, set before the first read:

```sql
PRAGMA locking_mode=exclusive;
PRAGMA journal_mode=wal;
```

Each `Write` must apply atomically. A transaction that writes several attached databases needs a server with atomic commits.

## Using the VFS

The VFS is the `grpsqlite` crate (`crates/vfs`).

### Linked into SQLite

```bash
cargo build --release -p grpsqlite --features static
```

builds `target/release/libgrpsqlite.a`. Compile SQLite with `-DSQLITE_EXTRA_INIT=grpsqlite_extra_init` and link the library; SQLite then registers gRPSQLite as its default VFS during `sqlite3_initialize`. [`scripts/build-sqlite`](scripts/build-sqlite) does this for the SQLite shell.

### Loaded at runtime

```bash
cargo build --release -p grpsqlite --features dynamic
```

builds `target/release/libgrpsqlite.so`, which any SQLite can load:

```sql
.load target/release/libgrpsqlite
```

Loading registers gRPSQLite as the default VFS.

### From Rust

With the `static` feature, `grpsqlite::register_static` registers a `Core` under any name, next to SQLite's own VFSes. `Core` takes any `Backend`; `GrpcBackend` is the gRPC client. `crates/sim/src/harness.rs` shows the pattern with rusqlite.

### Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `GRPSQLITE_URL` | `http://localhost:50051` | The server. |
| `GRPSQLITE_CLIENT_TOKEN` | empty | Sent with every `Open`. |
| `GRPSQLITE_CONNECT_TIMEOUT_MS` | `10000` | Connection timeout. |
| `GRPSQLITE_RPC_TIMEOUT_MS` | `30000` | Deadline for each request. |
| `GRPSQLITE_KEEPALIVE_INTERVAL_MS` | `10000` | HTTP/2 keepalive interval. |
| `GRPSQLITE_KEEPALIVE_TIMEOUT_MS` | `20000` | HTTP/2 keepalive timeout. |
| `GRPSQLITE_CACHE_DIR` | unset | Local page cache directory; set with `GRPSQLITE_CACHE_MAX_BYTES`. |
| `GRPSQLITE_CACHE_MAX_BYTES` | unset | Most bytes each open database may cache. |
| `GRPSQLITE_LOG` | `warn` | `off`, `error`, `warn`, `info`, `debug` or `trace`, sent to `sqlite3_log`. |

An invalid setting makes every open fail with `SQLITE_CANTOPEN` and logs the reason; the host process keeps running.

## Local page cache

Set `GRPSQLITE_CACHE_DIR` and `GRPSQLITE_CACHE_MAX_BYTES` to cache pages on local disk, one file per page, evicting the least recently used.

- **Writer:** while it holds the lease, nobody else can change the database, so cached pages are served without a request. Commits update the cache.
- **Replica:** each read sends the cached copy's XXH3-64 checksum; the server answers `checksum_matched` instead of sending bytes it already has.

Every cached page is checked against its checksum before use, so a damaged cache costs a request, never a wrong answer. The cache starts empty each time a database opens.

## Writing a server

You are writing a remote block device with leases. [`proto/grpsqlite.proto`](proto/grpsqlite.proto) is the contract; [`crates/memory`](crates/memory/src/lib.rs) is the reference implementation, and its `Store` is what the simulator tests.

- **Keys.** SQLite writes the main file in whole pages at page-aligned offsets, so key pages by `offset / page_size`. Reads can be shorter or start mid-page (the 100-byte header, the change counter at offset 24); serve them from the page that contains them.
- **Missing data.** Return the bytes that exist; the client zero-fills the rest.
- **Leases.** Check the lease inside the same transaction that applies a write or commit. Return `FAILED_PRECONDITION` for a missing, expired or replaced lease.
- **Versions.** With point-in-time reads, return a version with every read and read at the version a request carries. Versions are opaque bytes: a timestamp, a commit hash, a log offset.
- **Message size.** A `Commit` holds a whole transaction, and a `VACUUM` rewrites the whole database. Raise your gRPC server's message limit above the default 4 MiB.
- **Latency.** Each request is a small round trip; disable Nagle's algorithm on accepted connections.

## Testing

```bash
cargo test --workspace                               # unit, repro, end-to-end and seed tests
cargo run --release -p grpsqlite-sim --bin vopr -- --seeds 10000
cargo run --release -p grpsqlite-sim --bin vopr -- 42 --verbose   # replay one seed
scripts/mutants                                      # planted bugs the seeds must catch
scripts/build-sqlite && scripts/smoke                # the SQLite shell against the memory server
```

The VOPR (`crates/sim/src/vopr.rs`) runs SQLite, the VFS and the reference server's `Store` on one thread in simulated time, so a seed replays exactly. Each seed draws a server (atomic or not, with or without point-in-time reads), a page size, journal mode, cache, and fault rates, then runs random transactions, savepoints, rollbacks, `VACUUM`, reopens, replicas, crashes at any request, and lease takeovers. It checks every answer against a model of the committed rows. `crates/sim/tests/repro.rs` replays the bugs this design fixed.

## Contributing

Do not submit pull requests out of nowhere; they will be closed.

If you find a bug or want to request a feature, open a discussion. Contributors will turn it into an issue if needed.
