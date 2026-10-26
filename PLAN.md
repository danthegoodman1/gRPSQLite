# Development Plan

## Overarching Goal

Make gRPSQLite deliver the guarantees below with any SQLite build, any page size and any journal mode it documents, and prove them with a deterministic simulator. The work makes the VFS own transaction boundaries instead of relying on SQLite's optional batch-atomic path; moves per-database state out of the global VFS object; brings dependencies, SQLite and the protocol current; and makes the local cache correct before making it fast.

Non-goals: multi-writer databases, WAL with shared memory, cross-database atomic commits, and replacing gRPC.

## Guarantees

These are the contract. A change that alters one updates `README.md` and `proto/grpc_vfs.proto` in the same commit.

- **G1 Atomic commits.** A transaction reaches the server whole or not at all. A client crash or failure at any point leaves the server at the last committed state. Atomicity is per database; an `ATTACH` transaction commits each database separately.
- **G2 Durable commits.** `COMMIT` returns success only after the server acknowledges every page of the transaction. A failed `COMMIT` has an unknown outcome; the next transaction reads whichever state the server holds.
- **G3 Single writer.** At most one client holds a database's write lease. The server rejects writes without the current lease. A client that loses its lease stops writing and stops trusting its caches.
- **G4 Consistent replicas.** On a server with point-in-time reads, every read-only transaction reads exactly one committed version.
- **G5 Real SQLite.** Stock SQLite works, loaded dynamically or linked statically, with page sizes 512–65536, rollback-journal modes, `ATTACH`, and several databases and connections per process.
- **G6 Invisible caches.** Local caching changes latency, never results.

Changes from today's implied contract: G1 no longer depends on `SQLITE_ENABLE_BATCH_ATOMIC_WRITE`; G2's ambiguous-failure rule is new; G6 removes "blind local reads" (`LOCAL_READS`), which traded G4 and G6 for speed.

## Implementation Principles

- The VFS core reaches the network, the disk and the clock only through `Backend`, `CacheStore` and `Clock` traits, and starts no threads. `GrpcBackend` owns the tokio runtime.
- A panic in a VFS callback aborts the host process. VFS code returns SQLite error codes; it never unwraps.
- State lives with what it describes: per-database state in a shared `DbState`, per-open-file state in the handle. The VFS object holds configuration and the backend.
- Use SQLite's own signals (lock levels, `SQLITE_FCNTL_SYNC`, the header's change counter and page size) instead of inferring intent from offsets and lengths.
- Temporary files and, in atomic mode, rollback journals live in client memory. Only files that must survive a client crash go to the server.
- Build the smallest change that meets the phase gate. Each phase updates the README and proto comments to describe current behavior.
- Execution order: Phase 1, 2, 3, 4, 5, 6, 7.

## Testing Strategy

- **Repro suite:** Phase 2 turns every bug below (R1–R11) into a test that fails for the documented reason. A fixed bug keeps its test.
- **VOPR simulator:** follows `../s3-accelerator/AGENTS.md`. One seed drives SQLite (rusqlite, bundled), the VFS and `SimBackend` on one thread through random transactions, reopens, replicas and faults, and checks every answer against its own model. `cargo test` runs a fixed seed range; CI runs the commit hash as a seed; a phase closes with a 100,000-seed sweep and no failures. A failing seed becomes a regression test in `crates/sim/tests`, with the seed and failing commit in the commit message.
- **Planted bugs:** `scripts/mutants` applies one known bug per guarantee to a scratch copy; seeds in the default range must catch each one.
- **End to end:** the harness runs `GrpcBackend` against the example servers over real gRPC, and the shell from `scripts/build-sqlite` runs `examples/*.sql`.
- **Lints:** `cargo clippy --workspace --all-targets -- -D warnings`. `clippy.toml` in the VFS core and sim bans `HashMap`, `HashSet`, `Instant`, `SystemTime` and `thread::spawn`; the `GrpcBackend` module is exempt.
- **Code review:** each phase ends with `/code-review`; findings are fixed or recorded in the phase ledger.

## Phase 1: Toolchain, Dependencies and Build

Goal:
Current dependencies, a local Linux build against SQLite 3.54.1 with no protoc or Docker, and a VFS that never aborts its host.

Scope:
- 1A Replace the vendored `sqlite-plugin` 0.3.0 with crates.io 0.11.0. Upstream has since zero-filled short reads and returned `SQLITE_IOERR_SHORT_READ` (0.10.1; the vendored `x_read` leaves the buffer's tail unwritten and returns `SQLITE_OK`), set `pMethods` to NULL on a failed open (0.10.1), added `check_reserved_lock` (0.6.0; the vendored copy registers a NULL `xCheckReservedLock`, `vfs.rs:303`, which SQLite calls unguarded once a journal exists), made `sector_size` and `device_characteristics` per handle (0.8.0), and stopped bundling rusqlite under `static` (removing a second SQLite from the `.a` and `.so`). Its one local patch, a generic `file_control` hook, goes upstream as a PR and rides a `[patch.crates-io]` git dependency until released. Implement the now-required `lock`, `unlock` and `check_reserved_lock`.
- 1B Bump tonic and tonic-build 0.13 → 0.14.6 (adds `tonic-prost` and `tonic-prost-build`), prost 0.14, tokio 1.54, TinyUFO 0.9, xxhash-rust 0.8.19, parking_lot 0.13, uuid 1.29, log 0.4.34. Compile the proto with `protox` so builds need no system protoc. Generate it once in a `grpsqlite-proto` crate (today's root crate, renamed so 1D can take the `grpsqlite` name); the VFS depends on it instead of compiling the proto a second time.
- 1C `scripts/build-sqlite` downloads the SQLite 3.54.1 amalgamation, checks its SHA3-256, and links `target/sqlite3_with_grpsqlite`. Register the VFS through `-DSQLITE_EXTRA_INIT` instead of the `__attribute__((constructor))` stub, which runs before `sqlite3_initialize` and makes the shell print "attempt to configure SQLite after initialization" three times. The script replaces `static_dev.Dockerfile` and `static_dev.sh`.
- 1D Rename the VFS library to `grpsqlite` so `.load libgrpsqlite.so` finds `sqlite3_grpsqlite_init` by SQLite's default rule. Today `.load libsqlite_vfs.so`, as the README instructs, looks for `sqlite3_extension_init` and then `sqlite3_sqlitevfs_init`. Wire the crate's `static` and `dynamic` features to the plugin's; today both are always on.
- 1E No aborts: deny `clippy::unwrap_used`, `expect_used` and `panic` in the VFS crate. Connect lazily (`Endpoint::connect_lazy`) and fetch capabilities on the first open, returning `SQLITE_CANTOPEN` on failure. Validate configuration at open. Tolerate a host that already installed a `log` logger.
- 1F Logging: read the level from `GRPSQLITE_LOG` (default `warn`) and honor it in `enabled()`. Today the level is hard-coded to `Trace` (`lib.rs:1174`, `1201`), so every write formats its whole page through `{:?}` (`lib.rs:549`).
- 1G Remove scaffolding: `add()` and its test (`src/lib.rs`), `src/main.rs`, the unused `PRELOAD_CACHE*` settings, and root dependencies that only examples use (move them to dev-dependencies).

Completion gate:
On a machine without protoc or Docker, `cargo build --workspace` and clippy pass, `scripts/build-sqlite` produces a 3.54.1 shell that runs `examples/memory_server_test.sql` without warnings, and R9 and R11 return SQLite errors instead of aborting.

Testing plan:
- Build and clippy on a clean checkout without protoc.
- `.load target/release/libgrpsqlite.so` in a stock SQLite 3.54.1 shell.
- R9 and R11 from Phase 2 (run by hand until the harness lands).

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | Keep `sqlite-plugin` as the VFS framework | Review 2026-10-25: 0.11.0 (2026-07-21) offers a central `Vfs` trait, static and dynamic registration and a pragma hook, and graft-sqlite, also a remote page-store VFS, uses it. `rsqlite-vfs` 0.2.0 handles only `SIZE_HINT` in `xFileControl` and has no pragma hook; `sqlite-vfs` was last released 2022-07-08. |
| Incomplete | Work | 1A `sqlite-plugin` 0.11.0 plus upstream `file_control` PR | Missing: dependency swap, lock methods, PR link. |
| Incomplete | Work | 1B Dependency bumps and `protox` | Missing: updated `Cargo.toml` and `Cargo.lock`; build without protoc. |
| Incomplete | Work | 1C `scripts/build-sqlite` with 3.54.1 and `SQLITE_EXTRA_INIT` | Missing: script; shell output free of configure warnings. |
| Incomplete | Work | 1D Library rename and feature wiring | Missing: `.load` transcript from a stock shell. |
| Incomplete | Work | 1E Lazy connect, config validation, no-panic lints | Missing: R11 and R9 return errors. Observed at `e2a2d84`: both abort with exit 134. |
| Incomplete | Work | 1F Configurable log level | Missing: implementation. |
| Incomplete | Work | 1G Remove scaffolding | Missing: implementation. |
| Incomplete | Gate | Clean build without protoc or Docker; shell runs examples | Missing: command transcript. |

## Phase 2: Simulation Seam, Harness and Repro Suite

Goal:
Every bug in this plan has a failing automated test, and the VFS runs deterministically against an in-process backend.

Scope:
- 2A `Backend` trait: the synchronous RPC surface the VFS needs. `GrpcBackend` wraps tonic, and the ten near-identical `block_on`/log/map-error blocks in `lib.rs` collapse into it.
- 2B `SimBackend`: an in-process model server (files of pages, optional versions, a lease table) configurable as atomic, non-atomic or point-in-time, with a fault hook used from Phase 5.
- 2C `crates/sim`: registers the VFS under a test name in rusqlite's bundled SQLite (pin a `libsqlite3-sys` that bundles 3.54.x) with an explicit configuration (no environment variables), seeds SQLite's PRNG with `SQLITE_TESTCTRL_PRNG_SEED`, and uses a PRNG copied from `../s3-accelerator/crates/sim/src/prng.rs`.
- 2D Repro suite R1–R11 (ledger below). Crash cases abandon the client mid-transaction: `SimBackend` partitions it, the harness leaks the connection, and a fresh VFS instance reopens the database. Each failing test carries `#[ignore = "R#: fixed in Phase N"]` until fixed.
- 2E VOPR v0: random multi-statement transactions over two tables with rollbacks, savepoints, `VACUUM`, page sizes 512–65536, cache sizes, reopens and `ATTACH`, checked against a `BTreeMap` model and `PRAGMA integrity_check`. `cargo run --release -p grpsqlite-sim -- [SEED] [--seeds N]`.
- 2F CI: fmt, clippy, tests, and a simulator run seeded by the commit hash.

Completion gate:
R1–R11 run in `cargo test` and fail for their documented reasons on `main`; VOPR v0 finds R4- and R5-class failures, and a failing seed replays identically.

Testing plan:
- Run each repro against `main` and record the failure message.
- Replay one failing VOPR seed twice and diff the traces.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 2A `Backend` trait and `GrpcBackend` | Missing: implementation. |
| Incomplete | Work | 2B `SimBackend` | Missing: implementation. |
| Incomplete | Work | 2C Sim crate, PRNG, explicit config | Missing: implementation. |
| Incomplete | Test | R1 Torn commit, atomic mode: `journal_mode=memory`, `cache_size=10`, `BEGIN; UPDATE` 5,000 rows, crash before `COMMIT` | Missing: test. Observed at `e2a2d84` with `kill -9`: 129 `Write` RPCs reached the server; after restart 4,773 of 5,000 rows held uncommitted values. |
| Incomplete | Test | R2 Cache spill bypasses the batch: 20,000-row `INSERT`, `journal_mode=memory`, `cache_size=10` | Missing: test asserting zero `Write` RPCs in atomic mode. Observed: 551 `Write` and 2 `AtomicWriteBatch` RPCs. |
| Incomplete | Test | R3 Lost hot journal, non-atomic mode: `journal_mode=delete`, crash mid-transaction | Missing: test. Observed: after restart 2,508 rows instead of 5,000; `access()` (`lib.rs:421`) reports only locally open files, so SQLite never rolled the journal back. |
| Incomplete | Test | R4 `PRAGMA page_size=1024` on a new database | Missing: test. Observed: `file is not a database (26)`; the first-page cache assumes page size equals `sector_size` (`lib.rs:558`, `696`). |
| Incomplete | Test | R5 Reopen in one process: write `b.db`, write `a.db`, reopen `b.db` | Missing: test. Observed: `database disk image is malformed (11)`; the first-page cache is VFS-global and survives `close`. |
| Incomplete | Test | R6 Read-write client pinned to an old snapshot: point-in-time server, second session with `locking_mode=exclusive`, 3,000-row insert | Missing: test. Observed: `integrity_check` reports `Rowid ... out of order`; RW reads keep the first read's `time_millis` until `unlock`. |
| Incomplete | Test | R7 Temporary files go to the server: `CREATE INDEX` over 50,000 rows, `cache_size=10` | Missing: test asserting no remote temp I/O. Observed: 2,554 `Write` RPCs with `file=""`; every temp file shares that empty name, and `DELETEONCLOSE` is ignored. |
| Incomplete | Test | R8 Absolute path: `.open /tmp/abs/main.db` | Missing: test. Observed: `disk I/O error`; the example creates the lease's file under the basename `main.db` and reads by full path. |
| Incomplete | Test | R9 `LOCAL_CACHE_DIR` set without `MAX_CACHE_BYTES` | Missing: test. Observed: panic in `x_write` (`lib.rs:629` unwraps `None`), process abort, exit 134. |
| Incomplete | Test | R10 `LOCAL_READS=true` serves stale pages: update 1,000 of 3,000 rows | Missing: test. Observed: query counts 925 updated rows; batched writes never update the cache (`lib.rs:560-577`). |
| Incomplete | Test | R11 No server at startup: `select 1` | Missing: test. Observed: panic in the load-time constructor (`lib.rs:95`), exit 134. |
| Incomplete | Work | 2E VOPR v0 | Missing: driver, model, seed CLI. |
| Incomplete | Work | 2F CI workflow | Missing: workflow file and a passing run. |
| Incomplete | Gate | Repros fail as documented; failing seed replays | Missing: test output and replay diff. |

## Phase 3: Per-Database State and Atomic Transactions

Goal:
G1 and G5 hold across page sizes, journal modes, `ATTACH`, and several databases and connections per process.

Scope:
- 3A Replace the VFS-global lease, database name, write batch, read timestamp and first-page cache (`lib.rs:34-52`) with a `DbState` shared by one database's handles. Several connections to one database in a process share a reference-counted lease, and an in-process lock table enforces SQLite's lock levels among them.
- 3B Local files: temporary, transient and sub-journal handles (`OpenKind::is_temp`, `SubJournal`, or no path) live in client memory and vanish on close. In atomic mode the rollback journal also lives in client memory, because the server never holds a partial transaction; opening a WAL file in atomic mode returns `SQLITE_CANTOPEN`.
- 3C VFS-owned transaction batch for atomic servers: buffer every main-database write and truncate from a transaction's first write; serve reads through the buffer; send it as one `AtomicWriteBatch` at `SQLITE_FCNTL_SYNC` or `xSync`; discard it when the VFS returns an I/O error, on unlock below `RESERVED`, and on close. Stop advertising `SQLITE_IOCAP_BATCH_ATOMIC` and ignore the `*_ATOMIC_WRITE` file controls. SQLite's batch-atomic path exists only in builds with `SQLITE_ENABLE_BATCH_ATOMIC_WRITE`, which SQLite's default build and rusqlite's bundled build omit; cache spills write outside it (R2); and when `COMMIT_ATOMIC_WRITE` fails with an I/O error, SQLite retries with direct page writes that nothing undoes after a crash. Run `locking_mode=exclusive` through the VOPR, and reject it in atomic mode if a buffered write survives an aborted transaction.
- 3D Header cache per `DbState`, patched by the byte ranges SQLite writes and dropped on close, discard and lease loss. Read the page size from the header instead of assuming `sector_size`.
- 3E Read-write handles always read the latest version. Read-only handles pin a version at the first read after `lock(SHARED)` and release it at `unlock(NONE)`.
- 3F Non-atomic crash recovery: `access()` asks the server, treating size > 0 as existing (the unix VFS rule), and `check_reserved_lock` answers from the lock table.
- 3G Simplify: remove the `files` list, `write_batch_open` and the `Arc<Mutex<_>>` wrappers on single-owner fields; take the batch with `mem::take` instead of cloning every page (`lib.rs:1082`).

Completion gate:
R1–R7 pass. VOPR v0 passes 100,000 seeds over page sizes 512–65536, journal modes `memory`, `delete`, `truncate` and `persist` against an atomic server, and `delete` and `wal` with `locking_mode=exclusive` against a non-atomic server.

Testing plan:
- Unit tests for the write overlay, header patching and lock table.
- R1–R7 un-ignored.
- VOPR sweep; record the seed range and duration.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 3A `DbState`, shared lease, in-process lock table | Missing: implementation; R5 passing. |
| Incomplete | Work | 3B Local temp files and atomic-mode journals | Missing: implementation; R7 passing. |
| Incomplete | Work | 3C VFS-owned transaction batch | Missing: implementation; R1 and R2 passing. |
| Incomplete | Decision | `locking_mode=exclusive` in atomic mode | Needs: VOPR result deciding support or rejection. |
| Incomplete | Work | 3D Per-database header cache using the header's page size | Missing: implementation; R4 and R5 passing. |
| Incomplete | Work | 3E Latest reads for read-write, pinned versions for read-only | Missing: implementation; R6 passing. |
| Incomplete | Work | 3F Server-backed `access()` and `check_reserved_lock` | Missing: implementation; R3 passing. |
| Incomplete | Work | 3G Remove redundant state and batch clone | Missing: implementation. |
| Incomplete | Gate | R1–R7 pass; 100,000-seed VOPR sweep clean | Missing: test output and sweep log. |

## Phase 4: Protocol v2

Goal:
A smaller, unambiguous protocol that a server in any language can implement correctly from the proto comments alone.

Scope:
- 4A `Open(client_token, database, readonly)` returns a session, a lease for read-write opens, and the capabilities. It replaces `GetCapabilities`, which the VFS calls once per process with `readonly: false` (`lib.rs:97-101`) although the proto says it runs per file, and `AcquireLease`. One round trip per open.
- 4B Identity: the database is the canonical path the application opened, and each file is a `FileKind` (`MAIN`, `JOURNAL`, `WAL`) instead of a name suffix. Today the database is the path's basename, so `/a/x.db` and `/b/x.db` share one name and, on a server that keys leases by it, one lease. Atomic servers see only `MAIN`.
- 4C Versions: an opaque `bytes version` replaces `int64 time_millis`. Wall-clock milliseconds collide when two commits land in one millisecond, and cannot carry HLC timestamps or commit hashes. `AtomicWriteBatch` returns the new version.
- 4D `AtomicWriteBatch` carries the session and lease once (every `AtomicWrite` repeats both today) and an optional `truncate_to`, so a shrink commits with its pages.
- 4E Forward only pragmas prefixed `grpsqlite_`. Today every pragma, including `journal_mode` and `cache_size`, costs a round trip (`lib.rs:1013`).
- 4F Correct the docs: the checksum is XXH3-64 (the proto says xxhash64); the client-token variable is `GRPC_VFS_CLIENT_TOKEN` in code and `GRPSQLITE_CLIENT_TOKEN` in the proto; servers may return short reads because the VFS zero-fills them.
- 4G Rewrite the example servers for v2 and make them check leases. The versioned example keeps a per-page version map keyed by a `u64` counter instead of copying the whole file through `im` (last released 2022) on every write.

Completion gate:
Proto v2 merged; examples, harness and VOPR pass on it; the README's server guide matches the proto.

Testing plan:
- R8 un-ignored.
- End-to-end harness against each rewritten example.
- A lease-check test: an example server rejects a write carrying a stale lease.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A `Open` RPC | Missing: proto and backend change. |
| Incomplete | Work | 4B Path identity and `FileKind` | Missing: implementation; R8 passing. |
| Incomplete | Work | 4C Opaque versions | Missing: implementation. |
| Incomplete | Work | 4D Leaner `AtomicWriteBatch` with `truncate_to` | Missing: implementation. |
| Incomplete | Work | 4E Prefixed pragma forwarding | Missing: implementation. |
| Incomplete | Doc | 4F Proto and README corrections | Missing: doc diff. |
| Incomplete | Work | 4G Example servers on v2 with lease checks | Missing: implementation; stale-lease test. |
| Incomplete | Gate | Examples, harness and VOPR pass on v2 | Missing: test output. |

## Phase 5: Leases, Runtime and Failure Handling

Goal:
G2 and G3 hold under lease loss, server errors, ambiguous commits and client crashes, and the VOPR injects each of them.

Scope:
- 5A One runtime: `GrpcBackend` owns a multi-thread tokio runtime with one worker, one channel, and the heartbeat as a spawned task. This replaces the current-thread runtime plus a heartbeat thread that builds its own runtime and connection (`lib.rs:174-254`); one channel suffices because its background tasks run on the worker between SQLite calls.
- 5B Per-RPC deadlines, configurable, and keepalive settings separate from the connect timeout; `GRPC_VFS_CONNECT_TIMEOUT_SECS` sets all three today.
- 5C Lease loss: a `NotFound` heartbeat or no successful heartbeat within the lease TTL marks the `DbState` lost. Every later operation on it returns `SQLITE_IOERR` until reopen, and its caches lose authority. Clear the lease on close; today it stays set (`lib.rs:986`).
- 5D Close and delete always release local state and stop the heartbeat, even when the RPC fails (`lib.rs:978-996`).
- 5E VOPR faults: an RPC fails before or after the server applies it, the lease expires and a second writer takes over, the client crashes at any step. Properties: after a crash the server equals the model after some commit `k`, where every acknowledged commit ≤ `k` ≤ the last attempted; no uncommitted write is ever visible; a client writes nothing after losing its lease; every read-only transaction equals one committed model state.
- 5F `scripts/mutants` plants one bug per guarantee: spills bypass the batch, read-write reads stay pinned, `access()` checks only local files, the header cache survives close, a write succeeds after lease loss.

Completion gate:
The VOPR with faults passes 100,000 seeds, and seeds in the default range catch every planted bug.

Testing plan:
- Unit tests for lease-deadline logic against a test `Clock`.
- VOPR sweep with all fault profiles.
- `scripts/mutants` report.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A One runtime and channel | Missing: implementation. |
| Incomplete | Work | 5B RPC deadlines and keepalive settings | Missing: implementation. |
| Incomplete | Work | 5C Lease-loss handling | Missing: implementation; VOPR lease-takeover property. |
| Incomplete | Work | 5D Close and delete error paths | Missing: implementation. |
| Incomplete | Test | 5E VOPR fault profiles and properties | Missing: implementation and sweep log. |
| Incomplete | Test | 5F Planted bugs | Missing: `scripts/mutants` and its report. |
| Incomplete | Gate | 100,000 seeds clean; every mutant caught | Missing: sweep log and mutants report. |

## Phase 6: Local Page Cache

Goal:
G6: the cache speeds up reads and never changes results.

Scope:
- 6A One `PageCache` over a `CacheStore` trait (disk, and a simulated store with faults) replaces three copies of the deletion code (`lib.rs:390-407`, `490-526`, `935-955`) and two of the insertion code (`617-673`, `855-919`). File names hash the database and page, because keys containing `/` name directories that do not exist. Only main-database pages are cached.
- 6B Update the cache when a batch commits; drop entries on discard, rollback and lease loss.
- 6C Treat an empty `Read` response as a cache hit only when the request carried a checksum and the response echoes it; verify the local page's checksum before returning it, as the README promises and `read()` omits (`lib.rs:839-853`); fall back to the server on any cache error. Cache I/O never fails a SQLite operation.
- 6D A read-write client holding its lease serves cached pages without an RPC, since the lease makes its cache authoritative. Remove `LOCAL_READS`: on a replica it mixes page versions within one transaction, which SQLite reports as corruption.
- 6E Enable the cache only when both the directory and the size are set and valid.
- 6F VOPR cache faults (flipped bits, missing files, failed writes) and the property that every run returns identical results with the cache on and off.

Completion gate:
R9 and R10 pass; the VOPR with cache faults passes 100,000 seeds; a benchmark shows a read-write client issuing no `Read` RPCs for cached pages.

Testing plan:
- Unit tests for `PageCache` eviction and invalidation.
- VOPR sweep with cache faults.
- RPC-count benchmark for cached reads.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6A `PageCache` and `CacheStore`; hashed file names | Missing: implementation. |
| Incomplete | Work | 6B Write-through on commit; invalidation | Missing: implementation; R10 passing. |
| Incomplete | Work | 6C Checksum-gated hits and fallback | Missing: implementation. |
| Incomplete | Work | 6D Lease-authoritative reads; remove `LOCAL_READS` | Missing: implementation. |
| Incomplete | Work | 6E Cache configuration validation | Missing: implementation; R9 passing. |
| Incomplete | Test | 6F VOPR cache faults and cache-off equivalence | Missing: implementation and sweep log. |
| Incomplete | Gate | R9, R10 pass; sweep clean; zero-RPC cached reads | Missing: test output, sweep log, benchmark. |

## Phase 7: Remote Read Performance

Goal:
Fewer round trips per query for read-write and read-only workloads, each change justified by a measurement.

Scope:
- 7A Benchmark harness: RPC counts and latency for point lookups, range scans and bulk inserts, with network latency injected through toxiproxy.
- 7B Reuse a read-write client's cache across restarts: one comparison of the header's change counter at open validates every cached page, because every rollback-journal commit increments it.
- 7C Replica reads: compare a bulk validation RPC (pages changed since a version) with a multi-page `Read` that reads ahead during scans, and adopt what the benchmark favors.

Completion gate:
Each adopted change records a benchmark improvement in this ledger, and the VOPR still passes 100,000 seeds.

Testing plan:
- Benchmarks before and after each change.
- VOPR sweep.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 7A Benchmark harness with injected latency | Missing: harness and baseline numbers. |
| Incomplete | Work | 7B Cache reuse across restarts | Missing: implementation and benchmark. |
| Incomplete | Decision | 7C Replica validation versus read-ahead | Needs: benchmark comparison. |
| Incomplete | Gate | Recorded wins; VOPR clean | Missing: benchmark table and sweep log. |
