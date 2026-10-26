# Development Plan

## Overarching Goal

Make gRPSQLite deliver the guarantees below with any SQLite build, any page size and any journal mode it documents, and prove them with a deterministic simulator. The VFS owns transaction boundaries instead of relying on SQLite's optional batch-atomic path; per-database state lives outside the global VFS object; dependencies, SQLite and the protocol are current; and the local cache is correct before it is fast.

Non-goals: multi-writer databases, WAL with shared memory, cross-database atomic commits, and replacing gRPC.

## Guarantees

These are the contract. A change that alters one updates `README.md` and `proto/grpsqlite.proto` in the same commit.

- **G1 Atomic commits.** A transaction reaches the server whole or not at all. A client crash or failure at any point leaves the server at the last committed state. Atomicity is per database; an `ATTACH` transaction commits each database separately.
- **G2 Durable commits.** `COMMIT` returns success only after the server acknowledges every page of the transaction. A failed `COMMIT` has an unknown outcome; the next transaction reads whichever state the server holds.
- **G3 Single writer.** At most one client holds a database's write lease. The server rejects writes without the current lease. A client that loses its lease stops writing and stops trusting its caches.
- **G4 Consistent replicas.** On a server with point-in-time reads, every read-only transaction reads exactly one committed version.
- **G5 Real SQLite.** Stock SQLite works, loaded dynamically or linked statically, with page sizes 512–65536, rollback-journal modes, `ATTACH`, and several databases and connections per process.
- **G6 Invisible caches.** Local caching changes latency, never results.

Relative to `e2a2d84`: G1 no longer depends on `SQLITE_ENABLE_BATCH_ATOMIC_WRITE`, G2's ambiguous-failure rule is new, and G6 drops blind local reads (`LOCAL_READS`), which traded G4 and G6 for speed.

## Implementation Principles

- The VFS core reaches the network, the disk and the clock only through the `Backend` and `CacheStore` traits, and starts no threads. `GrpcBackend` owns the tokio runtime and the lease clock.
- A panic in a VFS callback aborts the host process. VFS code returns SQLite error codes; it never unwraps.
- State lives with what it describes: per-database state in a shared `DbState`, per-open-file state in the handle. The VFS object holds configuration and the backend.
- Use SQLite's own signals (lock levels, `SQLITE_FCNTL_SYNC`, the header's change counter and page size) instead of inferring intent from offsets and lengths.
- Temporary files and, in atomic mode, rollback journals live in client memory. Only files that must survive a client crash go to the server.
- Build the smallest change that meets the phase gate. Each phase updates the README and proto comments to describe current behavior.

## Testing Strategy

- **Repro suite:** every bug below (R1–R11), and every bug the VOPR has found since, is a test in `crates/sim/tests`. A fixed bug keeps its test.
- **VOPR simulator:** follows `../s3-accelerator/AGENTS.md`. One seed drives SQLite (rusqlite, bundled 3.54.1), the VFS and `SimBackend` on one thread through random transactions, reopens, replicas, cross-database transactions and faults, and checks every answer against its own model. Each step runs within a request budget, so a livelock fails its seed. `cargo test` runs a fixed seed range; CI runs 2,000 seeds from the commit hash; a phase closes with a 100,000-seed sweep and no failures. A failing seed becomes a regression in `crates/sim/tests/seeds.rs`.
- **Planted bugs:** `scripts/mutants` applies one known bug at a time to a scratch copy; seeds 0–299 must catch each one.
- **End to end:** `crates/sim/tests/e2e.rs` runs `GrpcBackend` against the memory server over real gRPC, and `scripts/smoke` runs `examples/*.sql` through the shell from `scripts/build-sqlite`.
- **Lints:** `cargo clippy --workspace --all-targets -- -D warnings`. `clippy.toml` bans `HashMap`, `HashSet`, `Instant`, `SystemTime` and `thread::spawn`; the `GrpcBackend` module and the benchmark are exempt.
- **Code review:** `/code-review` on the finished branch; findings are fixed or recorded in the ledger.

## Phase 1: Toolchain, Dependencies and Build

Goal:
Current dependencies, a local Linux build against SQLite 3.54.1 with no protoc or Docker, and a VFS that never aborts its host.

Scope:
- 1A Replace the vendored `sqlite-plugin` 0.3.0 with 0.11.0, which zero-fills short reads and returns `SQLITE_IOERR_SHORT_READ`, sets `pMethods` to NULL on a failed open, adds `check_reserved_lock`, makes `sector_size` and `device_characteristics` per handle, and stops bundling rusqlite under `static`. Its `file_control` hook and an `unregister_static` call ride as local patches in `vendor/sqlite-plugin`. Implement `lock`, `unlock` and `check_reserved_lock`.
- 1B Bump tonic 0.14, prost 0.14, tokio 1.54, xxhash-rust 0.8.19, parking_lot 0.13, log 0.4.34, rusqlite 0.41. Compile the proto with `protox` in a `grpsqlite-proto` crate, so builds need no system protoc.
- 1C `scripts/build-sqlite` downloads the SQLite 3.54.1 amalgamation, checks its SHA3-256, and builds `target/sqlite/sqlite3-grpsqlite`, registering the VFS through `-DSQLITE_EXTRA_INIT`, plus a stock shell and `libgrpsqlite.so`.
- 1D Name the VFS library `grpsqlite` so `.load libgrpsqlite` finds `sqlite3_grpsqlite_init`, and wire the crate's `static` and `dynamic` features to the plugin's.
- 1E No aborts: deny `clippy::unwrap_used`, `expect_used` and `panic` in the VFS crate; connect lazily; validate configuration at open; tolerate a host that already installed a `log` logger.
- 1F Logging: read the level from `GRPSQLITE_LOG` (default `warn`) and send records to `sqlite3_log`.
- 1G Remove scaffolding: the old `sqlite_vfs/` and `src/` trees, the Docker files, and the example servers that Phase 4 replaces.

Completion gate:
On a machine without protoc or Docker, `cargo build --workspace` and clippy pass, `scripts/build-sqlite` produces a 3.54.1 shell that runs `examples/*.sql`, and R9 and R11 return SQLite errors instead of aborting.

Testing plan:
- Build and clippy on a clean checkout without protoc.
- `.load target/release/libgrpsqlite` in a stock SQLite 3.54.1 shell.
- R9 and R11.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | Keep `sqlite-plugin` as the VFS framework | Review 2026-10-25: 0.11.0 offers a central `Vfs` trait, static and dynamic registration and a pragma hook, and graft-sqlite uses it. `rsqlite-vfs` 0.2.0 handles only `SIZE_HINT` in `xFileControl` and has no pragma hook; `sqlite-vfs` was last released 2022-07-08. |
| Complete | Work | 1A `sqlite-plugin` 0.11.0 with local patches | `Cargo.toml` `[patch.crates-io]`; `vendor/sqlite-plugin/PATCHES.md`, `file_control.patch`, `unregister.patch`; `lock`, `unlock`, `check_reserved_lock` in `crates/vfs/src/vfs.rs` over `crates/vfs/src/locks.rs`. |
| Blocked | Work | 1A Upstream the two plugin patches | Blocked by: the user's approval to open a PR on orbitinghail/sqlite-plugin, an outward-facing action. Both patches are ready as diffs against 0.11.0. |
| Complete | Work | 1B Dependency bumps and `protox` | `Cargo.toml` workspace dependencies; `crates/proto/build.rs` compiles with `protox` and `tonic_prost_build`; no protoc on this machine. |
| Complete | Decision | Drop TinyUFO and uuid | TinyUFO hashes with `ahash::RandomState::new`, which breaks seed replay; `PageCache` uses a deterministic LRU (`crates/vfs/src/cache.rs`). Session and lease ids are counters (`crates/memory/src/lib.rs`). |
| Complete | Work | 1C `scripts/build-sqlite` with 3.54.1 and `SQLITE_EXTRA_INIT` | `scripts/build-sqlite` prints `built ... (SQLite 3.54.1)`; the README quick start ran without configure warnings on 2026-10-25. |
| Complete | Work | 1D Library rename and feature wiring | `crates/vfs/Cargo.toml` (`static`, `dynamic`); `target/sqlite/sqlite3 :memory: '.load target/release/libgrpsqlite' '.open main.db' 'SELECT name FROM users'` printed `Alice`, `Bob`. |
| Complete | Work | 1E Lazy connect, config validation, no-panic lints | `crates/vfs/Cargo.toml` lints; `GrpcBackend` connects on first use; `r9_cache_settings_are_validated_not_panicked_on` and `e2e::r11_no_server_fails_the_open` pass. |
| Complete | Work | 1F Configurable log level | `crates/vfs/src/logging.rs`; `GRPSQLITE_LOG` in `crates/vfs/src/config.rs`. |
| Complete | Work | 1G Remove scaffolding | Commits `0984bcd` and `2525158` remove `sqlite_vfs/`, `src/`, the Docker files and the old examples. |
| Complete | Gate | Clean build without protoc or Docker; shell runs examples | `scripts/build-sqlite && scripts/smoke` passes locally and is a CI step (`.github/workflows/ci.yml`). |

## Phase 2: Simulation Seam, Harness and Repro Suite

Goal:
Every bug in this plan has an automated test, and the VFS runs deterministically against an in-process backend.

Scope:
- 2A `Backend` trait: the synchronous RPC surface the VFS needs. `GrpcBackend` wraps tonic.
- 2B `SimBackend`: runs requests against the reference server's `Store` in simulated time, with partitions, faults before and after the server applies a request, heartbeat loss, a request budget, and targeted response loss.
- 2C `crates/sim`: registers the VFS under a test name in rusqlite's bundled SQLite with an explicit configuration, seeds SQLite's PRNG with `SQLITE_TESTCTRL_PRNG_SEED`, and uses the PRNG from `../s3-accelerator/crates/sim/src/prng.rs`.
- 2D Repro suite R1–R11 (ledger below). Crash cases partition the client, drop its connection, wait out the lease and reopen through the same VFS.
- 2E VOPR: random transactions with rollbacks, savepoints, `VACUUM`, page sizes 512–65536, cache sizes, reopens, replicas and cross-database transactions, checked against a `BTreeMap` model and `PRAGMA integrity_check`. `cargo run --release -p grpsqlite-sim --bin vopr -- [SEED] [--seeds N]`.
- 2F CI: fmt, clippy, tests, the shell and examples, and 2,000 VOPR seeds from the commit hash.

Completion gate:
R1–R11 run in `cargo test`; each was observed failing at `e2a2d84`; planted bugs show the suite and the VOPR catch regressions; a seed replays identically.

Testing plan:
- Record each repro's failure at `e2a2d84`.
- Replay seeds twice and compare traces.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A `Backend` trait and `GrpcBackend` | `crates/vfs/src/backend.rs`, `crates/vfs/src/grpc.rs`. |
| Complete | Work | 2B `SimBackend` | `crates/sim/src/backend.rs`. |
| Complete | Work | 2C Sim crate, PRNG, explicit config | `crates/sim/src/prng.rs`, `crates/sim/src/harness.rs`. |
| Complete | Test | R1 Torn commit, atomic mode: `journal_mode=memory`, `cache_size=10`, `BEGIN; UPDATE` 5,000 rows, crash before `COMMIT` | `r1_crash_before_commit_leaves_no_trace`. Observed at `e2a2d84` with `kill -9`: 129 `Write` RPCs reached the server; after restart 4,773 of 5,000 rows held uncommitted values. |
| Complete | Test | R2 Cache spill bypasses the batch: 20,000-row `INSERT`, `journal_mode=memory`, `cache_size=10` | `r2_spilled_pages_commit_atomically` asserts zero `Write` and one `Commit`. Observed: 551 `Write` and 2 `AtomicWriteBatch` RPCs. |
| Complete | Test | R3 Lost hot journal, non-atomic mode: `journal_mode=delete`, crash mid-transaction | `r3_crash_mid_transaction_rolls_back_from_the_journal`. Observed: after restart 2,508 rows instead of 5,000; `access()` reported only locally open files. |
| Complete | Test | R4 `PRAGMA page_size=1024` on a new database | `r4_every_page_size_works` covers 512–65536. Observed: `file is not a database (26)`; the first-page cache assumed page size equals `sector_size`. |
| Complete | Test | R5 Reopen in one process: write `b.db`, write `a.db`, reopen `b.db` | `r5_reopening_another_database_keeps_them_apart`, including `ATTACH`. Observed: `database disk image is malformed (11)`; the first-page cache was VFS-global. |
| Complete | Test | R6 Read-write client pinned to an old snapshot | `r6_writer_reads_its_latest_commit`. Observed: `integrity_check` reported `Rowid ... out of order`. |
| Complete | Test | R7 Temporary files go to the server: `CREATE INDEX` over 50,000 rows, `cache_size=10` | `r7_temporary_files_stay_local`. Observed: 2,554 `Write` RPCs with `file=""`. |
| Complete | Test | R8 Absolute path: `.open /tmp/abs/main.db` | `r8_paths_name_distinct_databases`. Observed: `disk I/O error`; the server keyed the lease by basename. |
| Complete | Test | R9 Cache directory without a size | `r9_cache_settings_are_validated_not_panicked_on`. Observed: panic in `x_write`, exit 134. |
| Complete | Test | R10 `LOCAL_READS=true` serves stale pages | `r10_cached_pages_follow_commits`. Observed: 925 of 1,000 updated rows visible. |
| Complete | Test | R11 No server at startup | `e2e::r11_no_server_fails_the_open`. Observed: panic in the load-time constructor, exit 134. |
| Complete | Work | 2E VOPR | `crates/sim/src/vopr.rs`, `crates/sim/src/bin/vopr.rs`. |
| In Progress | Work | 2F CI workflow | `.github/workflows/ci.yml`. Needs: a passing run on the pull request. |
| Complete | Decision | Gate: detection shown by observations and planted bugs | The harness drives only the new VFS, so the repros cannot run against `e2a2d84`'s. Each was observed failing there (above), and `scripts/mutants` reintroduces the bug classes. |
| Complete | Gate | Repros cover R1–R11; planted bugs caught; seeds replay | `cargo test --workspace` passes; `seeds::a_seed_replays_exactly`; `scripts/mutants` at `60ecd07` caught all 9. |

## Phase 3: Per-Database State and Atomic Transactions

Goal:
G1 and G5 hold across page sizes, journal modes, `ATTACH`, and several databases and connections per process.

Scope:
- 3A A `DbState` shared by one database's handles holds the lease, header and size caches, the page cache and the lock table. Several connections to one database in a process share the lease.
- 3B Temporary, transient and sub-journal handles live in client memory. In atomic mode the rollback journal and super-journal also live in client memory, and WAL is refused. On non-atomic servers, a transaction that needs a super-journal fails with `SQLITE_CANTOPEN`.
- 3C VFS-owned transaction batch for atomic servers: buffer every main-database write and truncate; serve reads through the buffer; send it as one `Commit` at `SQLITE_FCNTL_SYNC` or `xSync`; discard it on an I/O error, on unlock below `RESERVED`, and on close.
- 3D Header cache per `DbState`, patched by committed writes and dropped on close, discard and lease loss. The page size comes from the header.
- 3E Read-write handles read the latest version. Read-only handles pin a version at the first response after `lock(SHARED)` and release it at `unlock(NONE)`.
- 3F Non-atomic crash recovery: `access()` asks the server, and `check_reserved_lock` answers from the lock table.
- 3G The rewrite drops the old VFS's `files` list, `write_batch_open`, `Arc<Mutex<_>>` wrappers and batch clone.

Completion gate:
R1–R7 pass, and the VOPR passes 100,000 seeds over page sizes 512–65536, journal modes `memory`, `delete`, `truncate` and `persist` against an atomic server, and `delete`, `truncate`, `persist` and `wal` against a non-atomic server.

Testing plan:
- Unit tests for the write overlay and lock table.
- VOPR sweep.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A `DbState`, shared lease, in-process lock table | `crates/vfs/src/vfs.rs` (`Db`, `DbState`), `crates/vfs/src/locks.rs`; R5 passes. |
| Complete | Work | 3B Local temp files and atomic-mode journals | `Handle::Local` in `crates/vfs/src/vfs.rs`; R7 passes. |
| Complete | Work | 3C VFS-owned transaction batch | `crates/vfs/src/overlay.rs` with randomized model tests; R1 and R2 pass. |
| Complete | Decision | `locking_mode=exclusive` in atomic mode | Rejected. Seed 38 showed that an exclusive writer stops bumping the change counter, so replicas mixed versions. `r6_exclusive_locking_is_rejected_with_atomic_commits`; mutant `exclusive-locking-allowed` caught at seed 39. |
| Complete | Work | 3D Per-database header cache using the header's page size | `DbState::written` and `truncated`; R4 and R5 pass; mutant `stale-state-after-commit` caught at seed 8. |
| Complete | Work | 3E Latest reads for read-write, pinned versions for read-only | `MainFile::snapshot`; R6 passes; mutant `replica-reads-latest` caught at seed 10. |
| Complete | Work | 3F Server-backed `access()` and `check_reserved_lock` | R3 passes; mutant `journal-exists-locally-only` caught at seed 0. |
| Complete | Work | 3G Remove redundant state and batch clone | `Overlay::into_commit_parts` moves `Bytes` into the request. |
| Complete | Test | Cross-database transactions | `Sim::cross_database` in `crates/sim/src/vopr.rs`: 100,000 seeds ran 451,283, of which 2,924 failed commits reached one database and not the other; each database matched its own model. |
| Complete | Gate | R1–R7 pass; 100,000-seed VOPR sweep clean | `cargo test --workspace`; `vopr 0 --seeds 100000` at `60ecd07`: 11,989,070 steps, 7,688,788 commits, no failures, 2 min 41 s. |

## Phase 4: Protocol v2

Goal:
A smaller, unambiguous protocol that a server in any language can implement correctly from the proto comments alone.

Scope:
- 4A `Open(client_token, database, readonly)` returns a session, a lease for read-write opens, and the capabilities.
- 4B The database is the path the application opened, and each file is a `FileKind` (`MAIN`, `JOURNAL`, `WAL`). Atomic servers see only `MAIN`.
- 4C An opaque `bytes version` replaces `int64 time_millis`; `Commit` returns the new version.
- 4D `Commit` carries the file reference once and an optional `truncate_to`, so a shrink commits with its pages.
- 4E Forward only pragmas prefixed `grpsqlite_`.
- 4F The proto documents XXH3-64 checksums, `GRPSQLITE_CLIENT_TOKEN`, short reads and the message-size limit.
- 4G The in-memory reference server (`crates/memory`) replaces the example servers: lease checks, versions keyed by a `u64` counter, and per-chunk version histories with pruning.

Completion gate:
Proto v2 in place; the memory server, harness and VOPR pass on it; the README's server guide matches the proto.

Testing plan:
- R8.
- End-to-end tests against the memory server.
- A stale-lease test.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A `Open` RPC | `proto/grpsqlite.proto` `Open`; `Core::open_main`. |
| Complete | Work | 4B Path identity and `FileKind` | `FileRef` in the proto; R8 passes. |
| Complete | Work | 4C Opaque versions | `bytes version` on reads, sizes and commits. |
| Complete | Work | 4D `Commit` with `truncate_to` | `CommitRequest` in the proto; the store rejects `Truncate` of `MAIN` on atomic servers. |
| Complete | Work | 4E Prefixed pragma forwarding | `Core::pragma` forwards only `grpsqlite_*`; `e2e::persists_across_connections_and_clients` reads `PRAGMA grpsqlite_server`. |
| Complete | Doc | 4F Proto and README corrections | `proto/grpsqlite.proto` header and RPC comments; README "Writing a server". |
| Complete | Work | 4G Reference server with lease checks | `crates/memory/src/lib.rs`; `stale_lease_cannot_write`, `reads_at_old_versions`, `truncate_then_extend_reads_zeros`. |
| Complete | Gate | Memory server, harness and VOPR pass on v2 | `cargo test --workspace`; `scripts/smoke`; 100,000-seed sweep at `60ecd07`. |

## Phase 5: Leases, Runtime and Failure Handling

Goal:
G2 and G3 hold under lease loss, server errors, ambiguous commits and client crashes, and the VOPR injects each of them.

Scope:
- 5A `GrpcBackend` owns a multi-thread tokio runtime with one worker, one lazy channel, and a heartbeat task per lease.
- 5B Per-request deadlines and keepalive settings, each configurable.
- 5C Lease loss: a rejected heartbeat, or none accepted within the lease TTL, breaks the `DbState`. Every later operation returns `SQLITE_IOERR` until reopen, and its caches lose authority. A WAL write that fails on a server without atomic commits breaks the `DbState` the same way, because SQLite assumes the frames never landed.
- 5D Close and delete release local state and stop the heartbeat even when the request fails.
- 5E VOPR faults: a request fails before or after the server applies it, heartbeats are lost, the lease expires and a second writer takes over, and the client crashes at any request. Properties: after a crash the server holds the state before or after the interrupted transaction, as its outcome allows; no uncommitted write is visible; a client reads and writes nothing after losing its lease; every read-only transaction sees one committed state.
- 5F `scripts/mutants` plants one bug at a time and requires a seed in 0–299 to catch it.

Completion gate:
The VOPR with faults passes 100,000 seeds, and seeds in the default range catch every planted bug.

Testing plan:
- VOPR sweep with all fault profiles.
- `scripts/mutants` report.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 5A One runtime and channel | `crates/vfs/src/grpc.rs`. |
| Complete | Work | 5B RPC deadlines and keepalive settings | `GRPSQLITE_RPC_TIMEOUT_MS`, `GRPSQLITE_KEEPALIVE_*` in `crates/vfs/src/config.rs`. |
| Complete | Work | 5C Lease-loss handling | `mark_broken` in `crates/vfs/src/vfs.rs`; VOPR `takeover` checks reads and writes after lease loss; mutant `lease-loss-ignored` caught at seed 14. |
| Complete | Work | 5C Failed WAL writes break the database | Seed 1,007,275 found a WAL commit whose response was lost: the writer kept reading the old state while the server held the new one. `wal_write_with_a_lost_response_breaks_the_writer` fails without the fix (`read Ok(1)`) and passes with it. |
| Complete | Work | 5D Close and delete error paths | Seed 303 regression in `crates/sim/tests/seeds.rs`. |
| Complete | Test | 5E VOPR fault profiles and properties | `Faults` and `SimBackend::partition_after`, `lose_response_of`; `crash`, `takeover`, `resolve` in `crates/sim/src/vopr.rs`; regressions 67,294 and 1,000,316 (an integrity check meeting a lapsed lease). |
| Complete | Test | 5E Liveness | A step that sends over 200,000 requests fails its seed; the peak across 100,000 seeds was 26,122. Mutant `stale-state-after-commit` had made SQLite loop forever in `btreeBeginTrans`. |
| Complete | Test | 5F Planted bugs | `scripts/mutants` at `60ecd07`: all 9 caught (commit-every-write seed 5, journal-exists-locally-only 0, stale-state-after-commit 8, failed-commit-reported-ok 20, lease-loss-ignored 14, replica-reads-latest 10, exclusive-locking-allowed 39, changed-pages-ignored 18, cache-skips-checksum 25). |
| Complete | Gate | 100,000 seeds clean; every mutant caught | Sweep and mutants above. |

## Phase 6: Local Page Cache

Goal:
G6: the cache speeds up reads and never changes results.

Scope:
- 6A One `PageCache` over a `CacheStore` trait (disk, and a simulated store with damage). The disk store keeps a directory per database, named by a hash of its path, with a file per page and a lock file. Only main-database pages are cached.
- 6B Update the cache from each commit; drop entries on discard, rollback and lease loss.
- 6C Verify each cached page's checksum before returning it; treat a `checksum_matched` response as a hit only after that check; fall back to the server on any cache error.
- 6D A read-write client holding its lease serves cached pages without a request. `LOCAL_READS` is gone.
- 6E Enable the cache only when both the directory and the size are set and valid.
- 6F VOPR cache damage: flipped bits, lost pages and manifests, failed writes.

Completion gate:
R9 and R10 pass; the VOPR with cache damage passes 100,000 seeds; a benchmark shows a read-write client sending no `Read` for cached pages.

Testing plan:
- Unit tests for `PageCache`.
- VOPR sweep with cache damage.
- Request-count benchmark.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 6A `PageCache` and `CacheStore`; hashed names | `crates/vfs/src/cache.rs` (`DiskStores`); `crates/sim/src/cache.rs` (`SimCacheStores`). |
| Complete | Work | 6B Write-through on commit; invalidation | `DbState::written`, `forget`; R10 passes. |
| Complete | Work | 6C Checksum-gated hits and fallback | `PageCache::read_verified`; mutant `cache-skips-checksum` caught at seed 25. |
| Complete | Work | 6D Lease-authoritative reads; `LOCAL_READS` removed | `read_committed` serves the writer's cache without a request. |
| Complete | Work | 6E Cache configuration validation | `EnvConfig::from_lookup`; R9 passes. |
| Complete | Test | 6F VOPR cache damage | `CacheFaults` drawn per seed; every answer must match the model, which is stronger than comparing runs with the cache on and off. |
| Complete | Gate | R9, R10 pass; sweep clean; zero-request cached reads | Benchmark: "writer, page cache" sends 0 requests for 500 lookups, 50 range scans and a full scan. |

## Phase 7: Remote Read Performance

Goal:
Fewer round trips per query for read-write and read-only workloads, each change justified by a measurement.

Scope:
- 7A Benchmark: request counts from the simulated server, and wall time over gRPC with latency injected by the memory server.
- 7B Reuse caches across restarts. A writer's manifest records the header's change counter, which every commit through an atomic server increments; a replica's records its version.
- 7C Replica reads: compare bulk validation (pages changed since a version) with read-ahead during scans, and adopt what the benchmark favors.

Completion gate:
Each adopted change records a benchmark improvement in this ledger, and the VOPR still passes 100,000 seeds.

Testing plan:
- Benchmarks before and after each change.
- VOPR sweep.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 7A Benchmark harness with injected latency | `crates/sim/src/bin/bench.rs`; `Service::with_latency` in `crates/memory/src/lib.rs`. |
| Complete | Decision | Latency from the server instead of toxiproxy | One process, no external tool, and the request counts it pairs with are exact. |
| Complete | Work | Disable Nagle on the memory server | `serve` sets `TCP_NODELAY`: a `SELECT` went from 21.7 s to 0.022 s at about 36 ms of delayed acknowledgment per request. |
| Complete | Work | Raise the message limit | A `VACUUM` over gRPC failed at 4 MiB; `Service::into_server` raises the decoding limit, and the proto documents it. |
| Complete | Work | 7B Cache reuse across restarts | `Manifest` in `crates/vfs/src/cache.rs`; a reopened writer's full scan takes 3 requests instead of 36. |
| Complete | Work | 7C Read-ahead on sequential scans | `MainFile::fetch_len` doubles to 1 MiB. Full scan 1,056 → 36 requests; 50 range scans 2,662 → 398. |
| Complete | Work | 7C File size in read responses | `ReadResponse.size`: replica 500 lookups 1,478 → 978 requests. |
| Complete | Work | 7C Bulk validation with `ChangedPages` | `PageCache::advance_to`, `Core::pin`. Cached replica: 500 lookups 978 → 500, range scans 448 → 50, full scan 37 → 1, lookups under 50 commits 1,245 → 619. Mutant `changed-pages-ignored` caught at seed 18; `e2e::a_cached_replica_follows_commits`. |
| Complete | Decision | 7C Adopt both | Read-ahead serves uncached scans; bulk validation serves cached replicas. Without `ChangedPages`, a cached replica sends as many requests as an uncached one. |
| Complete | Gate | Recorded wins; VOPR clean | Rows above; 100,000-seed sweep at `60ecd07`. |

## Review

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| In Progress | Test | `/code-review` of the branch | Missing: findings and their resolution. |
| Complete | Doc | README in the style of `../s3-accelerator` and `../wirenet` | `README.md`; the quick start ran as written on 2026-10-25. |
