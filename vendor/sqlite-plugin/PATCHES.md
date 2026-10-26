# Local patches

This is `sqlite-plugin` 0.11.0 from crates.io with three changes, applied
through `[patch.crates-io]` in the workspace `Cargo.toml`:

- `Vfs::file_control` receives every file-control opcode other than
  `SQLITE_FCNTL_PRAGMA`. gRPSQLite needs `SQLITE_FCNTL_SYNC`, which SQLite sends
  at every commit even with `PRAGMA synchronous=OFF`
  (`file_control.patch`).
- `vfs::unregister_static` unregisters a VFS and frees it. The simulator
  registers a VFS per simulated process, thousands per run
  (`unregister.patch`).
- `Vfs::SHARED_MEMORY` set to false registers no `xShm*` methods. With them
  registered, the defaults let `PRAGMA journal_mode=wal` succeed in normal
  locking mode and then fail every read and write
  (`shared_memory.patch`).

Each patch is one change, ready to send upstream; they apply in the order listed.
Delete this directory and the patch entry once a release includes all three.
