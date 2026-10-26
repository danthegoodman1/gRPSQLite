# Local patches

This is `sqlite-plugin` 0.11.0 from crates.io with one change, applied through
`[patch.crates-io]` in the workspace `Cargo.toml`:

- `Vfs::file_control` receives every file-control opcode other than
  `SQLITE_FCNTL_PRAGMA`. gRPSQLite needs `SQLITE_FCNTL_SYNC`, which SQLite sends
  at every commit even with `PRAGMA synchronous=OFF`.

`file_control.patch` is the diff against the published crate, ready to send
upstream. Delete this directory and the patch entry once a release includes it.
