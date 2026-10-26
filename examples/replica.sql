-- Run against `memory-server` after examples/atomic.sql. A read-only open is
-- a replica: each read transaction sees one committed version.
.bail on
.open --readonly main.db
SELECT count(*) FROM t1;
PRAGMA integrity_check;
