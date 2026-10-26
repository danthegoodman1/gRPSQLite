-- Run against `memory-server` (atomic commits, point-in-time reads).
.bail on
.open main.db
PRAGMA journal_mode=memory;
CREATE TABLE IF NOT EXISTS t1(a INTEGER PRIMARY KEY, b);
DELETE FROM t1;
-- A transaction far larger than the page cache still commits atomically.
PRAGMA cache_size=10;
WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 20000)
INSERT INTO t1 SELECT x, printf('%0100d', x) FROM s;
SELECT count(*) FROM t1;
-- Temporary files stay local.
CREATE INDEX IF NOT EXISTS t1b ON t1(b);
VACUUM;
-- Any page size works.
PRAGMA page_size=1024;
VACUUM;
PRAGMA page_size;
PRAGMA integrity_check;
PRAGMA grpsqlite_server;
