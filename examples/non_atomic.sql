-- Run against `memory-server --no-atomic --no-pit`.
.bail on
.open journal.db
PRAGMA journal_mode=delete;
CREATE TABLE IF NOT EXISTS t1(a, b);
WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x < 2000)
INSERT INTO t1 SELECT x, printf('%0100d', x) FROM s;
SELECT count(*) FROM t1;
PRAGMA integrity_check;

-- WAL needs exclusive locking, set before the first read.
.open wal.db
PRAGMA locking_mode=exclusive;
PRAGMA journal_mode=wal;
CREATE TABLE IF NOT EXISTS t1(a, b);
INSERT INTO t1 VALUES (1, 2), (3, 4);
SELECT count(*) FROM t1;
PRAGMA integrity_check;
