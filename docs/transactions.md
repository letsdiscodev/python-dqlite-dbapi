# Transactions

## Autocommit by default

`dqlite-dbapi` does **not** issue an implicit `BEGIN` before your DML. Each
statement runs in the dqlite engine's autocommit mode unless you have
explicitly opened a transaction.

This differs from stdlib `sqlite3` and `psycopg`, which auto-`BEGIN` on the
first DML. It matches the dqlite C and Go reference clients, which have the
same opt-in contract. If you are porting code that relied on implicit
transactions, add explicit `BEGIN` calls to recover atomic multi-statement
semantics.

Note that **isolation is always SERIALIZABLE** regardless — every write
goes through Raft consensus and every read is serializable. Only the
*grouping* of statements into one transaction is opt-in.

## Grouping statements into a transaction

Issue an explicit `BEGIN` through a cursor, then `commit()` / `rollback()`
on the connection:

```python
cur = conn.cursor()
cur.execute("BEGIN")
cur.execute("INSERT INTO t VALUES (?)", (1,))
cur.execute("INSERT INTO t VALUES (?)", (2,))
conn.commit()       # or conn.rollback()
```

Under the default session mode (`immediate`) a bare `BEGIN` / `BEGIN
TRANSACTION` is rewritten to `BEGIN IMMEDIATE`, so the deferred-to-write
upgrade can't fail later with `SQLITE_BUSY_SNAPSHOT`; an explicit `DEFERRED`
/ `IMMEDIATE` / `EXCLUSIVE` qualifier is passed through unchanged. dqlite's
Raft FSM serializes the transaction across the cluster regardless of the
qualifier, so the qualifier changes only lock-acquisition timing on the
leader, never isolation.

The session mode is selected with the `session_mode` connect argument (or the
`DQLITE_SESSION_MODE` environment variable): `immediate` (default; rewrites a
bare `BEGIN`), `deferred` / `exclusive` (no rewrite), or `read_only` (issues
`PRAGMA query_only = 1`).

The connection also works as a context manager (matching stdlib `sqlite3`):
the `with conn:` block commits on clean exit and rolls back on exception.
It does **not** close the connection — the connection stays reusable.

## `commit()` / `rollback()` details

- Calling `commit()` / `rollback()` before any query has run is a silent
  no-op (the driver won't open a TCP connection just to send a COMMIT).
- Calling them with no active transaction (e.g. right after DDL) is also a
  silent success, matching stdlib `sqlite3`.
- **Commit failures propagate.** Silent swallowing would risk hiding data
  loss, so a failing commit raises.

## `isolation_level`

The `Connection.isolation_level` attribute exists for pre-3.12 stdlib
parity. The setter accepts `None` and the legacy qualifier values (`""`,
`"DEFERRED"`, `"IMMEDIATE"`, `"EXCLUSIVE"`, case-insensitive) as no-ops; the
getter returns the last value set (default `None`). Any other value —
including `"SERIALIZABLE"` and `"AUTOCOMMIT"` — raises `ProgrammingError`,
because the qualifier cannot change dqlite's single serialized isolation
level.

## Leader flips during COMMIT

If the leader loses leadership *after* the COMMIT entry was submitted, the
write is in doubt — Raft may already have replicated it — and the driver
raises `AmbiguousCommitError` (an `OperationalError` subclass, so
`except OperationalError` still catches it). Use idempotent DML (`INSERT OR
REPLACE`, `UPDATE` on a unique key) or an out-of-band state check before
retrying. A plain not-leader rejection *before* the entry was submitted is a
clean failure (the write definitely did not apply) and raises a plain
`OperationalError`.

## With SQLAlchemy

If you use [sqlalchemy-dqlite](https://github.com/letsdiscodev/sqlalchemy-dqlite),
the dialect emits `BEGIN` for every `engine.begin()` / `connection.begin()`
block — you don't manage `BEGIN` yourself. The autocommit-by-default
behavior above applies only to direct dbapi users.
