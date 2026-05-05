# dqlite-dbapi

PEP 249 compliant interface for [dqlite](https://dqlite.io/).

## Installation

```bash
pip install dqlite-dbapi
```

## Sync Usage

```python
import dqlitedbapi

conn = dqlitedbapi.connect("localhost:9001")
cursor = conn.cursor()
cursor.execute("SELECT 1")
print(cursor.fetchone())
conn.close()
```

## Async Usage

```python
import asyncio
from dqlitedbapi.aio import aconnect

async def main():
    conn = await aconnect("localhost:9001")
    cursor = conn.cursor()
    await cursor.execute("SELECT 1")
    print(await cursor.fetchone())
    await conn.close()

asyncio.run(main())
```

## PEP 249 Compliance

- `apilevel = "2.0"`
- `threadsafety = 1`
- `paramstyle = "qmark"`

## Transactions

`dqlite-dbapi` does **not** issue implicit `BEGIN` before DML — each
statement runs in the underlying SQLite engine's autocommit mode unless
the caller has explicitly opened a transaction. This deviates from
PEP 249 §6's prescribed implicit-transaction model and from stdlib
`sqlite3` / `psycopg`, both of which auto-BEGIN on the first DML; users
porting from those drivers will see different behaviour and must add
explicit `BEGIN` calls (see below) to recover atomic multi-statement
semantics. The dqlite C and Go reference clients have the same opt-in
contract; this driver matches them rather than stdlib.

If you use SQLAlchemy via `sqlalchemy-dqlite`, the dialect emits
`BEGIN` for every `engine.begin()` / `connection.begin()` block — no
explicit `BEGIN` needed; the autocommit-by-default gap below applies
only to direct dbapi users.

Every write still goes through Raft consensus and every read is
serializable; **isolation is always SERIALIZABLE**, but transaction
*grouping* is opt-in.

To group statements into a transaction, issue an explicit `BEGIN`
through a cursor (or use `dqliteclient`'s `transaction()` async
context manager from the layer below):

```python
cur = conn.cursor()
cur.execute("BEGIN")
cur.execute("INSERT INTO t VALUES (?)", (1,))
cur.execute("INSERT INTO t VALUES (?)", (2,))
conn.commit()       # COMMIT
```

The bare `BEGIN` SQL is the SQLite default (`BEGIN DEFERRED`). dqlite's
Raft FSM serializes the transaction across the cluster regardless of
the `DEFERRED` / `IMMEDIATE` / `EXCLUSIVE` qualifier, so the qualifier
does not change isolation semantics — only the lock-acquisition timing
on the leader. There is no `isolation_level` attribute (cannot be
weakened on dqlite); the SQLAlchemy dialect rejects `AUTOCOMMIT` on
the same grounds.

Connection-level `commit()` / `rollback()` semantics:

- Calling `commit()` / `rollback()` before any query has run is a silent
  no-op (preserves the "no spurious connect" contract — we don't open
  a TCP connection just to send COMMIT).
- Calling `commit()` / `rollback()` with no active transaction
  (e.g. right after a DDL statement) is also silently successful, matching
  stdlib `sqlite3` semantics.
- The context-manager exit (`with conn: ...`) commits on clean exit and
  attempts rollback on exception. **Commit failures propagate** —
  silent swallowing was a footgun that could hide data loss.
- Operational caveat: a leader-flip during COMMIT raises
  `OperationalError` with a code in `dqlitewire.LEADER_ERROR_CODES`. The
  write may or may not have been persisted (Raft may already have
  replicated the commit log entry before the flip). Use idempotent DML
  (`INSERT OR REPLACE`, `UPDATE` on a unique key) or an out-of-band
  state-check before retrying.

## Differences from `aiosqlite`

This driver is PEP 249-shaped (sync) and exposes an async surface
under `dqlitedbapi.aio` that mirrors the sync `dqlitedbapi` surface
plus async-specific `aconnect` / `AsyncConnection` / `AsyncCursor`.

The full PEP 249 module attributes (`apilevel`, `threadsafety`,
`paramstyle`, `sqlite_version`, `sqlite_version_info`), exception
hierarchy (`Warning`, `Error`, `InterfaceError`, `DatabaseError`,
`DataError`, `OperationalError`, `IntegrityError`, `InternalError`,
`ProgrammingError`, `NotSupportedError`), type constructors
(`Date`, `Time`, `Timestamp`, `DateFromTicks`, `TimeFromTicks`,
`TimestampFromTicks`, `Binary`), type sentinels (`STRING`, `BINARY`,
`NUMBER`, `DATETIME`, `ROWID`), and stdlib-sqlite3-parity stubs
(`register_adapter`, `register_converter`, `complete_statement`,
`enable_callback_tracebacks`) are all re-exported under
`dqlitedbapi.aio` so cross-driver code porting from aiosqlite
imports them from one namespace. One notable deviation:

- **`AsyncConnection.__aexit__` does NOT close the connection.** It
  commits / rolls back per the body outcome and leaves the
  connection reusable, matching stdlib `sqlite3.Connection.__exit__`
  and PEP 249 §7. `aiosqlite.Connection.__aexit__` DOES close (closes
  its proxy thread + sqlite3 connection). Cross-driver code porting
  from `aiosqlite` must add an explicit `await conn.close()` (or
  switch to a pool) for eager-close semantics.

```python
# aiosqlite-style (closes on aexit):
async with aiosqlite.connect(":memory:") as conn:
    ...

# dqlitedbapi.aio-style (does NOT close on aexit, matches stdlib sqlite3):
async with await dqlitedbapi.aio.connect(...) as conn:
    ...
    await conn.close()  # explicit
```

## Layering (pool ownership)

Each `dqlitedbapi.Connection` (sync or async) owns exactly one
underlying `dqliteclient.DqliteConnection`. The dbapi layer does NOT
itself pool client connections — pooling lives one layer up:

- **SQLAlchemy users**: SA's `QueuePool` (and async siblings) over
  dbapi `Connection` objects is the production pool.
- **Direct dbapi users**: a custom pool over dbapi `Connection`
  objects is the supported pattern.
- `dqliteclient.ConnectionPool` is for direct-client usage and is
  unused by dqlitedbapi.

A dbapi `Connection.close()` always closes the underlying client
transport — not "return to a pool" — because the dbapi never
borrowed from one.

## Limitations vs. stdlib `sqlite3`

- **Multi-statement SQL is rejected.** `cursor.execute("SELECT 1;
  SELECT 2;")` raises `OperationalError` with "nonempty statement tail".
  Split into separate `execute()` calls.
- **Autocommit-by-default at the server.** Opposite of stdlib
  `sqlite3`'s implicit-transaction model — see Transactions above.
- **SERIALIZABLE isolation only.** Every statement is ordered by Raft;
  weaker isolation levels aren't exposed.
- **PEP 249 type sentinels (`STRING`, `BINARY`, `NUMBER`, `DATETIME`,
  `ROWID`) are unhashable.** Use chained equality against
  `description[i][1]`, NOT set/dict membership:

  ```python
  type_code = cur.description[i][1]
  if type_code == STRING or type_code == NUMBER:  # OK
      ...
  if type_code in {STRING, NUMBER}:               # raises TypeError
      ...
  ```

  The sentinels wrap multiple wire type codes (`NUMBER` covers
  INTEGER+FLOAT+BOOLEAN, `DATETIME` covers DATE+TIMESTAMP+ISO8601),
  so no canonical hash can satisfy the Python hash-eq invariant.
  Stdlib `sqlite3` doesn't export these sentinels at all, so the
  chained-equality form is the cross-driver-portable idiom.

- **`WITH ... INSERT/UPDATE/DELETE` (CTE-prefixed pure DML) reports
  zero `rowcount` and no `lastrowid`.** The driver dispatches between
  the row-returning and execute paths via a prefix-based heuristic;
  CTE-prefixed pure DML is misclassified as row-returning and the
  server's actual count / id is dropped. Stdlib `sqlite3` handles
  this correctly because it dispatches at the SQLite engine level.
  Workaround: rewrite as plain DML, or use `INSERT ... RETURNING id`
  to get the id back through the row-returning path. (PEP 249
  doesn't mandate `lastrowid` correctness for INSERT-via-CTE.)

## Cross-version semantic shift: NULL in BOOLEAN/DATETIME columns

Upstream dqlite commit `f30fc99` (`query: preserve SQLITE_NULL type
for NULL values`, 2026-01-25) changed the wire encoding of NULL cells
in columns declared `BOOLEAN`, `DATE`, `DATETIME`, or `TIMESTAMP`:

- **Before** `f30fc99`: a NULL cell was emitted with the column's
  coerced type — `BOOLEAN(0)` (decodes to `False`) or `ISO8601("")`
  (decodes to `""`), indistinguishable from a real `FALSE` / empty
  string.
- **After** `f30fc99`: a NULL cell is emitted with `SQLITE_NULL` and
  decodes to `None`.

Code that does `if cur.fetchone()[0] is None:` against an old-server
cluster will silently miss NULL rows. After a cluster upgrade past
`f30fc99`, the same code starts firing where it previously read
`False` / `""`. There is no driver-level handshake distinguishing the
two server versions — check your dqlite cluster version before relying
on `is None` for `BOOLEAN` / `DATETIME` columns.

## Layering

Three related packages play different roles:

- `dqliteclient.DqliteConnection` — the low-level async wire client.
  Directly speaks the dqlite wire protocol.
- `dqlitedbapi.Connection` — a sync PEP 249 wrapper built on top, runs a
  dedicated event-loop thread so sync code can use the async client
  transparently.
- `dqlitedbapi.aio.AsyncConnection` — the PEP 249–shaped async
  counterpart for code already running inside an event loop.

Use `dqlitedbapi` (sync or async) unless you specifically need
wire-level control.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup and contribution guidelines.

## License

MIT
