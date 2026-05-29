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
- `threadsafety = 2` (capability ceiling; default `check_same_thread=True`
  enforces strict per-thread — matches stdlib sqlite3's "advertise
  ceiling, default to floor" pattern)
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
on the leader. The `Connection.isolation_level` attribute exists for
pre-3.12 stdlib parity: the setter accepts `None` and the legacy
qualifier values (`""`, `"DEFERRED"`, `"IMMEDIATE"`, `"EXCLUSIVE"`,
case-insensitive) as no-ops, and the getter returns the last value set
(default `None`). Any other value — including `"SERIALIZABLE"` and
`"AUTOCOMMIT"` — raises `ProgrammingError`, since the qualifier cannot
change dqlite's single serialized isolation level. dqlite is
autocommit-by-default at the dbapi layer; explicit transactions are
managed via `conn.commit()` / `conn.rollback()` per PEP 249. The
SQLAlchemy dialect rejects `AUTOCOMMIT` on the same grounds.

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
`NUMBER`, `DATETIME`, `ROWID`), the working `register_adapter` /
`unregister_adapter` (process-global; mirrors stdlib's pre-3.12
behavior), and stdlib-sqlite3-parity stubs that raise
`NotSupportedError` (`register_converter`, `complete_statement`,
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
async with await dqlitedbapi.aio.aconnect(...) as conn:
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
  SELECT 2;")` raises `ProgrammingError("You can only execute one
  statement at a time.")` (stdlib's wording, but client-side, before
  any wire round-trip). Split into separate `execute()` calls; there
  is no `executescript` (it raises `NotSupportedError`).
- **`paramstyle = "qmark"` only.** stdlib accepts both `?` (qmark)
  AND `:name` (named) within a single connection; this driver
  advertises only `qmark` and rejects mappings with `ProgrammingError`.
- **Autocommit-by-default at the server.** Opposite of stdlib
  `sqlite3`'s implicit-transaction model — see Transactions above.
- **`Cursor.rowcount = len(rows)` after SELECT.** stdlib returns `-1`
  (rowcount is undefined for queries). dqlite knows the full result
  set at execute time, so the literal-PEP-249 reading is honoured.
  `rowcount = -1` is preserved for true non-result paths (PRAGMA
  write, never-executed cursor).
- **`Cursor.executemany` clears `lastrowid` to `None`.** stdlib leaves
  the prior INSERT's rowid sticky across executemany. The driver
  diverges deliberately (which row's rowid is "the" rowid for a batch
  of N inserts is ambiguous).
- **`Cursor.close()` clears `description` / `_rows` but preserves
  `rowcount` / `lastrowid`.** Matches stdlib for `rowcount`/`lastrowid`
  (both readable post-close); diverges only on `description`, which
  stdlib leaves populated whereas dqlite returns `None` (a closed
  cursor has no fetchable result set to describe).
- **`Cursor.lastrowid` returns `None` after a fresh CREATE TABLE.**
  stdlib `sqlite3` returns `0` for a never-INSERTed cursor; dqlite
  returns `None`. After the first INSERT/REPLACE both drivers agree
  on the sticky value through subsequent UPDATE / DELETE / DDL.
- **`Cursor.fetchmany(0)` returns `[]`.** stdlib (verified 3.12.3) and
  psycopg3 both treat `size=0` as "fetch arraysize" or "fetch all
  remaining"; dqlite reads PEP 249 literally (size=0 → 0 rows). Pass
  `None` or omit the argument to default to `arraysize`.
- **`Cursor.fetchmany(negative)` raises `ProgrammingError`.** Stdlib
  `sqlite3.Cursor.fetchmany` on Python 3.13+ rejects negative sizes
  with `ValueError`; older stdlib drained "fetch all remaining". The
  dbapi follows current stdlib but wraps the rejection in
  `ProgrammingError` to keep it in the dbapi.Error hierarchy per
  PEP 249 §7.
- **`cursor.execute("")` raises `ProgrammingError("empty statement")`.**
  stdlib silently accepts empty / whitespace-only / comment-only SQL
  as a no-op; the driver pre-flight rejects per PEP 249 §7 to surface
  caller bugs at the call site.
- **`threadsafety = 2`** (stdlib reports `3` — we don't claim Tier 3
  because cursors aren't shareable per the documented "share
  connections, not cursors" contract). Tier 2 = "threads may share
  the module AND connections"; matches the dbapi's
  ``check_same_thread=False`` opt-in behaviour. Default
  ``check_same_thread=True`` enforces strict per-thread; methods
  called from a foreign thread on a default-mode Connection raise
  ``ProgrammingError``. Tier 3 (sharing a single cursor across threads)
  is not supported.
- **No `executescript` / `create_function` / `create_aggregate` /
  `create_window_function` / `iterdump` / `backup` / `set_authorizer`
  / `serialize` / `blobopen`.** stdlib-specific APIs that have no
  server-side counterpart in dqlite. Stubs raise `NotSupportedError`.
- **SERIALIZABLE isolation only.** Every statement is ordered by Raft;
  weaker isolation levels aren't exposed.
- **Foreign keys are enforced by default.** The dqlite server defaults
  every fresh connection to `PRAGMA foreign_keys = ON`, so FK
  constraints are enforced out of the box — diverging from stdlib
  `sqlite3` / pysqlite, which default `OFF`. Issue
  `PRAGMA foreign_keys = OFF` per connection for SQLite's legacy
  unenforced behavior.
- **PEP 249 type sentinels (`STRING`, `BINARY`, `NUMBER`, `DATETIME`,
  `ROWID`) match a `type_code` by `==`, not by set/dict membership.**
  Use chained equality against `description[i][1]`:

  ```python
  type_code = cur.description[i][1]
  if type_code == STRING or type_code == NUMBER:  # OK
      ...
  if type_code in {STRING, NUMBER}:               # WRONG: silently False
      ...
  ```

  The sentinels wrap multiple wire type codes (`NUMBER` covers
  INTEGER+FLOAT+BOOLEAN, `DATETIME` covers DATE+TIMESTAMP+ISO8601) and
  match a `type_code` through ``__eq__``. They ARE hashable (so they can
  serve as dict keys internally), but a bare wire-int `type_code` does
  not hash equal to a sentinel, so `type_code in {STRING, NUMBER}`
  silently returns `False` rather than raising — which is why the
  chained-equality form is required. Stdlib `sqlite3` doesn't export
  these sentinels at all, so the chained-equality form is also the
  cross-driver-portable idiom.

- **`Binary(value)` leaks bare `TypeError` on bad input.** `Binary` is
  the stdlib `sqlite3.Binary = memoryview` alias, kept as a direct
  alias so `isinstance(Binary(b), memoryview)` holds for cross-driver
  porting code. Bad input (`Binary("not bytes")`, `Binary(123)`,
  `Binary(None)`) raises bare `TypeError` from the underlying
  `memoryview` constructor, *outside* the `dqlitedbapi.Error`
  hierarchy. The sibling `Date`/`Time`/`Timestamp`/`*FromTicks`
  constructors all wrap as `DataError`; `Binary` deliberately does
  not. Callers who want PEP 249 §7 hierarchy purity for binary input
  should wrap their own `try`/`except (TypeError, ValueError)` and
  re-raise as `dqlitedbapi.DataError`.

- **`WITH ... INSERT/UPDATE/DELETE` (CTE-prefixed pure DML) reports
  `rowcount == -1` and a stale `lastrowid`.** The driver dispatches
  between the row-returning and execute paths via a prefix-based
  heuristic; CTE-prefixed pure DML is misclassified as row-returning,
  so the server's actual count / id is dropped: `rowcount` comes back
  as `-1` ("undetermined") and `lastrowid` is left untouched, retaining
  whatever value a prior INSERT set rather than reflecting this
  statement. Stdlib `sqlite3` handles this correctly because it
  dispatches at the SQLite engine level.
  Workaround: rewrite as plain DML, or use `INSERT ... RETURNING id`
  to get the id back through the row-returning path. (PEP 249
  doesn't mandate `lastrowid` correctness for INSERT-via-CTE.)

- **`description[i][1]` is the `UNKNOWN` Type Object on empty result
  sets.** PEP 249 §6.1.2 requires `type_code` to compare equal to one
  of the Type Objects (`STRING` / `NUMBER` / `BINARY` / `DATETIME` /
  `ROWID`). On an empty result (zero rows AND zero per-column type
  tags — the wire layer derives types from the first row's header),
  the per-column affinity is genuinely unrecoverable from the wire; the
  driver emits the `UNKNOWN` Type Object (importable from
  `dqlitedbapi`) rather than synthesise a misleading default or `None`.
  Test for it with `type_code == UNKNOWN`; an `is None` check never
  matches. Callers needing column types on empty result sets should
  issue `PRAGMA table_info(<table>)` separately. The sibling
  NULL-first-row case is handled by a row-scan rescue (see
  `Cursor.description` docstring).

- **Result sets are fully materialised at `execute()` time.** Stdlib
  `sqlite3` streams rows from the C engine via `Cursor.fetchone`,
  yielding one row per call. dqlite drains every continuation frame
  inside `Cursor.execute()` before returning, materialising the full
  result into the cursor's in-memory `_rows`. The `fetchone` /
  `fetchmany` / `fetchall` surface then iterates the in-memory
  buffer; `Cursor.close()` is purely in-memory and does not send
  `INTERRUPT` to the server. For large queries, set `max_total_rows`
  (forwarded from `connect()`) to cap memory at the wire layer, or
  shape queries with explicit `LIMIT` to control batch size. This
  matches the dqlite wire protocol (continuation frames are
  server-pushed) but diverges from go-dqlite's lazy `Rows.Next`.

- **`SQLITE_TOOBIG` maps to `DataError`, not `DatabaseError`.** CPython
  `Modules/_sqlite/util.c::get_exception_class` maps `SQLITE_TOOBIG`
  (code 18, "string or BLOB exceeds size limit") to the generic
  `DatabaseError`. The dqlite dbapi maps it to the more specific
  `DataError` since the error is unambiguously a value-size violation.
  Cross-driver code catching `DatabaseError` keeps working
  (`DataError ⊂ DatabaseError`); code that distinguishes on the
  specific subclass catches more here than against stdlib.

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
