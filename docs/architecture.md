# Architecture

This page records the design decisions behind `dqlite-dbapi`. The user-facing
behaviour they produce is documented in [Transactions](transactions.md) and
[Differences from `sqlite3`](differences-from-sqlite3.md).

## Layering

```
dqlitedbapi          sync PEP 249 surface: Connection, Cursor
    │  one daemon thread + asyncio loop per Connection
dqlitedbapi.aio      async surface: AsyncConnection, AsyncCursor  ← all driver logic
    │
dqliteclient         DqliteConnection (wire session), ClusterClient (leader discovery)
```

All driver logic lives in `dqlitedbapi.aio`. The sync package is a thin
adapter: a sync `Connection` owns one `AsyncConnection` and one background
thread running an asyncio event loop; every sync method submits the matching
coroutine to that loop and blocks on the result. Fetch methods never hop to
the loop: results are fully buffered at execute time, so the sync cursor reads
the buffer directly.

Modules:

| Module | Responsibility |
| --- | --- |
| `aio/connection.py` | `AsyncConnection`: lifecycle, transactions, session mode, operation lock |
| `aio/cursor.py` | `AsyncCursor`: execute / executemany, result buffering, description |
| `connection.py`, `cursor.py` | sync adapters over the async classes |
| `_loop.py` | the background loop thread used by the sync adapter |
| `_sql.py` | statement classification (row-returning, DML verbs, BEGIN rewrite, PRAGMA intercept) |
| `_busy.py` | `SQLITE_BUSY` retry curve |
| `exceptions.py` | PEP 249 hierarchy and the client-error to dbapi-error mapping |
| `types.py`, `row.py` | type objects, constructors, adapters, datetime codecs, `Row` |

Lexical SQL helpers (comment stripping, top-level statement splitting,
literal blanking, leading keyword) come from `dqliteclient.sql`, which the
client also uses for its own transaction tracking. `_sql.py` holds only the
semantic rules the driver adds on top.

## Connection lifecycle

- Constructing a connection validates its arguments and does no I/O.
- The first operation (or an explicit `connect()` / `aconnect()`) resolves
  the cluster leader from the seed address through `ClusterClient`, opens the
  wire session against the leader, and applies the session mode.
- **An invalidated connection is a closed connection.** When the underlying
  wire session is lost (transport error, leader change, cancellation or
  interrupt during an operation), `closed` becomes `True`, `invalidated`
  becomes `True`, and every further operation raises `InterfaceError`. The
  driver never reconnects silently: a reconnect would drop transaction state
  the caller believes is still open. Pools (SQLAlchemy's included) discard the
  connection and open a fresh one.
- `close()` is idempotent and does not roll back (stdlib `sqlite3` parity).
- `force_close_transport()` is synchronous, thread-safe, never raises, and
  tears the socket down without waiting for an in-flight operation. It exists
  for pool `terminate` paths and for shutting down from a thread that is not
  the connection's owner.

## Threading and concurrency

### Sync surface

- `check_same_thread=True` (default): every method except
  `force_close_transport()` must be called from the thread that created the
  connection; other threads get `ProgrammingError`.
- `check_same_thread=False`: any thread may call the connection. Calls are
  serialised by a lock. Cursors are still single-thread objects, as in
  `sqlite3`: share the connection, give each thread its own cursor.
- **Interrupts close the connection.** A `KeyboardInterrupt` or `SystemExit`
  delivered while a call is blocked cancels the in-flight operation,
  force-closes the connection, and propagates. The driver must not be
  re-entered from a signal handler; that case is out of contract.
- The sync side imposes no deadline of its own. Every await in the async
  layer is already bounded by the client's per-RPC timeouts, so a blocked sync
  call ends when those fire or when `force_close_transport()` is called from
  another thread.

### Async surface

- An `AsyncConnection` binds to the event loop it first runs on. Using it
  from another loop raises `InterfaceError`; it cannot be shared across
  `asyncio.run()` calls or threads.
- One operation at a time: an `asyncio.Lock` serialises execute, commit and
  rollback across tasks on the same loop. Waiting tasks queue rather than
  fail. A cursor is a single-task object.
- Cancellation during a wire round-trip invalidates the connection (see
  above), because the server-side effect of the interrupted statement is
  unknown.

## Statements

- One statement per `execute()`; a second statement after `;` raises
  `ProgrammingError` before any network round-trip. `CREATE TRIGGER` bodies
  are understood, so their inner `;` do not count.
- Empty, comment-only, and NUL-containing SQL raise `ProgrammingError`.
- A `?` count that does not match the parameter count raises
  `ProgrammingError` client-side.
- Row-returning statements (`SELECT`, `VALUES`, `PRAGMA`, `EXPLAIN`, a
  `WITH` whose final statement is one of those, and any DML with
  `RETURNING`) go through the query path and populate `description` and the
  row buffer. Everything else goes through the exec path and populates
  `rowcount` (INSERT/UPDATE/DELETE/REPLACE only, else -1) and `lastrowid`
  (INSERT/REPLACE only, sticky across other statements).
- `PRAGMA busy_timeout` is answered locally: the server rejects it, and the
  driver owns the busy budget.
- A bare `BEGIN` / `BEGIN TRANSACTION` is rewritten according to the session
  mode: `immediate` → `BEGIN IMMEDIATE`, `exclusive` → `BEGIN EXCLUSIVE`,
  `deferred` and `read_only` → unchanged. An explicit qualifier is never
  rewritten.
- `SQLITE_BUSY` is retried on SQLite's default busy-callback curve until the
  connection's `busy_timeout` budget is spent. Each `executemany` iteration
  retries independently.
- `description` carries `(name, type_code, None, None, None, None, None)`;
  the type code is the wire `ValueType` of the first non-NULL cell in the
  column, or the `UNKNOWN` sentinel when there is none.
- `ISO8601` and `UNIXTIME` wire cells decode to `datetime` objects; every
  other cell is passed through as the wire primitive.

## Transactions

- Autocommit by default: no implicit `BEGIN`.
- `commit()` / `rollback()` are no-ops before the first connection and when no
  transaction is open. A server reply of "no transaction is active" is
  swallowed. Every other failure propagates.
- A `COMMIT` that fails with a leadership-lost code raises
  `AmbiguousCommitError` (an `OperationalError`): the write may or may not
  have been applied.
- `transaction()` is a context manager issuing `BEGIN` / `COMMIT` /
  `ROLLBACK`. Calling `commit()` or `rollback()` inside the block raises
  `InterfaceError`, because the block owns the boundaries.
- `with conn:` / `async with conn:` commit on success and roll back on
  exception; neither closes the connection.
- `session_mode` is read-only; `set_session_mode()` changes it, emitting
  `PRAGMA query_only` when crossing the `read_only` boundary. It may not be
  called inside a transaction.

## Errors

Client exceptions are translated at the boundary by `exceptions.translate`:

| Client exception | dbapi exception |
| --- | --- |
| `OperationalError` with code | class chosen from the SQLite primary code (constraint → `IntegrityError`, internal/nomem/notfound → `InternalError`, toobig → `DataError`, range/misuse → `InterfaceError`, corrupt/format/notadb → bare `DatabaseError`, dqlite parse/notfound → `ProgrammingError`, else `OperationalError`) |
| `DqliteConnectionError`, `ClusterError`, `ProtocolError` | `OperationalError` with `code=None` |
| `ClusterPolicyError` | `InterfaceError` prefixed `Cluster policy rejection` (never retried) |
| `DataError`, wire `EncodeError` | `DataError` |
| `InterfaceError` | `InterfaceError` |
| any other `DqliteError` | `DatabaseError` |

Every raised error keeps `code` and `raw_message` (the untruncated server
text). Connect-time failures are prefixed `Failed to connect: `. These
prefixes and the `Connection is closed` / `Cursor is closed` /
`invalidated (id=` wordings are load-bearing: `sqlalchemy-dqlite` classifies
disconnects on them.

## Contract with sqlalchemy-dqlite

The dialect relies on exactly this dbapi surface, beyond PEP 249 itself:

- module: `connect`, exception classes, `paramstyle`, `sqlite_version_info`,
  `FAILED_TO_CONNECT_PREFIX`, `CLUSTER_POLICY_REJECTION_PREFIX`;
- connection: `cursor()`, `commit()`, `rollback()`, `close()`,
  `force_close_transport()`, `connect()` (async), `session_mode`,
  `default_session_mode`, `set_session_mode()`, `in_transaction`;
- cursor: `execute()`, `executemany()`, `description`, `rowcount`,
  `lastrowid`, `drain_rows()` (async), `close()`;
- `types.format_utc_offset()` for the dialect's datetime processors.
