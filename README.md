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

`dqlite-dbapi` follows PEP 249's implicit-transaction model.

- The first DML statement on a connection starts a transaction; call
  `commit()` to flush it or `rollback()` to discard it.
- There is no autocommit mode. Every write goes through Raft consensus
  and every read is serializable.
- Calling `commit()` / `rollback()` before any query has run is a silent
  no-op (preserves the "no spurious connect" contract — we don't open
  a TCP connection just to send COMMIT).
- Calling `commit()` / `rollback()` with no active transaction
  (e.g. right after a DDL statement) is also silently successful, matching
  stdlib `sqlite3` semantics.
- The context-manager exit (`with conn: ...`) commits on clean exit and
  attempts rollback on exception. **Commit failures now propagate** — an
  earlier version silently swallowed them, which could hide data loss.

## Limitations vs. stdlib `sqlite3`

- **Multi-statement SQL is rejected.** `cursor.execute("SELECT 1;
  SELECT 2;")` raises `OperationalError` with "nonempty statement tail".
  Split into separate `execute()` calls.
- **No autocommit mode** — see above.
- **SERIALIZABLE isolation only.** Every statement is ordered by Raft;
  weaker isolation levels aren't exposed.

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
