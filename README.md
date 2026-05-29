# dqlite-dbapi

A [PEP 249](https://peps.python.org/pep-0249/) (DB-API 2.0) driver for
[dqlite](https://dqlite.io/), Canonical's distributed SQLite. If you have
used Python's standard `sqlite3` module, this will feel familiar — same
`connect()` / `cursor()` / `execute()` / `fetchone()` shape — but it talks
to a replicated dqlite cluster over the network instead of a local file.

It comes in two flavors from one package: a **synchronous** API
(`dqlitedbapi`) and an **async** API (`dqlitedbapi.aio`).

## Is this the package you want?

Yes, if you want a drop-in-style database driver for dqlite — directly,
or under a tool that expects a DB-API 2.0 driver. For SQLAlchemy
specifically, use [sqlalchemy-dqlite](https://github.com/letsdiscodev/sqlalchemy-dqlite)
(it builds on this package).

## Installation

```bash
pip install dqlite-dbapi
```

Requires Python 3.13+.

## Usage

### Sync

```python
import dqlitedbapi

conn = dqlitedbapi.connect("localhost:9001")
cur = conn.cursor()
cur.execute("SELECT 1")
print(cur.fetchone())
conn.close()
```

### Async

```python
import asyncio
from dqlitedbapi.aio import aconnect

async def main():
    conn = await aconnect("localhost:9001")
    cur = conn.cursor()
    await cur.execute("SELECT 1")
    print(await cur.fetchone())
    await conn.close()

asyncio.run(main())
```

## Key facts

- `apilevel = "2.0"`, `paramstyle = "qmark"` (`?` placeholders),
  `threadsafety = 2`.
- **Autocommit by default.** Unlike stdlib `sqlite3`, the driver does not
  auto-`BEGIN` before a statement — wrap writes in an explicit transaction
  when you need atomic multi-statement semantics. See
  [Transactions](docs/transactions.md).
- **Isolation is always SERIALIZABLE** — every write goes through Raft
  consensus, every read is serializable.

## The dqlite Python stack

This is one of four layered packages. Each builds on the one below:

| Package | Role |
| --- | --- |
| [sqlalchemy-dqlite](https://github.com/letsdiscodev/sqlalchemy-dqlite) | SQLAlchemy 2.0 dialect |
| **dqlite-dbapi** — this package | PEP 249 (DB-API 2.0) driver — sync & async |
| [dqlite-client](https://github.com/letsdiscodev/python-dqlite-client) | Async wire client — pooling, leader discovery |
| [dqlite-wire](https://github.com/letsdiscodev/python-dqlite-wire) | Wire-protocol codec |

Using SQLAlchemy or an ORM? Prefer
[sqlalchemy-dqlite](https://github.com/letsdiscodev/sqlalchemy-dqlite).

## Documentation

- [Transactions](docs/transactions.md) — the autocommit-by-default model and how to group statements.
- [Differences from `sqlite3`](docs/differences-from-sqlite3.md) — what to
  expect when porting from the stdlib module (and a server-version NULL
  gotcha).
- [Differences from `aiosqlite`](docs/differences-from-aiosqlite.md) — for
  the async surface.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup and contribution guidelines.

## License

MIT
