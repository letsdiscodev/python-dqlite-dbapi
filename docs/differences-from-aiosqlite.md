# Differences from `aiosqlite`

The async surface lives under `dqlitedbapi.aio` and mirrors the sync
`dqlitedbapi` surface, plus async-specific `aconnect` / `AsyncConnection` /
`AsyncCursor`.

For easy porting from [aiosqlite](https://github.com/omnilib/aiosqlite), the
full PEP 249 module surface is re-exported under `dqlitedbapi.aio` from one
namespace: the module attributes (`apilevel`, `threadsafety`, `paramstyle`,
`sqlite_version`, `sqlite_version_info`), the exception hierarchy (`Warning`,
`Error`, `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`,
`IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`),
the type constructors (`Date`, `Time`, `Timestamp`, `DateFromTicks`,
`TimeFromTicks`, `TimestampFromTicks`, `Binary`), the type sentinels
(`STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`, `UNKNOWN`), the `Row`
factory and `DescriptionTuple`, `register_adapter` / `unregister_adapter`, and
the `NotSupportedError` stubs for stdlib-only APIs.

## The one difference to watch

**`AsyncConnection.__aexit__` does not close the connection.** It commits or
rolls back per the block outcome and leaves the connection reusable — matching
stdlib `sqlite3.Connection.__exit__` and PEP 249 §7. `aiosqlite`'s
`__aexit__` *does* close (it tears down its proxy thread and sqlite3
connection).

So when porting from aiosqlite, add an explicit `await conn.close()` (or use a
pool) where you relied on close-on-exit:

```python
# aiosqlite — closes on aexit:
async with aiosqlite.connect(":memory:") as conn:
    ...

# dqlitedbapi.aio — does NOT close on aexit (matches stdlib sqlite3):
async with await dqlitedbapi.aio.aconnect(...) as conn:
    ...
    await conn.close()   # explicit
```

Everything else under [Differences from `sqlite3`](differences-from-sqlite3.md)
applies to the async surface too.
