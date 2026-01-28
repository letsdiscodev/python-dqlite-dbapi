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
    cursor = await conn.cursor()
    await cursor.execute("SELECT 1")
    print(await cursor.fetchone())
    await conn.close()

asyncio.run(main())
```

## PEP 249 Compliance

- `apilevel = "2.0"`
- `threadsafety = 1`
- `paramstyle = "qmark"`

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup and contribution guidelines.

## License

MIT
