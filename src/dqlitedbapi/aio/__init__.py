"""Async PEP 249-style interface for dqlite."""

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor

# PEP 249 module-level attributes (required by SQLAlchemy dialect initialization)
apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "qmark"  # Question mark style: WHERE name=?

# SQLite compatibility attributes (for SQLAlchemy)
sqlite_version_info = (3, 35, 0)
sqlite_version = "3.35.0"

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "aconnect",
    "AsyncConnection",
    "AsyncCursor",
]


async def aconnect(
    address: str,
    *,
    database: str = "default",
    timeout: float = 10.0,
) -> AsyncConnection:
    """Connect to a dqlite database asynchronously.

    Args:
        address: Node address in "host:port" format
        database: Database name to open
        timeout: Connection timeout in seconds

    Returns:
        An AsyncConnection object
    """
    conn = AsyncConnection(address, database=database, timeout=timeout)
    await conn.connect()
    return conn
