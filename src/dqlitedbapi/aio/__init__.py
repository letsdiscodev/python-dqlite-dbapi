"""Async PEP 249-style interface for dqlite."""

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor

__all__ = [
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
