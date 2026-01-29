"""Async PEP 249-style interface for dqlite."""

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

# PEP 249 module-level attributes (required by SQLAlchemy dialect initialization)
apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "qmark"  # Question mark style: WHERE name=?

# SQLite compatibility attributes (for SQLAlchemy)
sqlite_version_info = (3, 35, 0)
sqlite_version = "3.35.0"

__all__ = [
    # Module attributes
    "apilevel",
    "threadsafety",
    "paramstyle",
    # Functions
    "connect",
    "aconnect",
    # Classes
    "AsyncConnection",
    "AsyncCursor",
    # Exceptions
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]


def connect(
    address: str,
    *,
    database: str = "default",
    timeout: float = 10.0,
) -> AsyncConnection:
    """Create a dqlite connection (connects lazily on first use).

    This is a sync function that returns an AsyncConnection without
    establishing the TCP connection yet. SQLAlchemy requires connect()
    to be sync; the actual connection is made when the first query runs.

    Args:
        address: Node address in "host:port" format
        database: Database name to open
        timeout: Connection timeout in seconds

    Returns:
        An AsyncConnection object
    """
    return AsyncConnection(address, database=database, timeout=timeout)


async def aconnect(
    address: str,
    *,
    database: str = "default",
    timeout: float = 10.0,
) -> AsyncConnection:
    """Connect to a dqlite database asynchronously.

    Unlike connect(), this awaits the TCP connection before returning.

    Args:
        address: Node address in "host:port" format
        database: Database name to open
        timeout: Connection timeout in seconds

    Returns:
        A connected AsyncConnection object
    """
    conn = AsyncConnection(address, database=database, timeout=timeout)
    await conn.connect()
    return conn
