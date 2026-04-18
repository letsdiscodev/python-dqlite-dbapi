"""Async PEP 249-style interface for dqlite."""

import math

from dqlitedbapi import __version__
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
from dqlitedbapi.types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)

# PEP 249 module-level attributes (required by SQLAlchemy dialect initialization)
apilevel = "2.0"
# PEP 249 value 1: threads may share the module.
#
# The async API is further restricted: each AsyncConnection is bound
# to the event loop it was first used on (see dqlitedbapi.aio.connection).
# Use one AsyncConnection per loop.
threadsafety = 1
paramstyle = "qmark"  # Question mark style: WHERE name=?

# SQLite compatibility attributes (for SQLAlchemy)
sqlite_version_info = (3, 35, 0)
sqlite_version = "3.35.0"

__all__ = [
    # Module attributes
    "__version__",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "sqlite_version",
    "sqlite_version_info",
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
    # Type constructors
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    # Type objects
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
]


def connect(
    address: str,
    *,
    database: str = "default",
    timeout: float = 10.0,
    max_total_rows: int | None = 10_000_000,
    max_continuation_frames: int | None = 100_000,
    trust_server_heartbeat: bool = False,
) -> AsyncConnection:
    """Create a dqlite connection (connects lazily on first use).

    This is a sync function that returns an AsyncConnection without
    establishing the TCP connection yet. SQLAlchemy requires connect()
    to be sync; the actual connection is made when the first query runs.

    Args:
        address: Node address in "host:port" format
        database: Database name to open
        timeout: Connection timeout in seconds — must be a positive
            finite number. 0, negatives, and non-finite values are
            rejected here rather than silently passed through.
        max_total_rows: Cumulative row cap across continuation frames
            for a single query. Forwarded to the underlying
            AsyncConnection. None disables the cap.
        max_continuation_frames: Per-query continuation-frame cap.
            Forwarded to the underlying AsyncConnection.
        trust_server_heartbeat: Let the server-advertised heartbeat
            widen the per-read deadline. Default False.

    Returns:
        An AsyncConnection object
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ProgrammingError(f"timeout must be a positive finite number, got {timeout}")
    return AsyncConnection(
        address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        trust_server_heartbeat=trust_server_heartbeat,
    )


async def aconnect(
    address: str,
    *,
    database: str = "default",
    timeout: float = 10.0,
    max_total_rows: int | None = 10_000_000,
    max_continuation_frames: int | None = 100_000,
    trust_server_heartbeat: bool = False,
) -> AsyncConnection:
    """Connect to a dqlite database asynchronously.

    Unlike connect(), this awaits the TCP connection before returning.

    Args:
        address: Node address in "host:port" format
        database: Database name to open
        timeout: Connection timeout in seconds — must be positive and finite.
        max_total_rows: Cumulative row cap across continuation frames.
        max_continuation_frames: Per-query continuation-frame cap.
        trust_server_heartbeat: Let the server-advertised heartbeat
            widen the per-read deadline.

    Returns:
        A connected AsyncConnection object
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ProgrammingError(f"timeout must be a positive finite number, got {timeout}")
    conn = AsyncConnection(
        address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        trust_server_heartbeat=trust_server_heartbeat,
    )
    await conn.connect()
    return conn
