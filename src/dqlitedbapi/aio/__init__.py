"""Async PEP 249-style interface for dqlite."""

from dqlitedbapi import __version__
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import _validate_timeout
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

# SQLAlchemy dialect discovery reads ``dbapi.apilevel`` to confirm a
# PEP 249 shape; we expose the string for that handshake only. The
# module does NOT implement PEP 249 — cursor methods return
# coroutines. Cross-driver code that wants a synchronous PEP 249
# surface must import ``dqlitedbapi`` (the sync sibling), not
# ``dqlitedbapi.aio``. ``aiosqlite`` and ``asyncpg`` deliberately do
# not set ``apilevel`` for the same reason.
apilevel = "2.0"
# PEP 249 value 1: threads may share the module.
#
# The async API is further restricted: each AsyncConnection is bound
# to the event loop it was first used on (see dqlitedbapi.aio.connection).
# Use one AsyncConnection per loop.
threadsafety = 1
paramstyle = "qmark"  # Question mark style: WHERE name=?

# SQLite compatibility attributes (for SQLAlchemy).
#
# Mirror of the sync sibling in ``dqlitedbapi/__init__.py``; see that
# module for the rationale. The two constants must stay in lockstep —
# pinned by ``tests/integration/test_sqlite_version_pin.py``, which
# also asserts neither advertises more than the server actually bundles.
sqlite_version_info = (3, 35, 0)
sqlite_version = ".".join(str(v) for v in sqlite_version_info)

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
    close_timeout: float = 0.5,
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
        close_timeout: Budget (seconds) for the transport-drain during
            ``close()``. Forwarded to the underlying AsyncConnection.
            Default 0.5 s is sized for LAN.

    Returns:
        An AsyncConnection object
    """
    _validate_timeout(timeout)
    return AsyncConnection(
        address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
    )


async def aconnect(
    address: str,
    *,
    database: str = "default",
    timeout: float = 10.0,
    max_total_rows: int | None = 10_000_000,
    max_continuation_frames: int | None = 100_000,
    trust_server_heartbeat: bool = False,
    close_timeout: float = 0.5,
) -> AsyncConnection:
    """Connect to a dqlite database asynchronously.

    Unlike connect(), this awaits the TCP connection before returning.

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
        close_timeout: Budget (seconds) for the transport-drain during
            ``close()``. Forwarded to the underlying AsyncConnection.
            Default 0.5 s is sized for LAN.

    Returns:
        A connected AsyncConnection object
    """
    _validate_timeout(timeout)
    conn = AsyncConnection(
        address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
    )
    try:
        await conn.connect()
    except BaseException:
        # Clean up a partially-constructed AsyncConnection so loop-
        # bound locks, transport, and the reader task don't leak. The
        # SA dialect (DqliteDialect_aio.connect) uses the same
        # pattern. Catch BaseException to cover CancelledError from
        # an outer asyncio.timeout; suppress only Exception during
        # the close so the original cancel / error propagates.
        import contextlib

        with contextlib.suppress(Exception):
            await conn.close()
        raise
    return conn
