"""Async PEP 249-style surface.

``connect()`` is synchronous and opens the wire lazily (SQLAlchemy needs a sync factory);
``aconnect()`` awaits the open. Everything else re-exports the sync package's module-level
names so aiosqlite-style code can import one namespace.
"""

from typing import Final as _Final

from dqliteclient import DEFAULT_CLOSE_TIMEOUT_SECONDS as _DEFAULT_CLOSE_TIMEOUT_SECONDS
from dqliteclient import DEFAULT_TIMEOUT_SECONDS as _DEFAULT_TIMEOUT_SECONDS
from dqliteclient import DialFunc
from dqlitedbapi._module import (
    LEGACY_TRANSACTION_CONTROL,
    PARSE_COLNAMES,
    PARSE_DECLTYPES,
    __version__,
    apilevel,
    complete_statement,
    enable_callback_tracebacks,
    paramstyle,
    register_converter,
    sqlite_version,
    sqlite_version_info,
    threadsafety,
)
from dqlitedbapi.aio.connection import AsyncConnection, apply_stdlib_connect_kwargs
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import (
    CLUSTER_POLICY_REJECTION_PREFIX,
    FAILED_TO_CONNECT_PREFIX,
    AdapterLookupError,
    AmbiguousCommitError,
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
from dqlitedbapi.row import Row
from dqlitedbapi.types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    UNKNOWN,
    Binary,
    Date,
    DateFromTicks,
    DescriptionTuple,
    PrepareProtocol,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    register_adapter,
    unregister_adapter,
)
from dqlitewire import DEFAULT_MAX_CONTINUATION_FRAMES as _DEFAULT_MAX_CONTINUATION_FRAMES
from dqlitewire import DEFAULT_MAX_TOTAL_ROWS as _DEFAULT_MAX_TOTAL_ROWS

DEFAULT_CLOSE_TIMEOUT_SECONDS: _Final[float] = _DEFAULT_CLOSE_TIMEOUT_SECONDS
DEFAULT_TIMEOUT_SECONDS: _Final[float] = _DEFAULT_TIMEOUT_SECONDS

__all__ = [
    "__version__",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "sqlite_version",
    "sqlite_version_info",
    "LEGACY_TRANSACTION_CONTROL",
    "PARSE_DECLTYPES",
    "PARSE_COLNAMES",
    "connect",
    "aconnect",
    "AsyncConnection",
    "AsyncCursor",
    "DialFunc",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "AmbiguousCommitError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "AdapterLookupError",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "UNKNOWN",
    "Row",
    "DescriptionTuple",
    "PrepareProtocol",
    "register_adapter",
    "unregister_adapter",
    "register_converter",
    "complete_statement",
    "enable_callback_tracebacks",
    "CLUSTER_POLICY_REJECTION_PREFIX",
    "FAILED_TO_CONNECT_PREFIX",
]


def connect(
    address: str,
    *,
    database: str = "default",
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
    max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
    max_message_size: int | None = None,
    trust_server_heartbeat: bool = False,
    close_timeout: float = DEFAULT_CLOSE_TIMEOUT_SECONDS,
    dial_timeout: float | None = None,
    attempt_timeout: float | None = None,
    dial_func: DialFunc | None = None,
    busy_timeout: float = 5.0,
    session_mode: str | None = None,
    **stdlib_kwargs: object,
) -> AsyncConnection:
    """Build an :class:`AsyncConnection` that opens its wire session on first use.

    ``timeout`` bounds each RPC phase, not a whole call; wrap in ``asyncio.timeout`` for a
    wall-clock deadline.
    """
    connection = AsyncConnection(
        address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        max_message_size=max_message_size,
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
        dial_timeout=dial_timeout,
        attempt_timeout=attempt_timeout,
        dial_func=dial_func,
        busy_timeout=busy_timeout,
        session_mode=session_mode,
    )
    apply_stdlib_connect_kwargs(connection, stdlib_kwargs)
    return connection


async def aconnect(
    address: str,
    *,
    database: str = "default",
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
    max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
    max_message_size: int | None = None,
    trust_server_heartbeat: bool = False,
    close_timeout: float = DEFAULT_CLOSE_TIMEOUT_SECONDS,
    dial_timeout: float | None = None,
    attempt_timeout: float | None = None,
    dial_func: DialFunc | None = None,
    busy_timeout: float = 5.0,
    session_mode: str | None = None,
    **stdlib_kwargs: object,
) -> AsyncConnection:
    """Like :func:`connect`, but awaits the wire session before returning."""
    connection = connect(
        address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        max_message_size=max_message_size,
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
        dial_timeout=dial_timeout,
        attempt_timeout=attempt_timeout,
        dial_func=dial_func,
        busy_timeout=busy_timeout,
        session_mode=session_mode,
        **stdlib_kwargs,
    )
    try:
        await connection.connect()
    except BaseException:
        connection.force_close_transport()
        raise
    return connection
