"""Async PEP 249-style interface for dqlite."""

import asyncio
import contextlib
import logging
from typing import Final as _Final
from typing import Literal as _Literal

# Re-export the stdlib-sqlite3-parity NotSupportedError stubs from
# the sync surface so cross-driver code porting from aiosqlite (which
# mirrors stdlib's full register_* / complete_statement /
# enable_callback_tracebacks surface) gets a clean
# ``dbapi.NotSupportedError`` rather than ``AttributeError`` —
# matching the discipline already applied to ``register_adapter``
# (re-exported from the sync surface for the same reason).
from dqliteclient import DialFunc
from dqlitedbapi import (  # module-level re-export
    LEGACY_TRANSACTION_CONTROL,
    PARSE_COLNAMES,
    PARSE_DECLTYPES,
    complete_statement,
    enable_callback_tracebacks,
    register_converter,
)
from dqlitedbapi import __version__ as _parent_version

# ``Final`` does not propagate through ``from X import Y`` aliases —
# the re-export creates a new module-level binding that needs its
# own annotation to match the sync sibling's discipline. Mirrors
# the four sibling ``__version__`` Final pins across the workspace.
__version__: _Final[str] = _parent_version
from dqlitedbapi._constants import (
    SQLITE_VERSION as _SQLITE_VERSION,
)
from dqlitedbapi._constants import (
    SQLITE_VERSION_INFO as _SQLITE_VERSION_INFO,
)
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
    DescriptionTuple,
    PrepareProtocol,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    register_adapter,
    unregister_adapter,
)
from dqlitewire import (
    DEFAULT_MAX_CONTINUATION_FRAMES as _DEFAULT_MAX_CONTINUATION_FRAMES,
)
from dqlitewire import (
    DEFAULT_MAX_TOTAL_ROWS as _DEFAULT_MAX_TOTAL_ROWS,
)

# SQLAlchemy's async dialect discovery reads ``dbapi.apilevel`` to
# confirm a PEP 249 shape; we expose ``"2.0"`` for that handshake.
# The async surface does NOT fully implement PEP 249 — fetch methods
# return coroutines, matching the de-facto async-DB-API convention
# used by aiosqlite and asyncpg. Cross-driver code that wants a
# synchronous PEP 249 surface must import ``dqlitedbapi`` (the sync
# sibling), not ``dqlitedbapi.aio``. (aiosqlite and asyncpg do not
# set ``apilevel`` because they are not consumed via SA's
# ``import_dbapi`` discovery path; we set it for SA dialect glue.)
logger = logging.getLogger(__name__)

apilevel: _Final[_Literal["2.0"]] = "2.0"
# PEP 249 value 1: threads may share the module.
#
# The async API is further restricted: each AsyncConnection is bound
# to the event loop it was first used on (see dqlitedbapi.aio.connection).
# Use one AsyncConnection per loop.
threadsafety: _Final[_Literal[1]] = 1
paramstyle: _Final[_Literal["qmark"]] = "qmark"  # Question mark style: WHERE name=?

# SQLite compatibility attributes (for SQLAlchemy).
#
# Re-exported from ``dqlitedbapi._constants`` so the sync and the
# async surface cannot drift. See ``_constants.py`` for the rationale
# and the pin test (``tests/integration/test_sqlite_version_pin.py``).
sqlite_version_info: _Final[tuple[int, int, int]] = _SQLITE_VERSION_INFO
sqlite_version: _Final[str] = _SQLITE_VERSION

__all__ = [  # grouped by PEP 249 section, not alphabetical
    # Module attributes
    "__version__",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "sqlite_version",
    "sqlite_version_info",
    "LEGACY_TRANSACTION_CONTROL",
    "PARSE_DECLTYPES",
    "PARSE_COLNAMES",
    # Functions
    "connect",
    "aconnect",
    # Classes
    "AsyncConnection",
    "AsyncCursor",
    # go-dqlite-parity types
    "DialFunc",
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
    # Type aliases
    "DescriptionTuple",
    # Type-adapter registry (shared module-global with the sync
    # surface; calling on either namespace mutates the same dict)
    "register_adapter",
    "unregister_adapter",
    "PrepareProtocol",
    # NotSupportedError stubs mirroring stdlib sqlite3 — symmetric
    # with the sync surface so cross-driver code porting from
    # aiosqlite / stdlib gets a dbapi.Error rather than
    # AttributeError.
    "register_converter",
    "complete_statement",
    "enable_callback_tracebacks",
]


def connect(
    address: str,
    *,
    database: str = "default",
    timeout: float = 10.0,
    max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
    max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
    trust_server_heartbeat: bool = False,
    close_timeout: float = 0.5,
    dial_timeout: float | None = None,
    attempt_timeout: float | None = None,
    dial_func: DialFunc | None = None,
    **unknown_kwargs: object,
) -> AsyncConnection:
    """Create a dqlite connection (connects lazily on first use).

    This is a sync function that returns an AsyncConnection without
    establishing the TCP connection yet. SQLAlchemy requires connect()
    to be sync; the actual connection is made when the first query runs.

    Args:
        address: Node address in "host:port" format
        database: Database name to open
        timeout: Per-RPC-phase timeout in seconds — must be a positive
            finite number. The same budget is applied to each phase
            (send, read, any continuation drain), so a single call
            can take up to roughly N × ``timeout`` end-to-end. Wrap
            callers in ``asyncio.timeout(...)`` to enforce a
            wall-clock deadline. 0, negatives, and non-finite values
            are rejected here rather than silently passed through.
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
        dial_timeout: Per-TCP-connect budget (seconds) — mirrors
            go-dqlite's ``Config.DialTimeout``. ``None`` (default)
            collapses onto ``timeout``. Forwarded to the underlying
            AsyncConnection.
        attempt_timeout: Per-attempt envelope (seconds) covering dial
            + handshake + first RPC — mirrors go-dqlite's
            ``Config.AttemptTimeout``. ``None`` (default) collapses
            onto ``timeout``. Forwarded to the underlying
            AsyncConnection.
        dial_func: Caller-supplied async dialer replacing the default
            TCP path — mirrors go-dqlite's ``WithDialFunc``. ``None``
            (default) uses the standard
            ``asyncio.open_connection`` path. See
            :data:`dqliteclient.DialFunc`.

    Returns:
        An AsyncConnection object
    """
    # Accept no-op sentinels for ``isolation_level`` / ``autocommit``
    # symmetric with the setter — see sync sibling for rationale.
    _SENTINEL = object()
    iso = unknown_kwargs.pop("isolation_level", _SENTINEL)
    autoc = unknown_kwargs.pop("autocommit", _SENTINEL)
    if iso is not _SENTINEL and iso is not None:
        raise NotSupportedError(f"dqlite connect() accepts isolation_level=None only; got {iso!r}")
    if autoc is not _SENTINEL and autoc is not True and autoc != -1:
        raise NotSupportedError(
            f"dqlite connect() accepts autocommit=True or autocommit=-1 "
            f"(stdlib LEGACY_TRANSACTION_CONTROL) only; got {autoc!r}"
        )
    # Reject stdlib ``sqlite3.connect`` kwargs as ``NotSupportedError``
    # so cross-driver porting code's ``except dbapi.Error:`` catches
    # the rejection instead of bare ``TypeError``. Sync sibling does
    # the same.
    if unknown_kwargs:
        raise NotSupportedError(
            f"dqlite connect() rejects stdlib sqlite3 kwargs not supported "
            f"by this driver: {sorted(unknown_kwargs)}"
        )
    # Validation happens in ``AsyncConnection.__init__`` (both
    # ``timeout`` and ``close_timeout``); re-calling
    # ``_validate_timeout`` here was redundant and asymmetric.
    return AsyncConnection(
        address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
        dial_timeout=dial_timeout,
        attempt_timeout=attempt_timeout,
        dial_func=dial_func,
    )


async def aconnect(
    address: str,
    *,
    database: str = "default",
    timeout: float = 10.0,
    max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
    max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
    trust_server_heartbeat: bool = False,
    close_timeout: float = 0.5,
    dial_timeout: float | None = None,
    attempt_timeout: float | None = None,
    dial_func: DialFunc | None = None,
    **unknown_kwargs: object,
) -> AsyncConnection:
    """Connect to a dqlite database asynchronously.

    Unlike connect(), this awaits the TCP connection before returning.

    Args:
        address: Node address in "host:port" format
        database: Database name to open
        timeout: Per-RPC-phase timeout in seconds — must be a positive
            finite number. The same budget is applied to each phase
            (send, read, any continuation drain), so a single call
            can take up to roughly N × ``timeout`` end-to-end. Wrap
            callers in ``asyncio.timeout(...)`` to enforce a
            wall-clock deadline. 0, negatives, and non-finite values
            are rejected here rather than silently passed through.
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
        dial_timeout: Per-TCP-connect budget (seconds) — mirrors
            go-dqlite's ``Config.DialTimeout``. ``None`` (default)
            collapses onto ``timeout``. Forwarded to the underlying
            AsyncConnection.
        attempt_timeout: Per-attempt envelope (seconds) covering dial
            + handshake + first RPC — mirrors go-dqlite's
            ``Config.AttemptTimeout``. ``None`` (default) collapses
            onto ``timeout``. Forwarded to the underlying
            AsyncConnection.
        dial_func: Caller-supplied async dialer replacing the default
            TCP path — mirrors go-dqlite's ``WithDialFunc``. ``None``
            (default) uses the standard
            ``asyncio.open_connection`` path. See
            :data:`dqliteclient.DialFunc`.

    Returns:
        A connected AsyncConnection object
    """
    # Accept no-op sentinels for ``isolation_level`` / ``autocommit``
    # symmetric with the setter — see sync sibling for rationale.
    _SENTINEL = object()
    iso = unknown_kwargs.pop("isolation_level", _SENTINEL)
    autoc = unknown_kwargs.pop("autocommit", _SENTINEL)
    if iso is not _SENTINEL and iso is not None:
        raise NotSupportedError(f"dqlite aconnect() accepts isolation_level=None only; got {iso!r}")
    if autoc is not _SENTINEL and autoc is not True and autoc != -1:
        raise NotSupportedError(
            f"dqlite aconnect() accepts autocommit=True or autocommit=-1 "
            f"(stdlib LEGACY_TRANSACTION_CONTROL) only; got {autoc!r}"
        )
    # Reject stdlib ``sqlite3.connect`` kwargs as ``NotSupportedError``;
    # see ``connect`` sibling.
    if unknown_kwargs:
        raise NotSupportedError(
            f"dqlite aconnect() rejects stdlib sqlite3 kwargs not supported "
            f"by this driver: {sorted(unknown_kwargs)}"
        )
    # Validation happens in ``AsyncConnection.__init__`` (both
    # ``timeout`` and ``close_timeout``); re-calling
    # ``_validate_timeout`` here was redundant and asymmetric.
    conn = AsyncConnection(
        address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
        dial_timeout=dial_timeout,
        attempt_timeout=attempt_timeout,
        dial_func=dial_func,
    )
    try:
        await conn.connect()
    except BaseException:
        # Clean up a partially-constructed AsyncConnection so loop-
        # bound locks, transport, and the reader task don't leak. The
        # SA dialect (DqliteDialect_aio.connect) uses the same
        # pattern. Catch BaseException to cover CancelledError from
        # an outer asyncio.timeout.
        #
        # ``asyncio.shield`` lets the inner ``close()`` task run to
        # completion even when a FRESH outer cancel (e.g. from an
        # ``asyncio.timeout(...)`` wrapping the caller's
        # ``await aconnect(...)``) lands while we are suspended in
        # ``await conn.close()``. Without the shield, the close
        # would be cancelled mid-flight and the bare ``raise`` below
        # would re-raise a ``CancelledError`` from the close site
        # instead of the original connect-time exception — the
        # original would survive only as ``__context__``.
        # ``contextlib.suppress(asyncio.CancelledError)`` absorbs the
        # outer-await CancelledError so the bare ``raise`` below
        # re-delivers the ORIGINAL exception (asyncio will re-raise
        # the cancel at the next await on this task). ``except
        # Exception`` catches non-cancel close-time failures (e.g.
        # OSError on a stale transport) and logs them at DEBUG so the
        # original connect error remains user-visible. Mirrors the
        # sibling ``dqliteclient.connect`` shape (commit 1ba9371) and
        # the SA-glue ``aio_close`` discipline.
        try:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(conn.close())
        except Exception:
            logger.debug(
                "aconnect: exception during cleanup-close after failed connect",
                exc_info=True,
            )
        raise
    return conn
