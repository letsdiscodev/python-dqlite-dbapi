"""Async PEP 249-style interface for dqlite.

``connect`` is sync (lazy TCP open) for stdlib/SQLAlchemy factory-shape parity;
``aconnect`` is async and awaits the open. The naming inversion vs the async
``dqliteclient.connect`` is deliberate: this module ships the PEP 249 surface.
"""

import asyncio
import contextlib
import logging
from typing import Final as _Final
from typing import Literal as _Literal

from dqliteclient import DEFAULT_CLOSE_TIMEOUT_SECONDS as _DEFAULT_CLOSE_TIMEOUT_SECONDS
from dqliteclient import DEFAULT_TIMEOUT_SECONDS as _DEFAULT_TIMEOUT_SECONDS

# Re-export sync-surface stdlib stubs so aiosqlite-porting code gets a clean
# dbapi.NotSupportedError rather than AttributeError.
from dqliteclient import DialFunc
from dqlitedbapi import (
    CLUSTER_POLICY_REJECTION_PREFIX,
    FAILED_TO_CONNECT_PREFIX,
    MAX_CONTINUATION_FRAMES_UPPER_BOUND,
    UNKNOWN,
    Row,
    complete_statement,
    enable_callback_tracebacks,
    register_converter,
)
from dqlitedbapi import LEGACY_TRANSACTION_CONTROL as _LEGACY_TRANSACTION_CONTROL
from dqlitedbapi import PARSE_COLNAMES as _PARSE_COLNAMES
from dqlitedbapi import PARSE_DECLTYPES as _PARSE_DECLTYPES
from dqlitedbapi import __version__ as _parent_version
from dqlitedbapi._constants import (
    SQLITE_VERSION as _SQLITE_VERSION,
)
from dqlitedbapi._constants import (
    SQLITE_VERSION_INFO as _SQLITE_VERSION_INFO,
)
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import (
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

# Final does not propagate through ``from X import Y`` aliases; the re-export
# binding needs its own annotation.
__version__: _Final[str] = _parent_version
LEGACY_TRANSACTION_CONTROL: _Final[int] = _LEGACY_TRANSACTION_CONTROL
PARSE_COLNAMES: _Final[int] = _PARSE_COLNAMES
PARSE_DECLTYPES: _Final[int] = _PARSE_DECLTYPES
DEFAULT_CLOSE_TIMEOUT_SECONDS: _Final[float] = _DEFAULT_CLOSE_TIMEOUT_SECONDS
DEFAULT_TIMEOUT_SECONDS: _Final[float] = _DEFAULT_TIMEOUT_SECONDS

# Set for SA's import_dbapi handshake. The async surface does NOT fully
# implement PEP 249 — fetch methods return coroutines (aiosqlite/asyncpg
# convention); import the sync ``dqlitedbapi`` for a synchronous surface.
logger = logging.getLogger(__name__)

apilevel: _Final[_Literal["2.0"]] = "2.0"
# PEP 249 tier 2. For the async surface this is informational: an
# AsyncConnection is shareable across tasks within one event loop, not across
# threads/loops (cross-loop use raises via _check_loop_binding).
threadsafety: _Final[_Literal[2]] = 2
paramstyle: _Final[_Literal["qmark"]] = "qmark"

# Re-exported from _constants so sync and async surfaces cannot drift.
sqlite_version_info: _Final[tuple[int, int, int]] = _SQLITE_VERSION_INFO
sqlite_version: _Final[str] = _SQLITE_VERSION

__all__ = [  # grouped by PEP 249 section, not alphabetical
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
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "AdapterLookupError",
    # OperationalError subclass marking an in-doubt commit (leader flip mid-COMMIT).
    "AmbiguousCommitError",
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
    # Shared module-global registry with the sync surface (same dict).
    "register_adapter",
    "unregister_adapter",
    "PrepareProtocol",
    # NotSupportedError stubs mirroring stdlib sqlite3 (sync-surface parity).
    "register_converter",
    "complete_statement",
    "enable_callback_tracebacks",
    # Diagnostic substring anchors for retry/disconnect classifier middleware.
    "CLUSTER_POLICY_REJECTION_PREFIX",
    "FAILED_TO_CONNECT_PREFIX",
    "MAX_CONTINUATION_FRAMES_UPPER_BOUND",
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
    **unknown_kwargs: object,
) -> AsyncConnection:
    """Return an AsyncConnection that opens its TCP connection lazily on first use.

    Sync (SQLAlchemy requires connect() to be sync); the timeout/dial_* knobs
    are forwarded to AsyncConnection. timeout is per-RPC-phase, so one call can
    take up to ~N × timeout; wrap callers in asyncio.timeout for a wall-clock
    deadline. See the sync connect() for the busy-retry + PRAGMA contract.
    """
    # No-op sentinels for isolation_level / autocommit, symmetric with the setter.
    _SENTINEL = object()
    iso = unknown_kwargs.pop("isolation_level", _SENTINEL)
    autoc = unknown_kwargs.pop("autocommit", _SENTINEL)
    from dqlitedbapi.connection import _STDLIB_IMPLICIT_TX_VALUES as _IL_OK

    if (
        iso is not _SENTINEL
        and iso is not None
        and not (isinstance(iso, str) and iso.upper() in _IL_OK)
    ):
        raise NotSupportedError(
            f"dqlite connect() accepts isolation_level in "
            f"[None, {', '.join(repr(v) for v in sorted(_IL_OK))}]; "
            f"got {iso!r}"
        )
    # Exact-int gate (rejects Decimal('-1') / -1.0 / custom __eq__), matching
    # the AsyncConnection.autocommit setter.
    if (
        autoc is not _SENTINEL
        and autoc is not True
        and not (isinstance(autoc, int) and not isinstance(autoc, bool) and autoc == -1)
    ):
        raise NotSupportedError(
            f"dqlite connect() accepts autocommit=True or autocommit=-1 "
            f"(stdlib LEGACY_TRANSACTION_CONTROL) only; got {autoc!r}"
        )
    # check_same_thread is sync-only: an AsyncConnection is loop-bound regardless
    # of any flag. Reject loudly so SA users know to use the sync surface.
    if "check_same_thread" in unknown_kwargs:
        raise NotSupportedError(
            "check_same_thread is a sync-only kwarg. The async "
            "surface (AsyncConnection) is bound to its event loop "
            "by asyncio's structured-concurrency contract; cross-"
            "loop use raises via _check_loop_binding regardless of "
            "any flag. Use the sync dqlitedbapi.connect() with "
            "check_same_thread=False, or ensure one AsyncConnection "
            "per event loop."
        )
    # Reject unsupported stdlib kwargs as NotSupportedError so porting code's
    # ``except dbapi.Error`` catches it instead of a bare TypeError.
    if unknown_kwargs:
        raise NotSupportedError(
            f"dqlite connect() rejects stdlib sqlite3 kwargs not supported "
            f"by this driver: {sorted(unknown_kwargs)}"
        )
    # timeout/close_timeout validated in AsyncConnection.__init__.
    conn = AsyncConnection(
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
    if iso is not _SENTINEL:
        conn.isolation_level = iso
    if autoc is not _SENTINEL:
        conn.autocommit = autoc
    return conn


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
    **unknown_kwargs: object,
) -> AsyncConnection:
    """Connect to a dqlite database, awaiting the TCP open before returning.

    Same knobs as connect() (forwarded to AsyncConnection); see it for the
    timeout semantics and the busy-retry + PRAGMA contract.
    """
    # No-op sentinels for isolation_level / autocommit, symmetric with the setter.
    _SENTINEL = object()
    iso = unknown_kwargs.pop("isolation_level", _SENTINEL)
    autoc = unknown_kwargs.pop("autocommit", _SENTINEL)
    from dqlitedbapi.connection import _STDLIB_IMPLICIT_TX_VALUES as _IL_OK

    if (
        iso is not _SENTINEL
        and iso is not None
        and not (isinstance(iso, str) and iso.upper() in _IL_OK)
    ):
        raise NotSupportedError(
            f"dqlite aconnect() accepts isolation_level in "
            f"[None, {', '.join(repr(v) for v in sorted(_IL_OK))}]; "
            f"got {iso!r}"
        )
    # Exact-int gate symmetric with the autocommit setter.
    if (
        autoc is not _SENTINEL
        and autoc is not True
        and not (isinstance(autoc, int) and not isinstance(autoc, bool) and autoc == -1)
    ):
        raise NotSupportedError(
            f"dqlite aconnect() accepts autocommit=True or autocommit=-1 "
            f"(stdlib LEGACY_TRANSACTION_CONTROL) only; got {autoc!r}"
        )
    # check_same_thread is sync-only; see connect() for rationale.
    if "check_same_thread" in unknown_kwargs:
        raise NotSupportedError(
            "check_same_thread is a sync-only kwarg. The async "
            "surface (AsyncConnection) is bound to its event loop "
            "by asyncio's structured-concurrency contract; cross-"
            "loop use raises via _check_loop_binding regardless of "
            "any flag. Use the sync dqlitedbapi.connect() with "
            "check_same_thread=False, or ensure one AsyncConnection "
            "per event loop."
        )
    # Reject unsupported stdlib kwargs as NotSupportedError; see connect().
    if unknown_kwargs:
        raise NotSupportedError(
            f"dqlite aconnect() rejects stdlib sqlite3 kwargs not supported "
            f"by this driver: {sorted(unknown_kwargs)}"
        )
    # timeout/close_timeout validated in AsyncConnection.__init__.
    conn = AsyncConnection(
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
    try:
        await conn.connect()
    except BaseException:
        # Clean up the partial connection so loop-bound locks/transport/reader
        # don't leak. BaseException covers CancelledError from an outer timeout.
        # shield lets the close finish even if a fresh outer cancel lands mid-await;
        # suppressing CancelledError/KI/SE here keeps the bare raise re-delivering
        # the ORIGINAL connect error rather than a cancel/signal from the close site.
        from dqliteclient.cluster import _observe_drain_exception

        inner_drain = asyncio.ensure_future(conn.close())
        inner_drain.add_done_callback(_observe_drain_exception)
        try:
            with contextlib.suppress(asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                await asyncio.shield(inner_drain)
        except Exception:
            logger.debug(
                "aconnect: exception during cleanup-close after failed connect",
                exc_info=True,
            )
        # close() may short-circuit on its _closed guard (concurrent foreign-thread
        # close) and skip nulling these slots, leaving stale loop-bound primitives
        # that would misreport as a cross-loop error on reuse. Absent on fixtures.
        with contextlib.suppress(AttributeError):
            conn._connect_lock = None
            conn._op_lock = None
            conn._loop_ref = None
        raise
    # Apply after a successful connect so a partial connection can't reach a
    # thawed setter; the cleanup arm above unwinds partial-connect state.
    if iso is not _SENTINEL:
        conn.isolation_level = iso
    if autoc is not _SENTINEL:
        conn.autocommit = autoc
    return conn
