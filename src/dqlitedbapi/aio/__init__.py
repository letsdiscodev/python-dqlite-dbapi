"""Async PEP 249-style interface for dqlite.

Two top-level entry points with deliberately asymmetric sync/async
shapes:

* ``connect(address, ...)`` is a **sync** function returning an
  ``AsyncConnection`` whose TCP open is deferred to the first query.
  This matches the stdlib factory shape (sync function returning a
  Connection) and is the shape SQLAlchemy's async dialect glue
  (``DqliteDialect_aio.connect``) requires from
  ``import_dbapi().connect``. Cross-driver code porting from
  aiosqlite ports unchanged.

* ``aconnect(address, ...)`` is an **async** function that awaits
  the TCP open before returning. Use this when driving the async
  dbapi directly (no SA dialect involved); errors surface at the
  ``await`` site rather than on the first query.

The naming-inversion vs ``dqliteclient.connect`` (which is async) is
deliberate: this module ships the PEP 249 surface where stdlib
factory-shape parity is load-bearing. See the per-function docstrings
for the full contract of each entry point.
"""

import asyncio
import contextlib
import logging
from typing import Final as _Final
from typing import Literal as _Literal

from dqliteclient import DEFAULT_CLOSE_TIMEOUT_SECONDS as _DEFAULT_CLOSE_TIMEOUT_SECONDS
from dqliteclient import DEFAULT_TIMEOUT_SECONDS as _DEFAULT_TIMEOUT_SECONDS

# Re-export the stdlib-sqlite3-parity NotSupportedError stubs from
# the sync surface so cross-driver code porting from aiosqlite (which
# mirrors stdlib's full register_* / complete_statement /
# enable_callback_tracebacks surface) gets a clean
# ``dbapi.NotSupportedError`` rather than ``AttributeError`` —
# matching the discipline already applied to ``register_adapter``
# (re-exported from the sync surface for the same reason).
from dqliteclient import DialFunc
from dqlitedbapi import (  # module-level re-export
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

# ``Final`` does not propagate through ``from X import Y`` aliases —
# the re-export creates a new module-level binding that needs its
# own annotation to match the sync sibling's discipline. Mirrors
# the four sibling ``__version__`` Final pins across the workspace.
__version__: _Final[str] = _parent_version
LEGACY_TRANSACTION_CONTROL: _Final[int] = _LEGACY_TRANSACTION_CONTROL
PARSE_COLNAMES: _Final[int] = _PARSE_COLNAMES
PARSE_DECLTYPES: _Final[int] = _PARSE_DECLTYPES
DEFAULT_CLOSE_TIMEOUT_SECONDS: _Final[float] = _DEFAULT_CLOSE_TIMEOUT_SECONDS
DEFAULT_TIMEOUT_SECONDS: _Final[float] = _DEFAULT_TIMEOUT_SECONDS

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
# PEP 249 value 2: threads may share the module and connections.
#
# Mirrors the sync surface declaration (see ``dqlitedbapi.__init__``
# for the full rationale on the "advertise ceiling, default to
# floor" convention). The sync ``Connection`` opts into cross-
# thread sharing via ``check_same_thread=False``; the async
# ``AsyncConnection`` is bound to its event loop by asyncio's
# structured-concurrency contract (cross-loop use raises via
# ``_check_loop_binding``), so for the async surface the tier-2
# advertisement is informational — the actual capability is
# "share connection across tasks within the same event loop."
# Cursors are NOT shareable across threads/tasks (see
# ``AsyncConnection`` docstring).
threadsafety: _Final[_Literal[2]] = 2
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
    # dqlite-specific OperationalError subclass marking an in-doubt
    # commit (leader flip mid-COMMIT). Exposed alongside the PEP 249
    # standard set for cross-driver introspection symmetry.
    "AmbiguousCommitError",
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
    "UNKNOWN",
    # Row factory (sqlite3.Row equivalent)
    "Row",
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
    # Diagnostic-surface constants — the canonical substring
    # anchors classifier middleware uses to discriminate disconnect
    # / retry classes. Re-exported here for parity with the sync
    # surface so an async-only retry middleware author imports them
    # from ``dqlitedbapi.aio`` rather than reaching into the sync
    # module. ``is``-identity holds because these are the same
    # module-global objects.
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
        max_message_size: Maximum allowed inbound frame size in
            bytes. ``None`` (default) falls back to the wire-layer
            default (64 MiB). Forwarded to the underlying
            AsyncConnection. The wire layer validates the value
            (positive int, non-bool); pathological values raise
            ``ValueError`` from the wire layer at construction.
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
        busy_timeout: Maximum cumulative seconds to spend retrying
            BUSY responses before raising. Default ``5.0`` matches
            stdlib ``sqlite3.connect(timeout=5.0)``. See sync
            ``connect`` for the full retry-curve + PRAGMA
            interception contract — async surface mirrors it.

    Returns:
        An AsyncConnection object
    """
    # Accept no-op sentinels for ``isolation_level`` / ``autocommit``
    # symmetric with the setter — see sync sibling for rationale.
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
    # Tight exact-int gate symmetric with the
    # ``AsyncConnection.autocommit`` setter — sync sibling for
    # rationale (Decimal('-1') / -1.0 / custom-__eq__ rejected).
    if (
        autoc is not _SENTINEL
        and autoc is not True
        and not (isinstance(autoc, int) and not isinstance(autoc, bool) and autoc == -1)
    ):
        raise NotSupportedError(
            f"dqlite connect() accepts autocommit=True or autocommit=-1 "
            f"(stdlib LEGACY_TRANSACTION_CONTROL) only; got {autoc!r}"
        )
    # ``check_same_thread`` is a sync-only kwarg. The async surface
    # (AsyncConnection) is bound to its event loop by asyncio's
    # structured-concurrency contract — cross-loop use raises via
    # ``_check_loop_binding`` regardless of any flag, so the kwarg
    # has no equivalent semantic. Reject loudly with the sync-only
    # message so SA users porting from sqlite know they need the
    # sync surface (``dqlitedbapi.connect``) for the relaxation.
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
    )
    # Apply the validated ``isolation_level`` / ``autocommit`` kwargs
    # via the setters on the freshly-constructed connection — sync
    # sibling for cross-driver porting-idiom rationale.
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
        max_message_size: Maximum allowed inbound frame size in
            bytes. ``None`` (default) falls back to the wire-layer
            default (64 MiB). Forwarded to the underlying
            AsyncConnection. The wire layer validates the value
            (positive int, non-bool); pathological values raise
            ``ValueError`` from the wire layer at construction.
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
        busy_timeout: Maximum cumulative seconds to spend retrying
            BUSY responses before raising. Default ``5.0`` matches
            stdlib ``sqlite3.connect(timeout=5.0)``. Retries follow
            SQLite's deterministic ``sqliteDefaultBusyCallback``
            curve. ``0`` disables retry. See sync ``connect`` for the
            full PRAGMA-interception contract — the async surface
            mirrors it.

    Returns:
        A connected AsyncConnection object
    """
    # Accept no-op sentinels for ``isolation_level`` / ``autocommit``
    # symmetric with the setter — see sync sibling for rationale.
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
    # Tight exact-int gate symmetric with the setter — sync sibling
    # for rationale.
    if (
        autoc is not _SENTINEL
        and autoc is not True
        and not (isinstance(autoc, int) and not isinstance(autoc, bool) and autoc == -1)
    ):
        raise NotSupportedError(
            f"dqlite aconnect() accepts autocommit=True or autocommit=-1 "
            f"(stdlib LEGACY_TRANSACTION_CONTROL) only; got {autoc!r}"
        )
    # ``check_same_thread`` is a sync-only kwarg. See the lazy
    # ``connect()`` sibling for full rationale.
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
        max_message_size=max_message_size,
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
        dial_timeout=dial_timeout,
        attempt_timeout=attempt_timeout,
        dial_func=dial_func,
        busy_timeout=busy_timeout,
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
        # Schedule the cleanup-close as a Task with an explicit
        # ``_observe_drain_exception`` done-callback BEFORE awaiting
        # the shielded close. See sibling ``dqliteclient.connect``
        # for the orphan-task rationale.
        from dqliteclient.cluster import _observe_drain_exception

        inner_drain = asyncio.ensure_future(conn.close())
        inner_drain.add_done_callback(_observe_drain_exception)
        try:
            with contextlib.suppress(asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                # Absorb KeyboardInterrupt / SystemExit during the
                # cleanup-close too. The function's outer
                # ``except BaseException`` catch is designed to preserve
                # the original connect-time exception across cleanup;
                # a KI/SE delivered inside the shielded close (or its
                # done-callback chain) would otherwise propagate and
                # SUPPLANT the saved original via Python's
                # implicit-context machinery -- defeating the
                # "preserve original" invariant for the two signal
                # types that fell outside the narrower suppress.
                await asyncio.shield(inner_drain)
        except Exception:
            logger.debug(
                "aconnect: exception during cleanup-close after failed connect",
                exc_info=True,
            )
        # Defensive null-out: mirrors AsyncConnection.__aenter__'s
        # cleanup arm. ``close()`` may have short-circuited at its
        # TOP-of-method ``if self._closed: return`` guard because a
        # concurrent close (foreign-thread ``force_close_transport``)
        # flipped ``_closed=True`` first. The short-circuit then
        # skips the never-connected branch that nulls these slots,
        # leaving the lazy loop-bound primitives stale. A subsequent
        # reuse on a different loop would hit a misleading
        # cross-loop diagnostic in ``_ensure_locks`` instead of the
        # documented fresh-connect path. Tolerate the attributes
        # being absent on fixture-built objects.
        with contextlib.suppress(AttributeError):
            conn._connect_lock = None
            conn._op_lock = None
            conn._loop_ref = None
        raise
    # Apply the validated ``isolation_level`` / ``autocommit`` kwargs
    # via the setters on the freshly-connected AsyncConnection. Stdlib
    # parity: ``sqlite3.connect(":memory:", isolation_level=X)`` makes
    # ``conn.isolation_level == X``. See the sync ``connect()`` sibling
    # for the cross-driver porting-idiom rationale. Done after a
    # successful ``conn.connect()`` so a partially-constructed
    # connection cannot land on a thawed-out setter; the cleanup-close
    # arm above unwinds the partial-connect state separately.
    if iso is not _SENTINEL:
        conn.isolation_level = iso
    if autoc is not _SENTINEL:
        conn.autocommit = autoc
    return conn
