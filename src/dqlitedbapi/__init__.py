"""PEP 249 compliant interface for dqlite.

Closed-state error class
========================

Operations on a closed ``Connection`` or ``Cursor`` raise
``InterfaceError`` (subclass of ``Error``). PEP 249 §7 permits either
``InterfaceError`` or ``ProgrammingError`` for closed-state misuse;
this driver matches psycopg's convention. Stdlib ``sqlite3`` chose
``ProgrammingError``. Cross-driver code that wants to catch closed-
state misuse portably should catch ``Error`` (the parent class).

Limitations vs stdlib sqlite3
=============================

- ``Date``, ``Time``, ``Timestamp`` are constructor *functions* (not
  class aliases as in stdlib ``sqlite3.dbapi2``), so they raise PEP
  249 ``DataError`` on invalid inputs (year=0, month=13, etc.) rather
  than bare ``ValueError``. Trade-off:
  ``isinstance(value, dqlitedbapi.Date)`` raises ``TypeError`` because
  functions are not classes. Use ``isinstance(value, datetime.date)``
  for cross-driver porting code that runs against both stdlib
  ``sqlite3`` and ``dqlitedbapi``.
- ``DateFromTicks`` / ``TimeFromTicks`` / ``TimestampFromTicks``
  delegate to ``datetime.date.fromtimestamp`` (Date) and
  ``datetime.datetime.fromtimestamp`` (Time / Timestamp), rather
  than stdlib's ``time.localtime(ticks)[:3]`` / ``[3:6]`` / ``[:6]``.
  Sub-second precision in fractional ``ticks`` is preserved on the
  ``Time`` and ``Timestamp`` results (stdlib's ``time.localtime``
  truncates to integer seconds); ``Date`` returns ``datetime.date``
  with no sub-second component on either driver. The local-time
  interpretation matches stdlib on all three.
- ``total_changes`` is a method (callable; ``conn.total_changes()``)
  rather than stdlib's int-property. The method-form keeps
  ``hasattr(conn, "total_changes")`` returning ``True`` consistently
  with the rest of the unsupported-stub family.
"""

# Free-threaded Python (python3.13t / PEP 703) is not supported.
# The guard lives in ``dqlitewire.__init__`` (this package's transitive
# dependency via ``dqliteclient``), where it raises ``ImportError`` at
# import time. The ``_closed_flag`` pattern used by ``Connection``'s
# weakref finalizer relies on the GIL's C-level atomicity for
# list-element stores; relying on the wire-layer guard is
# intentional. Do NOT add a guard here that would bypass the wire
# package's opt-in env var — a user who set
# ``DQLITEWIRE_ALLOW_FREE_THREADED=1`` is signalling they accept
# the single-owner discipline across all layers.

import logging
from typing import Final as _Final
from typing import Literal as _Literal
from typing import NoReturn as _NoReturn

from dqliteclient import (
    DEFAULT_CLOSE_TIMEOUT_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    DialFunc,
)
from dqlitedbapi._constants import CLUSTER_POLICY_REJECTION_PREFIX
from dqlitedbapi._constants import (
    SQLITE_VERSION as _SQLITE_VERSION,
)
from dqlitedbapi._constants import (
    SQLITE_VERSION_INFO as _SQLITE_VERSION_INFO,
)
from dqlitedbapi.connection import (
    FAILED_TO_CONNECT_PREFIX,
    MAX_CONTINUATION_FRAMES_UPPER_BOUND,
    Connection,
)
from dqlitedbapi.cursor import Cursor
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
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
from dqlitewire import (
    DEFAULT_MAX_CONTINUATION_FRAMES as _DEFAULT_MAX_CONTINUATION_FRAMES,
)
from dqlitewire import (
    DEFAULT_MAX_TOTAL_ROWS as _DEFAULT_MAX_TOTAL_ROWS,
)

# PEP 249 module-level attributes
apilevel: _Final[_Literal["2.0"]] = "2.0"
# PEP 249 value 1: threads may share the module.
#
# This driver is stricter than the PEP minimum: each Connection is
# bound to the thread that created it. Any method call from a
# different thread raises ProgrammingError. Use one Connection per
# thread, or use the async API (dqlitedbapi.aio.aconnect) for a
# single-thread-per-loop model.
threadsafety: _Final[_Literal[1]] = 1
paramstyle: _Final[_Literal["qmark"]] = "qmark"  # Question mark style: WHERE name=?

# SQLite compatibility attributes (for SQLAlchemy).
#
# The literal values live in ``dqlitedbapi._constants`` so the sync
# and the async surface (`dqlitedbapi.aio`) cannot drift; both
# modules re-export from the same source of truth. See
# ``_constants.py`` for the rationale (advertised tuple gates SA
# dialect feature paths) and the pin test
# (``tests/integration/test_sqlite_version_pin.py``) that verifies
# the value against the live cluster.
sqlite_version_info: _Final[tuple[int, int, int]] = _SQLITE_VERSION_INFO
sqlite_version: _Final[str] = _SQLITE_VERSION

# Stdlib ``sqlite3.LEGACY_TRANSACTION_CONTROL = -1`` parity (3.12+).
# The ``Connection.autocommit`` setter already accepts the literal
# ``-1`` sentinel; expose the canonical name so cross-driver code can
# do ``conn.autocommit = dbapi.LEGACY_TRANSACTION_CONTROL`` without
# falling back to ``getattr(dbapi, "LEGACY_TRANSACTION_CONTROL", -1)``.
LEGACY_TRANSACTION_CONTROL: _Final[int] = -1

# Stdlib ``sqlite3.PARSE_DECLTYPES = 1`` / ``PARSE_COLNAMES = 2``
# parity. The values are exposed for cross-driver porting code that
# composes ``detect_types=`` flag values; the ``detect_types=`` connect
# kwarg itself is still rejected at the ``unknown_kwargs`` gate in
# ``connect()`` (this driver has no converter machinery — see the
# stubbed ``register_converter`` below). Exposing the constants only
# closes the AttributeError-outside-Error-hierarchy footgun; it does
# NOT enable any converter behaviour.
PARSE_DECLTYPES: _Final[int] = 1
PARSE_COLNAMES: _Final[int] = 2

__version__: _Final[str] = "0.1.6"

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
    "register_adapter",
    "unregister_adapter",
    "PrepareProtocol",
    "register_converter",
    "complete_statement",
    "enable_callback_tracebacks",
    # Diagnostic prefix surface
    "CLUSTER_POLICY_REJECTION_PREFIX",
    "FAILED_TO_CONNECT_PREFIX",
    # Validator caps
    "MAX_CONTINUATION_FRAMES_UPPER_BOUND",
    # Classes
    "Connection",
    "Cursor",
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
]


def connect(
    address: str,
    *,
    database: str = "default",
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
    max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
    trust_server_heartbeat: bool = False,
    close_timeout: float = DEFAULT_CLOSE_TIMEOUT_SECONDS,
    dial_timeout: float | None = None,
    attempt_timeout: float | None = None,
    dial_func: DialFunc | None = None,
    **unknown_kwargs: object,
) -> Connection:
    """Connect to a dqlite database.

    Args:
        address: Node address in "host:port" format
        database: Database name to open
        timeout: Per-RPC-phase timeout in seconds — must be a positive
            finite number. The same budget is applied to each phase of
            an operation (send, read, any continuation drain), so a
            single high-level call can take up to roughly N × ``timeout``
            end-to-end. To enforce a true end-to-end deadline, wrap the
            call in ``asyncio.timeout(...)``. ``0``, negatives, and
            non-finite values are rejected here rather than silently
            passed through to the underlying connection.
        max_total_rows: Cumulative row cap across continuation frames
            for a single query. Forwarded to the underlying
            :class:`Connection`. ``None`` disables the cap.
        max_continuation_frames: Per-query continuation-frame cap.
            Forwarded to the underlying :class:`Connection`.
        trust_server_heartbeat: Let the server-advertised heartbeat
            widen the per-read deadline. Default False.
        close_timeout: Budget (seconds) for the transport-drain during
            ``close()``. Forwarded to the underlying :class:`Connection`.
            Default 0.5 s is sized for LAN.
        dial_timeout: Per-TCP-connect budget (seconds) — mirrors
            go-dqlite's ``Config.DialTimeout``. ``None`` (default)
            collapses onto ``timeout``. Forwarded to the underlying
            :class:`Connection`.
        attempt_timeout: Per-attempt envelope (seconds) covering dial
            + handshake + first RPC — mirrors go-dqlite's
            ``Config.AttemptTimeout``. ``None`` (default) collapses
            onto ``timeout``. Forwarded to the underlying
            :class:`Connection`.
        dial_func: Caller-supplied async dialer replacing the default
            TCP path — mirrors go-dqlite's ``WithDialFunc``. Used for
            TLS, unix-socket transport, custom KEEPALIVE, etc.
            ``None`` (default) uses the standard
            ``asyncio.open_connection`` path. Forwarded to the
            underlying :class:`Connection`. See
            :data:`dqliteclient.DialFunc` for the protocol.

    Returns:
        A Connection object
    """
    # Accept the no-op sentinel values for stdlib's
    # ``isolation_level`` and ``autocommit`` kwargs so cross-driver
    # porting code that passes stdlib defaults through to the
    # constructor doesn't trip on the kwarg-reject path. The
    # ``isolation_level.setter`` and ``autocommit.setter`` already
    # accept these same sentinels post-construction as documented
    # no-ops; the constructor was the asymmetric surface.
    #
    # Stdlib parity ``conn = sqlite3.connect(":memory:",
    # isolation_level=None)`` yields ``conn.isolation_level is None``;
    # dqlite is fixed-mode autocommit at the wire layer, so the
    # only sentinel meaning the user can express is the "I
    # acknowledge the existing mode" no-op.
    _SENTINEL = object()
    iso = unknown_kwargs.pop("isolation_level", _SENTINEL)
    autoc = unknown_kwargs.pop("autocommit", _SENTINEL)
    # Accept stdlib pre-3.12 ``isolation_level`` accept-set as no-ops:
    # ``None``, ``""`` (the stdlib DEFAULT), ``"DEFERRED"``,
    # ``"IMMEDIATE"``, ``"EXCLUSIVE"``. See the
    # ``isolation_level.setter`` rationale on ``Connection``.
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
    if autoc is not _SENTINEL and autoc is not True and autoc != -1:
        raise NotSupportedError(
            f"dqlite connect() accepts autocommit=True or autocommit=-1 "
            f"(stdlib LEGACY_TRANSACTION_CONTROL) only; got {autoc!r}"
        )

    # Reject stdlib ``sqlite3.connect`` kwargs that this driver
    # cannot honour (``detect_types``, ``check_same_thread``,
    # ``factory``, ``cached_statements``, ``uri``) with
    # ``NotSupportedError`` rather than letting Python's call-
    # protocol leak ``TypeError`` (which escapes ``except
    # dbapi.Error:``). Cross-driver code that passes stdlib kwargs
    # through should be able to catch the rejection inside the
    # dbapi error hierarchy.
    if unknown_kwargs:
        raise NotSupportedError(
            f"dqlite connect() rejects stdlib sqlite3 kwargs not supported "
            f"by this driver: {sorted(unknown_kwargs)}"
        )
    # Validation happens in ``Connection.__init__`` (both ``timeout``
    # and ``close_timeout``); re-calling ``_validate_timeout`` here
    # was redundant and leaked the private symbol onto
    # ``dqlitedbapi.dir()``.
    return Connection(
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


# Module-level stdlib ``sqlite3``-parity hook: ``register_adapter``
# is a Python-side hook that runs in the driver before the wire
# encode (no wire-layer involvement needed). Common uses include
# Decimal/UUID/Path/Enum binding — patterns the existing ecosystem
# reaches for routinely.
#
# ``register_converter`` is genuinely unimplementable (dqlite's wire
# does not surface declared column types for converter dispatch);
# the SQL-completeness and callback-traceback helpers are likewise
# stdlib REPL utilities outside PEP 249. Those stay as
# NotSupportedError stubs.

# Re-export the implementation from ``dqlitedbapi.types`` where
# ``_convert_bind_param`` consults the registry. ``PrepareProtocol``
# is the stdlib-parity sentinel passed to ``__conform__`` for the
# adapter-discovery fallback path.
from dqlitedbapi.types import (  # noqa: E402
    PrepareProtocol,
    register_adapter,
    unregister_adapter,
)


def register_converter(*args: object, **kwargs: object) -> _NoReturn:
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 register_converter; "
        "the wire protocol does not surface declared column types for "
        "converter dispatch"
    )


def complete_statement(*args: object, **kwargs: object) -> _NoReturn:
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 complete_statement; "
        "REPL-helper utility not in PEP 249's surface"
    )


def enable_callback_tracebacks(*args: object, **kwargs: object) -> _NoReturn:
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 enable_callback_tracebacks; "
        "this driver has no callback-handler family for the toggle to apply to"
    )


# Convention from the Python logging HOWTO: attach a ``NullHandler``
# to the library's top-level logger so applications that have not
# configured logging don't see the ``lastResort`` stderr emission,
# and downstream code can silence the library cleanly via
# ``getLogger("dqlitedbapi").propagate = False``. The ``aio``
# sub-package inherits this handler via propagation; no separate
# handler is needed there.
logging.getLogger(__name__).addHandler(logging.NullHandler())
