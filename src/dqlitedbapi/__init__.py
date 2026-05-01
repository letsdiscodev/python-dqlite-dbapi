"""PEP 249 compliant interface for dqlite."""

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

from typing import NoReturn

from dqlitedbapi._constants import (
    SQLITE_VERSION as _SQLITE_VERSION,
)
from dqlitedbapi._constants import (
    SQLITE_VERSION_INFO as _SQLITE_VERSION_INFO,
)
from dqlitedbapi.connection import Connection
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
apilevel = "2.0"
# PEP 249 value 1: threads may share the module.
#
# This driver is stricter than the PEP minimum: each Connection is
# bound to the thread that created it. Any method call from a
# different thread raises ProgrammingError. Use one Connection per
# thread, or use the async API (dqlitedbapi.aio.aconnect) for a
# single-thread-per-loop model.
threadsafety = 1
paramstyle = "qmark"  # Question mark style: WHERE name=?

# SQLite compatibility attributes (for SQLAlchemy).
#
# The literal values live in ``dqlitedbapi._constants`` so the sync
# and the async surface (`dqlitedbapi.aio`) cannot drift; both
# modules re-export from the same source of truth. See
# ``_constants.py`` for the rationale (advertised tuple gates SA
# dialect feature paths) and the pin test
# (``tests/integration/test_sqlite_version_pin.py``) that verifies
# the value against the live cluster.
sqlite_version_info = _SQLITE_VERSION_INFO
sqlite_version = _SQLITE_VERSION

__version__ = "0.1.4"

__all__ = [  # noqa: RUF022 - grouped by PEP 249 section, not alphabetical
    # Module attributes
    "__version__",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "sqlite_version",
    "sqlite_version_info",
    # Functions
    "connect",
    "register_adapter",
    "register_converter",
    "complete_statement",
    "enable_callback_tracebacks",
    # Classes
    "Connection",
    "Cursor",
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
    max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
    max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
    trust_server_heartbeat: bool = False,
    close_timeout: float = 0.5,
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

    Returns:
        A Connection object
    """
    # Reject stdlib ``sqlite3.connect`` kwargs that this driver
    # cannot honour (``detect_types``, ``isolation_level``,
    # ``check_same_thread``, ``factory``, ``cached_statements``,
    # ``uri``, ``autocommit``) with ``NotSupportedError`` rather
    # than letting Python's call-protocol leak ``TypeError``
    # (which escapes ``except dbapi.Error:``). Cross-driver code
    # that passes stdlib kwargs through should be able to catch
    # the rejection inside the dbapi error hierarchy.
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
    )


# Module-level stdlib ``sqlite3``-parity stubs. dqlitedbapi does not
# support pluggable type adaptation / converter dispatch (the wire
# protocol carries type tags but no caller-side hook for adapter
# registration); the SQL-completeness predicate and callback
# traceback toggle are similarly stdlib-only utilities. Stub with
# ``NotSupportedError`` so cross-driver code that calls these at
# module level (``dqlitedbapi.register_adapter(decimal.Decimal,
# str)``) hits the dbapi error hierarchy instead of bare
# ``AttributeError``. Mirrors the per-class TPC / load_extension /
# create_function / create_collation stub family.


def register_adapter(*args: object, **kwargs: object) -> NoReturn:
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 register_adapter; "
        "pluggable type adaptation has no wire-layer hook"
    )


def register_converter(*args: object, **kwargs: object) -> NoReturn:
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 register_converter; "
        "the wire protocol does not surface declared column types for "
        "converter dispatch"
    )


def complete_statement(*args: object, **kwargs: object) -> NoReturn:
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 complete_statement; "
        "REPL-helper utility not in PEP 249's surface"
    )


def enable_callback_tracebacks(*args: object, **kwargs: object) -> NoReturn:
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 enable_callback_tracebacks; "
        "this driver has no callback-handler family for the toggle to apply to"
    )
