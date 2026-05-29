"""PEP 249 compliant interface for dqlite."""

# Free-threaded Python (PEP 703) guard lives in ``dqlitewire.__init__``; relying on it is
# intentional. Don't add a guard here that bypasses the wire package's opt-in env var.

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
# Tier 2 = capability ceiling: connections shareable across threads with
# ``check_same_thread=False``; cursors never shareable. Default enforcement stays per-thread.
threadsafety: _Final[_Literal[2]] = 2
paramstyle: _Final[_Literal["qmark"]] = "qmark"

# SQLAlchemy compat. Values live in ``_constants`` so sync and async surfaces can't drift.
sqlite_version_info: _Final[tuple[int, int, int]] = _SQLITE_VERSION_INFO
sqlite_version: _Final[str] = _SQLITE_VERSION

# Stdlib ``sqlite3.LEGACY_TRANSACTION_CONTROL = -1`` parity (3.12+).
LEGACY_TRANSACTION_CONTROL: _Final[int] = -1

# Stdlib parity constants only; ``detect_types=`` is still rejected in ``connect()`` and these
# enable no converter behaviour (no converter machinery — see ``register_converter`` stub).
PARSE_DECLTYPES: _Final[int] = 1
PARSE_COLNAMES: _Final[int] = 2

__version__: _Final[str] = "0.2.2"

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
    "register_adapter",
    "unregister_adapter",
    "PrepareProtocol",
    "register_converter",
    "complete_statement",
    "enable_callback_tracebacks",
    "CLUSTER_POLICY_REJECTION_PREFIX",
    "FAILED_TO_CONNECT_PREFIX",
    "MAX_CONTINUATION_FRAMES_UPPER_BOUND",
    "Connection",
    "Cursor",
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
    check_same_thread: bool = True,
    session_mode: str | None = None,
    **unknown_kwargs: object,
) -> Connection:
    """Connect to a dqlite database. ``timeout`` is per-RPC-phase, not end-to-end."""
    # Accept stdlib's ``isolation_level``/``autocommit`` no-op sentinels so cross-driver code
    # passing stdlib defaults doesn't trip the kwarg-reject path (dqlite is fixed-mode autocommit).
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
    # Tight gate (exact-bool True / exact-int -1) matching the setter: a loose ``!= -1`` would
    # admit ``Decimal('-1')``, ``-1.0``, etc. and break ``isinstance(conn.autocommit, int)``.
    if (
        autoc is not _SENTINEL
        and autoc is not True
        and not (isinstance(autoc, int) and not isinstance(autoc, bool) and autoc == -1)
    ):
        raise NotSupportedError(
            f"dqlite connect() accepts autocommit=True or autocommit=-1 "
            f"(stdlib LEGACY_TRANSACTION_CONTROL) only; got {autoc!r}"
        )

    # Reject unsupported stdlib kwargs as NotSupportedError so callers can catch them inside
    # the dbapi error hierarchy, rather than leaking a ``TypeError`` past ``except dbapi.Error``.
    if unknown_kwargs:
        raise NotSupportedError(
            f"dqlite connect() rejects stdlib sqlite3 kwargs not supported "
            f"by this driver: {sorted(unknown_kwargs)}"
        )
    # Timeout validation happens in ``Connection.__init__``; don't re-validate here.
    conn = Connection(
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
        check_same_thread=check_same_thread,
        session_mode=session_mode,
    )
    # Route through the setters so ``connect(isolation_level=X)`` actually sticks (stdlib parity).
    if iso is not _SENTINEL:
        conn.isolation_level = iso
    if autoc is not _SENTINEL:
        conn.autocommit = autoc
    return conn


# ``register_adapter`` is a real Python-side bind hook; ``register_converter`` is unimplementable
# (dqlite's wire doesn't surface declared column types), as are the REPL helpers below.
from dqlitedbapi.types import (  # noqa: E402
    PrepareProtocol,
    register_adapter,
    unregister_adapter,
)


def register_converter(*args: object, **kwargs: object) -> _NoReturn:
    """Always raises NotSupportedError; converter dispatch is unimplementable on the wire."""
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 register_converter; "
        "the wire protocol does not surface declared column types for "
        "converter dispatch"
    )


def complete_statement(*args: object, **kwargs: object) -> _NoReturn:
    """Always raises NotSupportedError; REPL helper outside PEP 249."""
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 complete_statement; "
        "REPL-helper utility not in PEP 249's surface"
    )


def enable_callback_tracebacks(*args: object, **kwargs: object) -> _NoReturn:
    """Always raises NotSupportedError; this driver has no callback-handler family."""
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 enable_callback_tracebacks; "
        "this driver has no callback-handler family for the toggle to apply to"
    )


# Library NullHandler (logging HOWTO convention); the ``aio`` sub-package inherits via propagation.
logging.getLogger(__name__).addHandler(logging.NullHandler())
