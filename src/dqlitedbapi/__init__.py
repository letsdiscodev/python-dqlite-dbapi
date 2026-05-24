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
# PEP 249 value 2: threads may share the module and connections.
#
# This declares the driver's CAPABILITY ceiling: connections are
# safe to share across threads once the user opts in via
# ``check_same_thread=False`` at ``connect()`` (matches stdlib
# sqlite3's convention — stdlib advertises ``threadsafety = 3``
# while defaulting ``check_same_thread=True`` to enforce strict
# per-thread at the Python layer). Cursors are explicitly NOT
# shareable across threads — the documented contract is "share
# connections, not cursors" (see ``Connection`` docstring); the
# ceiling stays at PEP 249 tier 2 for that reason. Tier 3 (cursors
# shareable too) is tracked as future work in
# ``issues/dbapi-threadsafety-tier-3-cursor-sharing-stdlib-parity.md``.
#
# DEFAULT enforcement is still per-thread: ``check_same_thread``
# defaults to ``True`` and ``Connection._check_thread()`` raises
# ``ProgrammingError`` on cross-thread method calls. Pass
# ``check_same_thread=False`` to use the connection sharing
# advertised here. The wire is always serialised by ``_op_lock``;
# the Phase 2 hardening (``_state_lock`` for transaction owner
# read-check-reserve, ``_cursors`` WeakSet add/discard) closes
# the cross-thread correctness gaps for the connection-shared
# path.
#
# This declaration uses the "advertise ceiling, default to floor"
# pattern stdlib sqlite3 established since CPython 3.11
# (bpo-45613 / gh-31464 made ``sqlite3.threadsafety`` dynamic via
# ``SQLITE_THREADSAFE`` mode; serialized = tier 3). The pattern
# describes capability rather than default enforcement;
# capability-detection libraries reading this value learn the
# safe-with-opt-in ceiling.
threadsafety: _Final[_Literal[2]] = 2
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

__version__: _Final[str] = "0.2.0"

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
    "AmbiguousCommitError",
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
    max_message_size: int | None = None,
    trust_server_heartbeat: bool = False,
    close_timeout: float = DEFAULT_CLOSE_TIMEOUT_SECONDS,
    dial_timeout: float | None = None,
    attempt_timeout: float | None = None,
    dial_func: DialFunc | None = None,
    busy_timeout: float = 5.0,
    check_same_thread: bool = True,
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
        max_message_size: Maximum allowed inbound frame size in
            bytes. ``None`` (default) falls back to the wire-layer
            default (64 MiB). Forwarded to the underlying
            :class:`Connection`. The wire layer validates the value
            (positive int, non-bool); pathological values raise
            ``ValueError`` from the wire layer at construction.
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
        busy_timeout: Maximum cumulative seconds to spend retrying
            BUSY responses before raising. Default ``5.0`` matches
            stdlib ``sqlite3.connect(timeout=5.0)``. Retries follow
            SQLite's deterministic ``sqliteDefaultBusyCallback``
            curve (1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100 ms
            then flat 100 ms). ``0`` disables retry (first BUSY
            raises immediately — stdlib parity). dqlite's VFS
            authorizer rejects the canonical ``PRAGMA busy_timeout``
            on the server side; this driver intercepts the PRAGMA at
            the Cursor layer so ``cur.execute("PRAGMA busy_timeout
            = 30000")`` works transparently and updates the same
            backing field as the kwarg.
        check_same_thread: When ``True`` (default), every method
            call on the returned Connection must come from the
            thread that called ``connect()``; cross-thread calls
            raise ``ProgrammingError``. When ``False``, the cross-
            thread check is gated off and the Connection can be
            shared across threads — the wire is already serialised
            by the per-Connection op-lock regardless of caller
            thread. Per-cursor result state is NOT thread-safe; the
            documented pattern is "share connections, not cursors."
            Matches stdlib ``sqlite3.connect(check_same_thread=
            False)``. The fork check is NEVER relaxed; cross-process
            Connection use raises ``InterfaceError`` in both modes.
            See ``dqlitedbapi.Connection`` docstring for the
            cursor / messages / close() contracts under the
            relaxation.

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
    # Tight gate matching the ``Connection.autocommit`` setter: only
    # exact-bool ``True`` and exact-int ``-1`` are accepted. Stdlib
    # parity (``sqlite3.connect`` requires ``int`` or
    # ``sqlite3.LEGACY_TRANSACTION_CONTROL`` exactly). The looser
    # ``autoc != -1`` predicate previously accepted ``Decimal('-1')``,
    # ``-1.0``, custom ``__eq__`` objects, etc. — breaking the
    # ``isinstance(conn.autocommit, int)`` cross-driver introspection
    # idiom that the setter docstring promises.
    if (
        autoc is not _SENTINEL
        and autoc is not True
        and not (isinstance(autoc, int) and not isinstance(autoc, bool) and autoc == -1)
    ):
        raise NotSupportedError(
            f"dqlite connect() accepts autocommit=True or autocommit=-1 "
            f"(stdlib LEGACY_TRANSACTION_CONTROL) only; got {autoc!r}"
        )

    # Reject other stdlib ``sqlite3.connect`` kwargs that this driver
    # cannot honour (``detect_types``, ``factory``, ``cached_statements``,
    # ``uri``) with ``NotSupportedError`` rather than letting Python's
    # call-protocol leak ``TypeError`` (which escapes ``except
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
    )
    # Apply the validated ``isolation_level`` / ``autocommit`` kwargs
    # via the setters on the freshly-constructed connection. Stdlib
    # parity: ``sqlite3.connect(":memory:", isolation_level=X)`` makes
    # ``conn.isolation_level == X``. The prior path popped + validated
    # these kwargs and then silently dropped them, breaking the
    # cross-driver porting idiom the setter docstrings explicitly
    # promised. Routing through the setters preserves the strict
    # validation each setter encodes (the connect-side gate is the
    # outer accept-set; the setter-side gate is the strict-int /
    # exact-string check). The setters tolerate the _SENTINEL no-op
    # via the same accept-set, so this is a single uniform path.
    if iso is not _SENTINEL:
        conn.isolation_level = iso
    if autoc is not _SENTINEL:
        conn.autocommit = autoc
    return conn


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
    """Stdlib ``sqlite3.register_converter`` parity stub.

    Always raises :class:`NotSupportedError`. The dqlite wire protocol
    does not surface declared column types for converter dispatch, so
    the stdlib's typename-keyed converter registry has no anchor here.
    Use :func:`register_adapter` for the bind-side hook (Python value
    to wire) — the read-side hook is genuinely unimplementable on the
    dqlite wire.
    """
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 register_converter; "
        "the wire protocol does not surface declared column types for "
        "converter dispatch"
    )


def complete_statement(*args: object, **kwargs: object) -> _NoReturn:
    """Stdlib ``sqlite3.complete_statement`` parity stub.

    Always raises :class:`NotSupportedError`. This is a stdlib REPL
    utility outside PEP 249's surface; dqlitedbapi ships PEP 249 only.
    Callers needing SQL completeness detection should use a third-party
    SQL parser.
    """
    raise NotSupportedError(
        "dqlitedbapi does not support stdlib sqlite3 complete_statement; "
        "REPL-helper utility not in PEP 249's surface"
    )


def enable_callback_tracebacks(*args: object, **kwargs: object) -> _NoReturn:
    """Stdlib ``sqlite3.enable_callback_tracebacks`` parity stub.

    Always raises :class:`NotSupportedError`. This toggle controls
    traceback printing for stdlib ``sqlite3``'s
    ``create_function``/``create_aggregate``/``create_collation``
    callback family; dqlitedbapi has no Python callback handlers
    (the wire layer does not surface user-defined SQL functions) for
    the toggle to apply to.
    """
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
