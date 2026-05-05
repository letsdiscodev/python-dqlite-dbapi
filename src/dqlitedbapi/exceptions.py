"""PEP 249 exception hierarchy for dqlite."""

import sqlite3 as _stdlib_sqlite3
from functools import lru_cache

__all__ = [
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
]


# Hardcoded primary SQLite result-code table. The numeric range 0-28
# (plus 100 / 101 for ROW / DONE) collides with stdlib's authorizer-
# action constants (``SQLITE_CREATE_INDEX = 1``, ``SQLITE_CREATE_TABLE
# = 2``, ``SQLITE_CREATE_TRIGGER = 7``, ``SQLITE_DETACH = 25``,
# ``SQLITE_ALTER_TABLE = 26``, etc.) — a ``dir()`` walk over the
# ``sqlite3`` module yields the alphabetically-first name per numeric
# value, which is the AUTHORIZER constant for ~half the primary
# error codes. Hardcoding the canonical primary names per the
# upstream SQLite ``rescode.html`` page guarantees the right
# symbol even when stdlib's constant set grows.
#
# Source of truth: https://www.sqlite.org/rescode.html
_PRIMARY_RESULT_CODE_NAMES: dict[int, str] = {
    0: "SQLITE_OK",
    1: "SQLITE_ERROR",
    2: "SQLITE_INTERNAL",
    3: "SQLITE_PERM",
    4: "SQLITE_ABORT",
    5: "SQLITE_BUSY",
    6: "SQLITE_LOCKED",
    7: "SQLITE_NOMEM",
    8: "SQLITE_READONLY",
    9: "SQLITE_INTERRUPT",
    10: "SQLITE_IOERR",
    11: "SQLITE_CORRUPT",
    12: "SQLITE_NOTFOUND",
    13: "SQLITE_FULL",
    14: "SQLITE_CANTOPEN",
    15: "SQLITE_PROTOCOL",
    16: "SQLITE_EMPTY",
    17: "SQLITE_SCHEMA",
    18: "SQLITE_TOOBIG",
    19: "SQLITE_CONSTRAINT",
    20: "SQLITE_MISMATCH",
    21: "SQLITE_MISUSE",
    22: "SQLITE_NOLFS",
    23: "SQLITE_AUTH",
    24: "SQLITE_FORMAT",
    25: "SQLITE_RANGE",
    26: "SQLITE_NOTADB",
    27: "SQLITE_NOTICE",
    28: "SQLITE_WARNING",
    100: "SQLITE_ROW",
    101: "SQLITE_DONE",
}


@lru_cache(maxsize=1)
def _stdlib_extended_code_to_name() -> dict[int, str]:
    """Cache stdlib's extended-code ``SQLITE_*`` constants
    (codes >= 256) as a code-to-name map. Built lazily on first
    access.

    Extended codes use upper bits (subcode << 8 | primary) so they
    do NOT collide with the authorizer / opcode / limit / config
    constants, which are all in the 0-255 range. Walking
    ``dir(sqlite3)`` and filtering on ``value >= 256`` yields a
    clean code-to-name table for every extended result code stdlib
    exposes (e.g. ``SQLITE_CONSTRAINT_UNIQUE = 2067``,
    ``SQLITE_IOERR_READ = 266``).
    """
    table: dict[int, str] = {}
    for name in dir(_stdlib_sqlite3):
        if not name.startswith("SQLITE_"):
            continue
        value = getattr(_stdlib_sqlite3, name)
        if not isinstance(value, int):
            continue
        if value < 256:
            # Primary codes (0-28) and authorizer / opcode / limit
            # constants share this range; primary error names are
            # hand-curated in ``_PRIMARY_RESULT_CODE_NAMES`` to dodge
            # the alphabetical-collision ambiguity.
            continue
        table.setdefault(value, name)
    return table


def _sqlite_errorname(code: int | None) -> str | None:
    """Look up the symbolic SQLite error name for ``code``. Returns
    ``None`` for ``None`` codes and for codes not present in either
    the primary-code table or stdlib's extended-code constant set
    (e.g. dqlite-namespace codes ≥1000)."""
    if code is None:
        return None
    name = _PRIMARY_RESULT_CODE_NAMES.get(code)
    if name is not None:
        return name
    return _stdlib_extended_code_to_name().get(code)


class Warning(Exception):  # PEP 249 mandated class name
    """PEP 249 Warning class.

    Exported for compatibility with generic cross-driver code and
    for symmetry with ``Connection.messages`` / ``Cursor.messages``
    (both PEP 249 extension surfaces). The driver does not currently
    raise ``Warning`` — the dqlite wire protocol does not surface the
    SQLite-level warning conditions (data truncation on BLOB / TEXT
    bind, implicit type conversion during bind, etc.) that stdlib
    ``sqlite3`` would report. The parallel ``Connection.messages``
    attribute is therefore always empty in practice. If a concrete
    warning condition surfaces later, populate ``messages`` and use
    this class as the tuple's first element.
    """

    pass


# Cap on ``raw_message`` carried by any code-bearing dbapi Error.
# Mirrors ``dqliteclient.exceptions.DqliteError._MAX_RAW_MESSAGE``.
# The wire layer caps a single FailureResponse at ~64 KiB; combined
# with BaseExceptionGroup chains and cross-process pickling, an
# unbounded ``raw_message`` can produce multi-MB pickled exception
# payloads. 4 KiB is well above any realistic SQLite error string
# while bounding the worst-case fan-out.
_MAX_RAW_MESSAGE: int = 4 * 1024


def _cap_raw_message(raw_message: str) -> str:
    if len(raw_message) <= _MAX_RAW_MESSAGE:
        return raw_message
    overflow = len(raw_message) - _MAX_RAW_MESSAGE
    return raw_message[:_MAX_RAW_MESSAGE] + f"... [raw_message truncated, {overflow} codepoints]"


class Error(Exception):
    """Base class for all database errors."""

    def __reduce__(
        self,
    ) -> tuple[type["Error"], tuple[object, ...], dict[str, object]]:
        # Default ``Exception.__reduce__`` returns ``(cls, self.args)``,
        # losing every field set on the instance after
        # ``Exception.__init__`` — most notably ``raw_message`` and
        # ``code`` carried by :class:`InterfaceError` /
        # :class:`_DatabaseErrorWithCode`. SA's ``is_disconnect``
        # reads ``raw_message`` first; without preserving it,
        # cross-process error capture (Celery, multiprocessing pool,
        # SA's multiprocess test harness) silently dropped the
        # un-truncated server text and forced the substring branch
        # of the disconnect classifier to fall back to ``str(cause)``.
        #
        # Mirrors the discipline applied at the client layer in
        # ``dqliteclient.exceptions.DqliteError``.
        return (self.__class__, self.args, self.__getstate__())

    def __getstate__(self) -> dict[str, object]:
        return self.__dict__.copy()

    def __setstate__(self, state: dict[str, object] | None) -> None:
        if state:
            self.__dict__.update(state)


class InterfaceError(Error):
    """Error related to the database interface.

    Optionally carries the SQLite or dqlite extended error ``code``
    and the full server text on ``raw_message``. Most ``InterfaceError``
    instances are misuse diagnostics raised inside the driver itself
    (no code), but server-emitted ``DQLITE_PROTO`` (1001 — protocol
    misuse) routes to ``InterfaceError`` per PEP 249 §6 and carries
    the wire-level code so callers / SA's ``is_disconnect`` /
    ``raw_message``-consuming log tooling can branch on it without
    walking ``__cause__``. Symmetric with :class:`DatabaseError`'s
    code-bearing accessor; callers should use
    ``getattr(exc, "code", None)`` to test rather than ``isinstance``.
    """

    code: int | None
    raw_message: str

    def __init__(
        self,
        message: object = "",
        code: int | None = None,
        *,
        raw_message: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        resolved = str(message) if raw_message is None else raw_message
        self.raw_message = _cap_raw_message(resolved)

    @property
    def sqlite_errorcode(self) -> int | None:
        """Stdlib ``sqlite3``-parity alias for :attr:`code` (since
        Python 3.11). Returns the same value as :attr:`code`."""
        return self.code

    @property
    def sqlite_errorname(self) -> str | None:
        """Stdlib ``sqlite3``-parity alias (Python 3.11+) for the
        symbolic name of :attr:`code` (e.g. ``"SQLITE_CONSTRAINT_UNIQUE"``).

        Looked up via stdlib ``sqlite3``'s ``SQLITE_*`` constant set so
        cross-driver code that branches on
        ``e.sqlite_errorname == "SQLITE_BUSY"`` continues to work
        against dqlite. Returns ``None`` if :attr:`code` is ``None`` or
        if the code is not present in stdlib's constant table — the
        latter covers dqlite-namespace codes (≥1000) which have no
        upstream symbolic name.
        """
        return _sqlite_errorname(self.code)

    def __repr__(self) -> str:
        msg = self.args[0] if self.args else ""
        if self.code is None:
            return f"{type(self).__name__}({msg!r})"
        return f"{type(self).__name__}({msg!r}, code={self.code})"


class DatabaseError(Error):
    """Error related to the database.

    Optionally carries the SQLite extended error ``code`` and the
    full server text on ``raw_message``. Most callers see the
    code-bearing subclasses (OperationalError, IntegrityError,
    InternalError, DataError, ProgrammingError) instead, but a few
    SQLite primary codes (e.g. CORRUPT, NOTADB, FORMAT) route
    directly to DatabaseError per PEP 249's ``"errors related to
    the database"`` umbrella, and those still surface a code so
    callers can branch on it without walking ``__cause__``.
    """

    code: int | None
    raw_message: str

    def __init__(
        self,
        message: object = "",
        code: int | None = None,
        *,
        raw_message: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        resolved = str(message) if raw_message is None else raw_message
        self.raw_message = _cap_raw_message(resolved)

    @property
    def sqlite_errorcode(self) -> int | None:
        """Stdlib ``sqlite3``-parity alias for :attr:`code`.

        Python 3.11 added ``sqlite3.Error.sqlite_errorcode`` exposing
        the SQLite extended error code. Cross-driver code that branches
        on ``e.sqlite_errorcode == sqlite3.SQLITE_BUSY`` continues to
        work against dqlite without the caller importing dqlite-specific
        symbols. Returns the same value as :attr:`code`.
        """
        return self.code

    @property
    def sqlite_errorname(self) -> str | None:
        """Stdlib ``sqlite3``-parity alias (Python 3.11+); see
        :class:`InterfaceError.sqlite_errorname`."""
        return _sqlite_errorname(self.code)

    def __repr__(self) -> str:
        msg = self.args[0] if self.args else ""
        if self.code is None:
            return f"{type(self).__name__}({msg!r})"
        return f"{type(self).__name__}({msg!r}, code={self.code})"


class _DatabaseErrorWithCode(DatabaseError):
    """Internal marker base for the five PEP 249 ``DatabaseError``
    subclasses :class:`OperationalError`, :class:`IntegrityError`,
    :class:`InternalError`, :class:`ProgrammingError`, and
    :class:`DataError`.

    Private by design — not part of the public PEP 249 hierarchy, not
    re-exported via ``__all__``. The ``__init__`` and ``__repr__`` live
    on :class:`DatabaseError` itself, so any ``DatabaseError`` instance
    can carry ``code`` and ``raw_message`` (some primary SQLite codes —
    CORRUPT, NOTADB, FORMAT — route directly to bare ``DatabaseError``
    per :data:`~dqlitedbapi.cursor._CODE_TO_EXCEPTION`).

    **Do not branch on ``isinstance(exc, _DatabaseErrorWithCode)`` to
    detect code-bearing exceptions.** That predicate is incomplete:
    bare ``DatabaseError`` instances raised for CORRUPT/NOTADB/FORMAT
    also carry a code but are not marker subclasses. Use
    ``getattr(exc, "code", None) is not None`` instead.

    The marker is preserved as a grouping signal for the five canonical
    code-bearing PEP 249 subclasses (pinned by
    ``tests/test_exception_coded_mixin.py``); a future refactor that
    silently promoted/demoted a class out of this group would break
    that contract.
    """

    pass


class OperationalError(_DatabaseErrorWithCode):
    """Error related to database operation.

    Optional ``code`` attribute carries the SQLite extended error code
    forwarded from the dqlite server (e.g. ``SQLITE_IOERR_NOT_LEADER``).
    Callers can inspect ``getattr(exc, "code", None)`` to branch on
    specific wire-level failures without importing the lower-level
    client exception module.
    """

    pass


class IntegrityError(_DatabaseErrorWithCode):
    """Error related to database integrity.

    Raised when the relational integrity of the database is affected,
    e.g. a UNIQUE, NOT NULL, FOREIGN KEY, or CHECK constraint violation.
    The SQLite primary error code is 19 (SQLITE_CONSTRAINT) plus a
    family of extended codes that all share ``code & 0xFF == 19``.

    Optional ``code`` attribute carries the SQLite extended error code
    mirror of :class:`OperationalError`.
    """

    pass


class InternalError(_DatabaseErrorWithCode):
    """Internal database error.

    Raised for the SQLite ``SQLITE_INTERNAL`` primary error code (2) and
    its extended family — the same classification stdlib ``sqlite3``
    applies. Optional ``code`` attribute mirrors :class:`OperationalError`
    so callers that branch on the SQLite extended code can do so without
    reaching into the client layer.
    """

    pass


class ProgrammingError(_DatabaseErrorWithCode):
    """Programming error (e.g., table not found, SQL syntax error).

    Optional ``code`` attribute carries the SQLite extended error code
    when the error originates from a server-reported failure (e.g.
    ``SQLITE_RANGE`` = 25, bind-index out of range). Mirror of
    :class:`OperationalError`.
    """

    pass


class NotSupportedError(DatabaseError):
    """Method or database API not supported by database."""

    pass


class DataError(_DatabaseErrorWithCode):
    """Error due to problems with the processed data.

    Optional ``code`` attribute carries the SQLite extended error code
    for server-reported data-category failures (e.g.
    ``SQLITE_MISMATCH``, ``SQLITE_TOOBIG``). Mirror of
    :class:`OperationalError` so callers that branch on the extended
    code can do so without reaching into the client layer.
    """

    pass
