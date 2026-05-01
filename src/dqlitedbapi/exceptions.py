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


@lru_cache(maxsize=1)
def _stdlib_code_to_name() -> dict[int, str]:
    """Cache stdlib's ``SQLITE_*`` constant set as a code-to-name
    map. Built lazily on first access so import overhead is paid
    only when ``sqlite_errorname`` is actually queried.

    Skips non-error constants (authorizer / opcode names that share
    the prefix) by including only ``SQLITE_*`` integers that fit in
    the 16-bit primary or extended-code range. The stdlib constants
    are a stable surface — Python 3.11+ guarantees the names match
    the upstream SQLite header.
    """
    table: dict[int, str] = {}
    for name in dir(_stdlib_sqlite3):
        if not name.startswith("SQLITE_"):
            continue
        value = getattr(_stdlib_sqlite3, name)
        if not isinstance(value, int):
            continue
        # First-wins for collisions — stdlib uses the canonical
        # error symbol on the lower numeric value.
        table.setdefault(value, name)
    return table


def _sqlite_errorname(code: int | None) -> str | None:
    """Look up the symbolic SQLite error name for ``code``. Returns
    ``None`` for ``None`` codes and for codes not present in stdlib's
    constant table (e.g. dqlite-namespace codes ≥1000)."""
    if code is None:
        return None
    return _stdlib_code_to_name().get(code)


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


class Error(Exception):
    """Base class for all database errors."""

    pass


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
        self.raw_message = str(message) if raw_message is None else raw_message

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
        self.raw_message = str(message) if raw_message is None else raw_message

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
