"""PEP 249 exception hierarchy for dqlite."""

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
    """Error related to the database interface."""

    pass


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
