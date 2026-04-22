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


class Warning(Exception):  # noqa: A001
    """Exception raised for important warnings."""

    pass


class Error(Exception):
    """Base class for all database errors."""

    pass


class InterfaceError(Error):
    """Error related to the database interface."""

    pass


class DatabaseError(Error):
    """Error related to the database."""

    pass


class DataError(DatabaseError):
    """Error due to problems with the processed data."""

    pass


class _DatabaseErrorWithCode(DatabaseError):
    """Internal base for DatabaseError subclasses that carry a SQLite
    extended error ``code`` attribute.

    Private by design — not part of the public PEP 249 hierarchy, not
    re-exported via ``__all__``. Present purely to eliminate three
    byte-identical copies of ``__init__`` and ``__repr__`` across
    :class:`OperationalError`, :class:`IntegrityError`, and
    :class:`InternalError`.

    ``__repr__`` reads ``type(self).__name__``, so each concrete
    subclass surfaces its own name — Sentry/Rollbar and
    ``logger.error("%r", exc)`` show the right class and the
    ``code=…`` attribute (which the default ``Exception.__repr__``
    would drop because it is not in ``args``).
    """

    code: int | None

    def __init__(self, message: object = "", code: int | None = None) -> None:
        super().__init__(message)
        self.code = code

    def __repr__(self) -> str:
        msg = self.args[0] if self.args else ""
        if self.code is None:
            return f"{type(self).__name__}({msg!r})"
        return f"{type(self).__name__}({msg!r}, code={self.code})"


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


class ProgrammingError(DatabaseError):
    """Programming error (e.g., table not found, SQL syntax error)."""

    pass


class NotSupportedError(DatabaseError):
    """Method or database API not supported by database."""

    pass
