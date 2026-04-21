"""PEP 249 exception hierarchy for dqlite."""


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


class OperationalError(DatabaseError):
    """Error related to database operation.

    Optional ``code`` attribute carries the SQLite extended error code
    forwarded from the dqlite server (e.g. ``SQLITE_IOERR_NOT_LEADER``).
    Callers can inspect ``getattr(exc, "code", None)`` to branch on
    specific wire-level failures without importing the lower-level
    client exception module.
    """

    def __init__(self, message: object = "", code: int | None = None) -> None:
        super().__init__(message)
        self.code = code

    def __repr__(self) -> str:
        # Sentry/Rollbar and standard logger ``%r`` formatting surface
        # only the default repr, which drops ``code`` because it is not
        # in ``args``. Override so the SQLite extended error code is
        # visible in logs without reaching into ``.code`` manually.
        msg = self.args[0] if self.args else ""
        if self.code is None:
            return f"{type(self).__name__}({msg!r})"
        return f"{type(self).__name__}({msg!r}, code={self.code})"


class IntegrityError(DatabaseError):
    """Error related to database integrity.

    Raised when the relational integrity of the database is affected,
    e.g. a UNIQUE, NOT NULL, FOREIGN KEY, or CHECK constraint violation.
    The SQLite primary error code is 19 (SQLITE_CONSTRAINT) plus a
    family of extended codes that all share ``code & 0xFF == 19``.

    Optional ``code`` attribute carries the SQLite extended error code
    mirror of :class:`OperationalError`.
    """

    def __init__(self, message: object = "", code: int | None = None) -> None:
        super().__init__(message)
        self.code = code

    def __repr__(self) -> str:
        msg = self.args[0] if self.args else ""
        if self.code is None:
            return f"{type(self).__name__}({msg!r})"
        return f"{type(self).__name__}({msg!r}, code={self.code})"


class InternalError(DatabaseError):
    """Internal database error.

    Raised for the SQLite ``SQLITE_INTERNAL`` primary error code (2) and
    its extended family — the same classification stdlib ``sqlite3``
    applies. Optional ``code`` attribute mirrors :class:`OperationalError`
    so callers that branch on the SQLite extended code can do so without
    reaching into the client layer.
    """

    def __init__(self, message: object = "", code: int | None = None) -> None:
        super().__init__(message)
        self.code = code

    def __repr__(self) -> str:
        msg = self.args[0] if self.args else ""
        if self.code is None:
            return f"{type(self).__name__}({msg!r})"
        return f"{type(self).__name__}({msg!r}, code={self.code})"


class ProgrammingError(DatabaseError):
    """Programming error (e.g., table not found, SQL syntax error)."""

    pass


class NotSupportedError(DatabaseError):
    """Method or database API not supported by database."""

    pass
