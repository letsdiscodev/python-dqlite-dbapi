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
    """Error related to database operation."""

    pass


class IntegrityError(DatabaseError):
    """Error related to database integrity."""

    pass


class InternalError(DatabaseError):
    """Internal database error."""

    pass


class ProgrammingError(DatabaseError):
    """Programming error (e.g., table not found, SQL syntax error)."""

    pass


class NotSupportedError(DatabaseError):
    """Method or database API not supported by database."""

    pass
