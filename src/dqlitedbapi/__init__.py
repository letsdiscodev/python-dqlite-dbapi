"""PEP 249 compliant interface for dqlite."""

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

# PEP 249 module-level attributes
apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "qmark"  # Question mark style: WHERE name=?

# SQLite compatibility attributes (for SQLAlchemy)
# dqlite uses SQLite 3.x internally
sqlite_version_info = (3, 35, 0)
sqlite_version = "3.35.0"

__all__ = [
    # Module attributes
    "apilevel",
    "threadsafety",
    "paramstyle",
    # Functions
    "connect",
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

__version__ = "0.1.3"


def connect(
    address: str,
    *,
    database: str = "default",
    timeout: float = 10.0,
) -> Connection:
    """Connect to a dqlite database.

    Args:
        address: Node address in "host:port" format
        database: Database name to open
        timeout: Connection timeout in seconds

    Returns:
        A Connection object
    """
    return Connection(address, database=database, timeout=timeout)
