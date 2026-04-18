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
# PEP 249 value 1: threads may share the module.
#
# This driver is stricter than the PEP minimum: each Connection is
# bound to the thread that created it. Any method call from a
# different thread raises ProgrammingError. Use one Connection per
# thread, or use the async API (dqlitedbapi.aio.aconnect) for a
# single-thread-per-loop model.
threadsafety = 1
paramstyle = "qmark"  # Question mark style: WHERE name=?

# SQLite compatibility attributes (for SQLAlchemy)
# dqlite uses SQLite 3.x internally
sqlite_version_info = (3, 35, 0)
sqlite_version = "3.35.0"

__all__ = [
    # Module attributes
    "__version__",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "sqlite_version",
    "sqlite_version_info",
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
        timeout: Connection timeout in seconds — must be a positive
            finite number. ``0``, negatives, and non-finite values are
            rejected here rather than silently passed through to the
            underlying connection.

    Returns:
        A Connection object
    """
    import math

    if not math.isfinite(timeout) or timeout <= 0:
        raise ProgrammingError(f"timeout must be a positive finite number, got {timeout}")
    return Connection(address, database=database, timeout=timeout)
