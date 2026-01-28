"""PEP 249 type objects and constructors for dqlite."""

import datetime


# Type constructors
def Date(year: int, month: int, day: int) -> datetime.date:  # noqa: N802
    """Construct a date value."""
    return datetime.date(year, month, day)


def Time(hour: int, minute: int, second: int) -> datetime.time:  # noqa: N802
    """Construct a time value."""
    return datetime.time(hour, minute, second)


def Timestamp(  # noqa: N802
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> datetime.datetime:
    """Construct a timestamp value."""
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks: float) -> datetime.date:  # noqa: N802
    """Construct a date from a Unix timestamp."""
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks: float) -> datetime.time:  # noqa: N802
    """Construct a time from a Unix timestamp."""
    return datetime.datetime.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks: float) -> datetime.datetime:  # noqa: N802
    """Construct a timestamp from a Unix timestamp."""
    return datetime.datetime.fromtimestamp(ticks)


def Binary(data: bytes) -> bytes:  # noqa: N802
    """Construct a binary value."""
    return bytes(data)


# Type objects for column type checking
class _DBAPIType:
    """Base type for DB-API type objects."""

    def __init__(self, *values: str) -> None:
        self.values = set(values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return other.upper() in self.values
        return NotImplemented

    def __hash__(self) -> int:
        return hash(frozenset(self.values))


STRING = _DBAPIType("TEXT", "VARCHAR", "CHAR", "CLOB")
BINARY = _DBAPIType("BLOB", "BINARY", "VARBINARY")
NUMBER = _DBAPIType("INTEGER", "INT", "SMALLINT", "BIGINT", "REAL", "FLOAT", "DOUBLE", "NUMERIC")
DATETIME = _DBAPIType("DATE", "TIME", "TIMESTAMP", "DATETIME")
ROWID = _DBAPIType("ROWID", "INTEGER PRIMARY KEY")
