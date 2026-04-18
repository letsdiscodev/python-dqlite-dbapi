"""PEP 249 type objects and constructors for dqlite."""

import datetime
from typing import Any


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


# Internal conversion helpers.
#
# The wire codec deals only in primitives (ISO8601 → str, UNIXTIME → int64).
# PEP 249 specifies that drivers SHOULD return datetime objects for date/time
# columns — and every major Python driver (psycopg, mysqlclient, asyncpg, ...)
# does. These helpers implement that conversion at the driver (DBAPI) layer,
# matching Go's database/sql driver split.


def _iso8601_from_datetime(value: datetime.datetime | datetime.date) -> str:
    """Format a datetime/date as an ISO 8601 string for wire transmission.

    Uses the space-separated layout so values are byte-for-byte comparable
    with what Go and the C client produce. Accepts both naive and
    timezone-aware datetimes — naive values round-trip as naive (matching
    pysqlite semantics), aware values preserve the offset.
    """
    if isinstance(value, datetime.datetime):
        base = f"{value.year:04d}" + value.strftime("-%m-%d %H:%M:%S")
        if value.microsecond:
            base += f".{value.microsecond:06d}"
        if value.tzinfo is None:
            return base
        offset = value.utcoffset()
        assert offset is not None
        total_seconds = int(offset.total_seconds())
        sign = "+" if total_seconds >= 0 else "-"
        hours, remainder = divmod(abs(total_seconds), 3600)
        minutes = remainder // 60
        return base + f"{sign}{hours:02d}:{minutes:02d}"
    # datetime.date (must come after datetime check — datetime is a subclass).
    return value.isoformat()


def _datetime_from_iso8601(text: str) -> datetime.datetime | None:
    """Parse an ISO 8601 string into ``datetime.datetime``.

    Returns ``None`` for the empty string — pre-null-patch dqlite servers
    sometimes emit empty text for NULL datetime cells, and the modern
    server still tolerates empty ISO8601 values. Returning None matches
    PEP 249 NULL semantics.

    Naive input round-trips as naive; aware input preserves the offset.
    """
    if not text:
        return None
    s = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        pass
    try:
        d = datetime.date.fromisoformat(s)
    except ValueError as exc:
        raise ValueError(f"Cannot parse ISO 8601 datetime: {text!r}") from exc
    return datetime.datetime(d.year, d.month, d.day)


def _datetime_from_unixtime(value: int) -> datetime.datetime:
    """Decode a UNIXTIME int64 into a UTC-aware ``datetime.datetime``.

    UNIXTIME is unambiguously seconds-since-epoch in UTC, so returning a
    UTC-aware value is faithful. Callers that want local time can convert.
    """
    return datetime.datetime.fromtimestamp(value, tz=datetime.UTC)


def _convert_bind_param(value: Any) -> Any:
    """Map driver-level Python types to wire primitives.

    The wire codec accepts only bool/int/float/str/bytes/None; datetime and
    date are driver-level conveniences that we stringify to ISO 8601 before
    handing off. Everything else passes through unchanged.
    """
    if isinstance(value, datetime.datetime | datetime.date):
        return _iso8601_from_datetime(value)
    return value
