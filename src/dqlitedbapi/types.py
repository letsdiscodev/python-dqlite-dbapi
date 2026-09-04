"""PEP 249 type objects, constructors, adapters and datetime codecs."""

import datetime
import math
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Final, TypeGuard, final

from dqlitedbapi.exceptions import AdapterLookupError, DataError, ProgrammingError
from dqlitewire import ValueType

__all__ = [
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "UNKNOWN",
    "Binary",
    "Date",
    "DateFromTicks",
    "DescriptionTuple",
    "PrepareProtocol",
    "RowFactory",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "format_utc_offset",
    "register_adapter",
    "unregister_adapter",
]

# ``cursor.description`` entry: only name and type_code are populated.
type DescriptionTuple = tuple[str, "int | DBAPIType", None, None, None, None, None]
type Description = tuple[DescriptionTuple, ...] | None

# stdlib ``sqlite3``-style factory, called as ``factory(cursor, row_tuple)``.
type RowFactory = Callable[..., Any]


def is_int_not_bool(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)


# Constructors are functions so out-of-range input raises DataError, not ValueError.


def Date(year: int, month: int, day: int) -> datetime.date:
    try:
        return datetime.date(year, month, day)
    except (TypeError, ValueError) as e:
        raise DataError(f"Date({year!r}, {month!r}, {day!r}) invalid: {e}") from e


def Time(
    hour: int,
    minute: int,
    second: int,
    microsecond: int = 0,
    tzinfo: datetime.tzinfo | None = None,
) -> datetime.time:
    try:
        return datetime.time(hour, minute, second, microsecond, tzinfo=tzinfo)
    except (TypeError, ValueError) as e:
        raise DataError(
            f"Time({hour!r}, {minute!r}, {second!r}, {microsecond!r}) invalid: {e}"
        ) from e


def Timestamp(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
    microsecond: int = 0,
    tzinfo: datetime.tzinfo | None = None,
) -> datetime.datetime:
    try:
        return datetime.datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo)
    except (TypeError, ValueError) as e:
        raise DataError(
            f"Timestamp({year!r}, {month!r}, {day!r}, {hour!r}, {minute!r}, {second!r}) "
            f"invalid: {e}"
        ) from e


def _validate_ticks(ticks: float) -> float:
    if isinstance(ticks, bool | str) or not isinstance(ticks, int | float | Decimal):
        raise DataError(f"Invalid timestamp ticks: {ticks!r} ({type(ticks).__name__})")
    try:
        coerced = float(ticks)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataError(f"Invalid timestamp ticks: {ticks!r} ({exc})") from exc
    if not math.isfinite(coerced):
        raise DataError(f"Invalid timestamp ticks: {coerced}")
    return coerced


def DateFromTicks(ticks: float) -> datetime.date:
    coerced = _validate_ticks(ticks)
    try:
        return datetime.date.fromtimestamp(coerced)
    except (OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid timestamp ticks {ticks}: {e}") from e


def TimeFromTicks(ticks: float) -> datetime.time:
    coerced = _validate_ticks(ticks)
    try:
        return datetime.datetime.fromtimestamp(coerced).time()
    except (OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid timestamp ticks {ticks}: {e}") from e


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    coerced = _validate_ticks(ticks)
    try:
        return datetime.datetime.fromtimestamp(coerced)
    except (OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid timestamp ticks {ticks}: {e}") from e


Binary = memoryview


@final
class DBAPIType:
    """Type object comparing equal to its SQL type names (str) and wire ``ValueType`` codes.

    Hashes by name, so ``code in {NUMBER}`` is False even when ``NUMBER == code``;
    compare with ``==``.
    """

    def __init__(self, *values: str | int | ValueType, name: str) -> None:
        self.values: frozenset[str | int] = frozenset(
            int(v) if isinstance(v, ValueType) else v for v in values
        )
        self.name = name

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DBAPIType):
            return self.values == other.values
        if isinstance(other, str):
            return other.upper() in self.values
        if isinstance(other, ValueType):
            return int(other) in self.values
        if is_int_not_bool(other):
            return other in self.values
        return NotImplemented

    def __hash__(self) -> int:
        return hash(("DBAPIType", self.name))

    def __repr__(self) -> str:
        return self.name


STRING: Final[DBAPIType] = DBAPIType(
    "TEXT", "VARCHAR", "CHAR", "CLOB", ValueType.TEXT, name="STRING"
)
BINARY: Final[DBAPIType] = DBAPIType("BLOB", "BINARY", "VARBINARY", ValueType.BLOB, name="BINARY")
NUMBER: Final[DBAPIType] = DBAPIType(
    "INTEGER",
    "INT",
    "SMALLINT",
    "BIGINT",
    "REAL",
    "FLOAT",
    "DOUBLE",
    "NUMERIC",
    ValueType.INTEGER,
    ValueType.FLOAT,
    ValueType.BOOLEAN,
    name="NUMBER",
)
DATETIME: Final[DBAPIType] = DBAPIType(
    "DATE",
    "TIME",
    "TIMESTAMP",
    "DATETIME",
    ValueType.ISO8601,
    ValueType.UNIXTIME,
    name="DATETIME",
)
ROWID: Final[DBAPIType] = DBAPIType("ROWID", "INTEGER PRIMARY KEY", ValueType.INTEGER, name="ROWID")
# Type code of a column whose type the wire could not tell (empty or all-NULL result).
UNKNOWN: Final[DBAPIType] = DBAPIType(name="UNKNOWN")


_MAX_ERROR_TEXT: Final[int] = 200


def _truncate(text: str) -> str:
    if len(text) <= _MAX_ERROR_TEXT:
        return text
    return f"{text[:_MAX_ERROR_TEXT]}... [truncated, {len(text) - _MAX_ERROR_TEXT} chars]"


def format_utc_offset(offset: datetime.timedelta) -> str:
    """Format a UTC offset as ``±HH:MM`` (``±HH:MM:SS`` for sub-minute offsets)."""
    if not isinstance(offset, datetime.timedelta):
        raise DataError(
            f"tzinfo.utcoffset() returned {type(offset).__name__}; expected timedelta or None"
        )
    total_us = round(offset.total_seconds() * 1_000_000)
    if abs(total_us) >= 24 * 3600 * 1_000_000:
        raise DataError(f"tzinfo offset out of range: {offset!r} (|offset| must be < 24h)")
    if total_us % 1_000_000:
        raise DataError(f"tzinfo offset has sub-second precision: {offset!r}")
    total_seconds = total_us // 1_000_000
    sign = "+" if total_seconds >= 0 else "-"
    hours, rem = divmod(abs(total_seconds), 3600)
    minutes, seconds = divmod(rem, 60)
    if seconds:
        return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{sign}{hours:02d}:{minutes:02d}"


def _utc_offset(value: datetime.datetime | datetime.time) -> datetime.timedelta | None:
    if value.tzinfo is None:
        return None
    try:
        offset = value.utcoffset()
    except Exception as exc:
        raise DataError(
            f"tzinfo.utcoffset() raised {type(exc).__name__} for {value!r}: {exc}"
        ) from exc
    if offset is None:
        raise DataError(f"{value!r} is tz-aware but tzinfo.utcoffset() returned None")
    return offset


def iso8601_from_datetime(value: datetime.datetime | datetime.date) -> str:
    """Space-separated ISO 8601, byte-compatible with the Go and C clients."""
    if not isinstance(value, datetime.datetime):
        return value.isoformat()
    text = f"{value.year:04d}" + value.strftime("-%m-%d %H:%M:%S")
    if value.microsecond:
        text += f".{value.microsecond:06d}"
    offset = _utc_offset(value)
    return text if offset is None else text + format_utc_offset(offset)


def iso8601_from_time(value: datetime.time) -> str:
    text = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
    if value.microsecond:
        text += f".{value.microsecond:06d}"
    offset = _utc_offset(value)
    return text if offset is None else text + format_utc_offset(offset)


def datetime_from_iso8601(text: str) -> datetime.datetime | datetime.time | None:
    """Decode an ISO8601 cell; ``""`` (a NULL from pre-null-patch servers) decodes to ``None``."""
    if not text:
        return None
    try:
        return datetime.datetime.fromisoformat(text)
    except ValueError:
        pass
    try:
        return datetime.time.fromisoformat(text)
    except ValueError as exc:
        raise DataError(f"Cannot parse ISO 8601 datetime from server: {_truncate(text)!r}") from exc


# datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC).timestamp(), the largest value
# ``fromtimestamp`` accepts on every platform.
_MAX_UNIXTIME_SECONDS: Final[int] = 253402300799


def datetime_from_unixtime(value: int) -> datetime.datetime:
    """Decode a UNIXTIME cell to a UTC-aware datetime; negative values are rejected
    because ``fromtimestamp`` is platform-inconsistent on them."""
    if not is_int_not_bool(value):
        raise DataError(f"UNIXTIME value {value!r} must be int, got {type(value).__name__}")
    if not 0 <= value <= _MAX_UNIXTIME_SECONDS:
        raise DataError(
            f"UNIXTIME value {value!r} out of representable range (0..{_MAX_UNIXTIME_SECONDS})"
        )
    try:
        return datetime.datetime.fromtimestamp(value, tz=datetime.UTC)
    except (TypeError, OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid UNIXTIME from server: {value!r}") from e


_ADAPTERS: dict[type, Callable[[Any], Any]] = {}

_WIRE_PRIMITIVES: Final[tuple[type, ...]] = (
    int,
    float,
    str,
    bytes,
    bytearray,
    memoryview,
    bool,
    type(None),
)


class PrepareProtocol:
    """Passed to a value's ``__conform__`` hook, as in stdlib ``sqlite3``."""


def register_adapter(type_: type, adapter: Callable[[Any], Any], /) -> None:
    """Register ``adapter(value)`` to run on bind parameters of exactly ``type_``.

    The registry is process-global and shared by the sync and async surfaces.
    """
    if not callable(adapter):
        raise ProgrammingError(f"adapter must be callable, got {type(adapter).__name__}")
    if not isinstance(type_, type):
        raise ProgrammingError(f"type_ must be a class, got {type(type_).__name__}")
    _ADAPTERS[type_] = adapter


def unregister_adapter(type_: type, /) -> None:
    if _ADAPTERS.pop(type_, None) is None:
        raise AdapterLookupError(f"no adapter registered for {type_.__name__}")


def adapt_bind_param(value: Any) -> Any:
    """Convert one bind parameter to a wire primitive.

    Order: registered adapter (exact type), else ``__conform__``; then
    datetime/date/time become ISO 8601 text. A raising ``__conform__``
    propagates unwrapped (stdlib parity); other adapter failures are
    ``DataError``.
    """
    original_type = type(value)
    adapted = False
    adapter = _ADAPTERS.get(original_type)
    if adapter is not None:
        try:
            value = adapter(value)
        except Exception as exc:
            raise DataError(f"adapter for {original_type.__name__} failed: {exc}") from exc
        adapted = True
    else:
        conform = getattr(value, "__conform__", None)
        if conform is not None:
            conformed = conform(PrepareProtocol)
            if conformed is not None:
                value = conformed
                adapted = True
    if isinstance(value, datetime.datetime | datetime.date):
        return iso8601_from_datetime(value)
    if isinstance(value, datetime.time):
        return iso8601_from_time(value)
    if not isinstance(value, _WIRE_PRIMITIVES):
        what = f"adapter for {original_type.__name__} produced" if adapted else "type"
        raise (DataError if adapted else ProgrammingError)(
            f"{what} {type(value).__name__} is not supported; the wire accepts only "
            "int / float / str / bytes / bytearray / memoryview / bool / None. "
            "Register an adapter that returns one of those."
        )
    if isinstance(value, str) and "\x00" in value:
        raise DataError(
            f"TEXT bind with embedded NUL at offset {value.index(chr(0))} rejected; "
            "dqlite TEXT is NUL-terminated. Use a BLOB column (bind bytes) instead."
        )
    return value
