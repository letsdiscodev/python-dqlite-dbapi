"""PEP 249 type objects and constructors for dqlite."""

import datetime
import math
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Final, final

from dqlitedbapi.exceptions import AdapterLookupError, DataError, ProgrammingError
from dqlitewire import ValueType

# ``DescriptionTuple`` is exported so typed wrappers don't import an
# underscore-prefixed alias across a package boundary.
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
    "register_adapter",
    "unregister_adapter",
]

# ``cursor.description`` 7-tuple (PEP 249 §6.1.2); dqlite populates only
# name + type_code (wire ``ValueType`` int, or the ``UNKNOWN`` sentinel
# when the type can't be resolved), the rest are always None.
type DescriptionTuple = tuple[str, int | _DBAPIType | None, None, None, None, None, None]
type _Description = tuple[DescriptionTuple, ...] | None

# stdlib ``sqlite3``-style row factory, invoked as ``factory(cursor,
# row_tuple)``. ``Callable[..., Any]`` (not the precise shape) avoids
# forward-reference circularity with the cursor/connection types.
type RowFactory = Callable[..., Any]


# Type constructors are functions (not class aliases as in stdlib
# sqlite3) so out-of-range inputs raise PEP 249 ``DataError`` instead of
# bare ``ValueError`` / ``TypeError``. Consequence: ``isinstance(v,
# Date)`` raises; cross-driver code must use ``isinstance(v,
# datetime.date)`` etc.


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
    """Construct a time value.

    ``microsecond`` / ``tzinfo`` are accepted (beyond PEP 249) so mixing
    with ``datetime.time`` doesn't silently drop sub-second precision.
    """
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
    """Construct a timestamp value."""
    try:
        return datetime.datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo)
    except (TypeError, ValueError) as e:
        raise DataError(
            f"Timestamp({year!r}, {month!r}, {day!r}, {hour!r}, "
            f"{minute!r}, {second!r}) invalid: {e}"
        ) from e


def _validate_ticks(ticks: float) -> float:
    """Normalize ``ticks`` to a finite float or raise ``DataError``.

    Returns float because ``datetime.fromtimestamp`` rejects ``Decimal``.
    """
    # bool / str silently coerce to a valid timestamp (epoch, float("1.5"))
    # -- reject explicitly so a buggy unconverted caller value isn't masked.
    if isinstance(ticks, bool):
        raise DataError(f"Invalid timestamp ticks: {ticks!r} (bool)")
    if isinstance(ticks, str):
        raise DataError(f"Invalid timestamp ticks: {ticks!r} (str)")
    # numpy.bool_ slips past the bool guard (not a Python bool subclass) and
    # coerces to 1.0; reject anything outside the real numeric types.
    if not isinstance(ticks, (int, float, Decimal)):
        raise DataError(f"Invalid timestamp ticks: {ticks!r} ({type(ticks).__name__})")
    # Catch OverflowError: a future CPython may raise (not saturate) on huge
    # Decimals, and custom __float__ may raise it directly.
    try:
        coerced = float(ticks)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataError(f"Invalid timestamp ticks: {ticks!r} ({exc})") from exc
    if not math.isfinite(coerced):
        raise DataError(f"Invalid timestamp ticks: {coerced}")
    return coerced


def DateFromTicks(ticks: float) -> datetime.date:
    """Construct a date from a Unix timestamp (naive, host-local tz).

    The UNIXTIME decoder returns UTC-aware datetimes, so round-tripping
    this value through a UNIXTIME column shifts by the host's UTC offset;
    use ISO8601 (TEXT) columns for faithful round-trip of naive values.
    """
    coerced = _validate_ticks(ticks)
    try:
        return datetime.date.fromtimestamp(coerced)
    except (OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid timestamp ticks {ticks}: {e}") from e


def TimeFromTicks(ticks: float) -> datetime.time:
    """Construct a time from a Unix timestamp (naive, host-local tz).

    Unlike stdlib, preserves the microsecond component of fractional
    ``ticks``. See ``DateFromTicks`` for the UNIXTIME tz asymmetry.
    """
    coerced = _validate_ticks(ticks)
    try:
        return datetime.datetime.fromtimestamp(coerced).time()
    except (OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid timestamp ticks {ticks}: {e}") from e


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    """Construct a timestamp from a Unix timestamp (naive, host-local tz).

    Unlike stdlib, preserves the microsecond component of fractional
    ``ticks``. See ``DateFromTicks`` for the UNIXTIME tz asymmetry.
    """
    coerced = _validate_ticks(ticks)
    try:
        return datetime.datetime.fromtimestamp(coerced)
    except (OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid timestamp ticks {ticks}: {e}") from e


# Aliased directly to ``memoryview`` (matching stdlib sqlite3 3.13) so
# ``isinstance(Binary(b), memoryview)`` holds for cross-driver ports.
# Unlike the sibling constructors, bad input leaks bare ``TypeError``
# (not ``DataError``): wrapping in a function would break that isinstance.
Binary = memoryview


@final
class _DBAPIType:
    """DB-API type object: compares equal to matching uppercase SQL type
    names (str) and wire-level ``ValueType`` codes (int).

    Hashable (by name) for use as dict keys, but the hash-eq invariant is
    relaxed: multi-code objects hash by name while a bare wire int hashes
    to itself, so ``1 in {NUMBER}`` silently returns False even though
    ``NUMBER == 1`` is True. Introspect via chained ``==``, not ``in``.
    """

    def __init__(self, *values: str | int | ValueType, _name: str = "") -> None:
        normalized: set[str | int] = set()
        for v in values:
            if isinstance(v, ValueType):
                normalized.add(int(v))
            else:
                normalized.add(v)
        self.values = normalized
        self._name = _name

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _DBAPIType):
            return self.values == other.values
        if isinstance(other, str):
            return other.upper() in self.values
        if isinstance(other, ValueType):
            return int(other) in self.values
        if isinstance(other, int) and not isinstance(other, bool):
            return other in self.values
        return NotImplemented

    def __hash__(self) -> int:
        return hash(("_DBAPIType", self._name))

    def __repr__(self) -> str:
        return self._name or f"_DBAPIType({sorted(self.values, key=str)!r})"


STRING: Final[_DBAPIType] = _DBAPIType(
    "TEXT", "VARCHAR", "CHAR", "CLOB", ValueType.TEXT, _name="STRING"
)
# ``ValueType.ISO8601`` is deliberately NOT in STRING: the dbapi layer
# converts it to datetime before the user sees it, so it matches DATETIME
# (matching psycopg2/3), not STRING.
BINARY: Final[_DBAPIType] = _DBAPIType(
    "BLOB", "BINARY", "VARBINARY", ValueType.BLOB, _name="BINARY"
)
# NUMBER deliberately includes BOOLEAN: SQLite stores BOOL with numeric
# affinity (no dedicated BOOL type). Differs from psycopg2 (separate
# BOOLEAN object); branch on the wire ``ValueType.BOOLEAN`` int if needed.
NUMBER: Final[_DBAPIType] = _DBAPIType(
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
    _name="NUMBER",
)
DATETIME: Final[_DBAPIType] = _DBAPIType(
    "DATE",
    "TIME",
    "TIMESTAMP",
    "DATETIME",
    ValueType.ISO8601,
    ValueType.UNIXTIME,
    _name="DATETIME",
)
# ROWID deliberately overlaps NUMBER on ``ValueType.INTEGER``: the wire
# carries no rowid hint, so any INTEGER column matches both. Differs from
# psycopg2 (disjoint); match ROWID first if the distinction matters.
ROWID: Final[_DBAPIType] = _DBAPIType(
    "ROWID", "INTEGER PRIMARY KEY", ValueType.INTEGER, _name="ROWID"
)
# Sentinel for ``description[i][1]`` when the wire can't resolve a column
# type (empty result sets, NULL-only columns). Emitting None would violate
# PEP 249 §6.1.2 (must compare equal to a Type Object); UNKNOWN has an
# empty ``values`` so it compares False against all real Type Objects.
UNKNOWN: Final[_DBAPIType] = _DBAPIType(_name="UNKNOWN")


# Internal conversion helpers: the wire codec deals only in primitives,
# so these implement the PEP 249 date/time <-> datetime conversion at the
# driver layer.


# Cap on server-controlled text in exception messages: a 64 MiB hostile
# ISO8601 cell would otherwise inflate every DataError (and persist across
# pickle / logging via ``Error.__reduce__``).
_MAX_DATA_ERROR_TEXT_DISPLAY: Final[int] = 200


def _truncate_for_message(text: str) -> str:
    """Bound a server-controlled string before interpolating into a message."""
    if len(text) <= _MAX_DATA_ERROR_TEXT_DISPLAY:
        return text
    return (
        f"{text[:_MAX_DATA_ERROR_TEXT_DISPLAY]}"
        f"... [truncated, {len(text) - _MAX_DATA_ERROR_TEXT_DISPLAY} chars]"
    )


def _format_utc_offset(offset: datetime.timedelta) -> str:
    """Format a UTC offset as ``±HH:MM`` or ``±HH:MM:SS`` for sub-minute
    offsets (historical IANA LMT zones), which 3.11+ fromisoformat round-trips.

    Rejects (broken hand-rolled tzinfo only): non-timedelta offsets,
    |offset| >= 24h, and sub-second precision -- each would either escape
    ``dbapi.Error`` or emit a token peer decoders reject.
    """
    if not isinstance(offset, datetime.timedelta):
        raise DataError(
            f"tzinfo.utcoffset() returned non-timedelta "
            f"{type(offset).__name__}; CPython contract requires "
            "timedelta or None"
        )
    total_us = round(offset.total_seconds() * 1_000_000)
    if abs(total_us) >= 24 * 3600 * 1_000_000:
        raise DataError(f"tzinfo offset out of range: {offset!r} (|offset| must be < 24h)")
    if total_us % 1_000_000 != 0:
        raise DataError(
            f"tzinfo offset has sub-second precision: {offset!r} "
            "(dqlite wire encoding supports whole-second resolution only)"
        )
    total_seconds = total_us // 1_000_000
    sign = "+" if total_seconds >= 0 else "-"
    hours, rem = divmod(abs(total_seconds), 3600)
    minutes, seconds = divmod(rem, 60)
    if seconds:
        return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{sign}{hours:02d}:{minutes:02d}"


def _iso8601_from_datetime(value: datetime.datetime | datetime.date) -> str:
    """Format a datetime/date as an ISO 8601 string for wire transmission.

    Space-separated layout for byte-compatibility with Go / the C client.
    ``datetime.fold`` is NOT encoded (ISO 8601 has no notation), so DST
    "fall back" datetimes decode as fold=0 -- matching stdlib sqlite3.
    """
    if isinstance(value, datetime.datetime):
        base = f"{value.year:04d}" + value.strftime("-%m-%d %H:%M:%S")
        if value.microsecond:
            base += f".{value.microsecond:06d}"
        if value.tzinfo is None:
            return base
        # Wrap a raising utcoffset() as DataError (keeping tzinfo failures
        # in dbapi.Error); Exception not BaseException so cancellation isn't
        # swallowed. A None offset (declared-aware but unresolvable) is
        # rejected below rather than silently demoted to naive.
        try:
            offset = value.utcoffset()
        except Exception as exc:
            raise DataError(
                f"tzinfo.utcoffset() raised {type(exc).__name__} for {value!r}: {exc}"
            ) from exc
        if offset is None:
            raise DataError(
                f"datetime is tz-aware but tzinfo.utcoffset() returned None for "
                f"{value!r}; cannot encode without a resolvable UTC offset"
            )
        return base + _format_utc_offset(offset)
    # datetime.date (must come after the datetime check — datetime subclasses it).
    return value.isoformat()


def _iso8601_from_time(value: datetime.time) -> str:
    """Format a ``datetime.time`` as an ISO 8601 string."""
    base = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
    if value.microsecond:
        base += f".{value.microsecond:06d}"
    if value.tzinfo is None:
        return base
    # See ``_iso8601_from_datetime`` for the wrap rationale.
    try:
        offset = value.utcoffset()
    except Exception as exc:
        raise DataError(
            f"tzinfo.utcoffset() raised {type(exc).__name__} for {value!r}: {exc}"
        ) from exc
    if offset is None:
        raise DataError(
            f"time is tz-aware but tzinfo.utcoffset() returned None for "
            f"{value!r}; cannot encode without a resolvable UTC offset"
        )
    return base + _format_utc_offset(offset)


def _datetime_from_iso8601(text: str) -> datetime.datetime | datetime.time | None:
    """Parse an ISO 8601 string into ``datetime.datetime`` / ``.time``.

    Returns None for the empty string (pre-null-patch servers emit it for
    NULL datetime cells). Caveat: an empty-string projection on an
    ISO8601-tagged column also decodes to None, indistinguishable from
    NULL -- use ``CAST(... AS TEXT)`` to keep the distinction.

    Tries datetime.fromisoformat then time.fromisoformat (matching the
    ``_iso8601_from_time`` encoder); no date arm because 3.11+
    datetime.fromisoformat already covers bare ``YYYY-MM-DD``. A bare
    date therefore widens to a midnight datetime (matching pysqlite);
    time does not widen. Sub-microsecond fractional seconds are truncated
    (CPython drops digits past the sixth without rounding).

    Wraps a malformed-string ValueError as DataError (PEP 249 contract).
    """
    if not text:
        return None
    try:
        return datetime.datetime.fromisoformat(text)
    except ValueError:
        pass
    try:
        return datetime.time.fromisoformat(text)
    except ValueError as exc:
        # Genuine garbage. Truncate before interpolation: a 64 MiB hostile
        # cell would otherwise persist across pickle / logging / raw_message.
        raise DataError(
            f"Cannot parse ISO 8601 datetime from server: {_truncate_for_message(text)!r}"
        ) from exc


# Max UNIXTIME ``fromtimestamp(tz=UTC)`` accepts everywhere (datetime.MAX,
# year 9999). A literal, not computed, so import doesn't OverflowError on
# 32-bit Windows libc.
_MAX_UNIXTIME_SECONDS: Final[int] = (
    253402300799  # = datetime(9999,12,31,23,59,59,tz=UTC).timestamp()
)


def _datetime_from_unixtime(value: int) -> datetime.datetime:
    """Decode a UNIXTIME int64 into a UTC-aware ``datetime.datetime``.

    UTC-aware (UNIXTIME is seconds-since-epoch in UTC), which is asymmetric
    with the naive-local ``*FromTicks`` constructors -- round-tripping one
    through a UNIXTIME column shifts by the host offset; use ISO8601 (TEXT)
    for naive round-trip. Integer seconds only (no subsecond precision).

    Rejects ``value`` outside 0..MAX with DataError: the wire allows int64
    but ``fromtimestamp`` is platform-inconsistent on negatives (glibc
    accepts, Windows rejects), so reject uniformly.
    """
    # Reject bool (silently coerces to epoch/epoch+1) before fromtimestamp;
    # reachable with non-int via direct register_converter calls.
    if isinstance(value, bool):
        raise DataError(f"UNIXTIME value {value!r} must not be bool")
    if not isinstance(value, int):
        raise DataError(f"UNIXTIME value {value!r} must be int, got {type(value).__name__}")
    if not (0 <= value <= _MAX_UNIXTIME_SECONDS):
        raise DataError(
            f"UNIXTIME value {value!r} out of representable range "
            f"(0..{_MAX_UNIXTIME_SECONDS}); pre-1970 and post-9999 not supported"
        )
    try:
        return datetime.datetime.fromtimestamp(value, tz=datetime.UTC)
    except (TypeError, OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid UNIXTIME from server: {value!r}") from e


# register_adapter registry, consulted before the built-in datetime
# branches so a caller adapter can override them. Module-scope per stdlib
# pre-3.12 sqlite3.
_ADAPTERS: dict[type, Callable[[Any], Any]] = {}

# Wire-primitive types the codec accepts after adapter / __conform__
# chaining. ``bool`` listed explicitly (it's an int subclass) for greppability.
_WIRE_PRIMITIVES: tuple[type, ...] = (
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
    """Stdlib ``sqlite3.PrepareProtocol`` parity sentinel.

    Passed to a value's ``__conform__`` hook when no ``register_adapter``
    entry matches; carries no behaviour, used only as protocol identity.
    """

    pass


def register_adapter(type_: type, adapter: Callable[[Any], Any], /) -> None:
    """Register a Python-side adapter callable for ``type_``.

    ``adapter(value)`` runs before the wire encode for params of exactly
    ``type_`` (e.g. ``register_adapter(uuid.UUID, lambda u: u.bytes)``).
    Positional-only to match stdlib's C signature.

    No ``register_converter`` counterpart: the wire carries no declared
    column types, so use ``row_factory`` for per-row decoding.

    Scope is process-global (a single module dict shared by sync and async
    connections, matching pre-3.12 sqlite3); tests should clean up via
    :func:`unregister_adapter`.
    """
    # Stdlib accepts anything silently; tighten validation but keep the
    # rejection in the PEP 249 hierarchy.
    if not callable(adapter):
        raise ProgrammingError(
            f"adapter must be callable, got {type(adapter).__name__}",
            code=None,
        )
    if not isinstance(type_, type):
        raise ProgrammingError(
            f"type_ must be a class, got {type(type_).__name__}",
            code=None,
        )
    _ADAPTERS[type_] = adapter


def unregister_adapter(type_: type, /) -> None:
    """Remove a previously-registered adapter for ``type_``.

    The built-in datetime defaults (hardcoded in ``_convert_bind_param``)
    are unaffected; removing an override of a built-in type restores them.
    Raises :class:`~dqlitedbapi.exceptions.AdapterLookupError` (subclass of
    both ProgrammingError and LookupError) if no entry exists.
    """
    if type_ not in _ADAPTERS:
        raise AdapterLookupError(
            f"no adapter registered for {type_.__name__}",
            code=None,
        )
    del _ADAPTERS[type_]


def _convert_bind_param(value: Any) -> Any:
    """Map driver-level Python types to wire primitives.

    Stringifies datetime/date/time to ISO 8601; everything else passes
    through. A ``register_adapter`` adapter takes precedence and can
    override even the datetime handling.

    Two deliberate divergences from stdlib sqlite3: adapter/``__conform__``
    output that is itself a datetime chains into the ISO 8601 arm (stdlib
    is single-pass), and datetime/date/time *subclasses* bind via
    isinstance (stdlib requires an exact-type adapter).

    Rejects embedded-NUL TEXT binds with DataError (wire TEXT is
    NUL-terminated UTF-8); stdlib stores them. Use a BLOB column instead.
    """
    # type(value) not isinstance: adapter lookup is exact-class match
    # (stdlib contract; subclasses don't inherit the parent's adapter).
    original_type = type(value)
    adapter = _ADAPTERS.get(original_type)
    # Distinguish "adapter returned a non-primitive" from "no adapter" for
    # the post-chain validation diagnostic.
    transformed = False
    if adapter is not None:
        value = adapter(value)
        transformed = True
    else:
        # Stdlib parity: fall back to the value's ``__conform__`` hook
        # (getattr so instance-bound hooks are honoured). None declines.
        proto_method = getattr(value, "__conform__", None)
        if proto_method is not None:
            try:
                adapted = proto_method(PrepareProtocol)
            except BaseException as e:
                # Stdlib parity: a raising ``__conform__`` propagates
                # UNWRAPPED. Tag it so the outer ``_convert_params`` wrap arm
                # distinguishes this from register_adapter raises (which it
                # wraps as DataError); a marker attr keeps the exception type
                # intact for tests pinning the unwrapped-propagation contract.
                e._dqlite_conform_propagate = True  # type: ignore[attr-defined]
                raise
            if adapted is not None:
                value = adapted
                transformed = True
    # datetime subclasses date (not time), so check datetime/date first.
    if isinstance(value, datetime.datetime | datetime.date):
        return _iso8601_from_datetime(value)
    if isinstance(value, datetime.time):
        return _iso8601_from_time(value)
    # Validate adapter/__conform__ output here so the diagnostic names the
    # registration site, rather than letting it surface as a wire EncodeError.
    if not isinstance(value, _WIRE_PRIMITIVES):
        if transformed:
            raise DataError(
                f"adapter for {original_type.__name__} produced "
                f"non-primitive {type(value).__name__}; wire layer accepts "
                f"only int / float / str / bytes / bytearray / memoryview / "
                f"bool / None. Register an adapter that returns one of those."
            )
        raise DataError(
            f"type {original_type.__name__} is not supported; wire layer "
            f"accepts only int / float / str / bytes / bytearray / "
            f"memoryview / bool / None. Register an adapter that returns "
            f"one of those."
        )
    # Reject embedded-NUL TEXT here (after the adapter chain) so the message
    # names the BLOB workaround; the wire encoder also rejects but with an
    # internal-mechanic diagnostic. See the docstring for the stdlib divergence.
    if isinstance(value, str) and "\x00" in value:
        raise DataError(
            f"TEXT bind with embedded NUL at offset "
            f"{value.index(chr(0))} rejected by dqlite wire "
            f"(NUL-terminated UTF-8). Cross-driver divergence from "
            f"stdlib sqlite3 which preserves NULs in TEXT. Use a "
            f"BLOB column (bind bytes/memoryview) to round-trip "
            f"NUL-containing data."
        )
    return value
