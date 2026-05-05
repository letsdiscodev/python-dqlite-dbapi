"""PEP 249 type objects and constructors for dqlite."""

import datetime
import math
from typing import Any, Final

from dqlitedbapi.exceptions import DataError
from dqlitewire.constants import ValueType

# PEP 249 §3: type objects + constructors. Private helpers
# (``_DescriptionTuple``, ``_Description``, encoder/decoder helpers)
# are deliberately NOT exported — they are consumed by sibling
# modules via the already-tightened import surface.
__all__ = [
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "Binary",
    "Date",
    "DateFromTicks",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
]

# Shape of ``cursor.description`` per PEP 249 §6.1.2: a sequence of
# 7-tuples ``(name, type_code, display_size, internal_size, precision,
# scale, null_ok)``. dqlite populates only ``name`` and ``type_code``
# (the wire ``ValueType`` int); the other five are always ``None``.
# Live here so sync/async cursors and the sqlalchemy adapter share one
# shape instead of repeating the inline tuple at every site.
_DescriptionTuple = tuple[str, int | None, None, None, None, None, None]
_Description = tuple[_DescriptionTuple, ...] | None


# Type constructors
def Date(year: int, month: int, day: int) -> datetime.date:
    """Construct a date value."""
    return datetime.date(year, month, day)


def Time(
    hour: int,
    minute: int,
    second: int,
    microsecond: int = 0,
    tzinfo: datetime.tzinfo | None = None,
) -> datetime.time:
    """Construct a time value.

    Accepts optional ``microsecond`` and ``tzinfo`` for parity with
    stdlib ``datetime.time``. PEP 249 does not require this,
    but mixing the driver's ``Time()`` with ``datetime.time`` would
    otherwise drop sub-second precision silently.
    """
    return datetime.time(hour, minute, second, microsecond, tzinfo=tzinfo)


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
    return datetime.datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo)


def _validate_ticks(ticks: float) -> float:
    """Normalize ``ticks`` to a finite float or raise ``DataError``.

    ``fromtimestamp`` raises different stdlib exceptions depending on
    the failure mode (``ValueError`` for NaN on some platforms,
    ``OverflowError`` / ``OSError`` for out-of-range, ``TypeError`` for
    unsupported argument types like ``Decimal``). Normalize every
    failure mode up front so the caller always sees a single DB-API
    ``DataError``.

    ``bool`` is an ``int`` subclass, but coercing ``True`` / ``False``
    to 1.0 / 0.0 silently produces a Unix-epoch timestamp — a caller-
    bug trap. Mirror ``arraysize.setter`` and reject explicitly.
    Strings are rejected even though ``float("1.5")`` succeeds: PEP
    249's ``*FromTicks`` API takes a numeric tick value, and accepting
    a string would silently encourage a buggy caller who passed an
    unconverted wire value.

    Returns the coerced ``float`` so callers can pass a value
    ``datetime.fromtimestamp`` accepts (it rejects ``Decimal``).
    """
    # Mirror ``arraysize.setter`` (cursor.py): bool is an int subclass,
    # but silent coercion to 0/1 is a footgun.
    if isinstance(ticks, bool):
        raise DataError(f"Invalid timestamp ticks: {ticks!r} (bool)")
    if isinstance(ticks, str):
        raise DataError(f"Invalid timestamp ticks: {ticks!r} (str)")
    try:
        coerced = float(ticks)
    except (TypeError, ValueError) as exc:
        raise DataError(f"Invalid timestamp ticks: {ticks!r} ({exc})") from exc
    if not math.isfinite(coerced):
        raise DataError(f"Invalid timestamp ticks: {coerced}")
    return coerced


def DateFromTicks(ticks: float) -> datetime.date:
    """Construct a date from a Unix timestamp.

    Returns a naive date interpreted as the host's **local** time zone,
    matching stdlib ``sqlite3.dbapi2.DateFromTicks``. For an explicit
    UTC interpretation, call ``datetime.datetime.fromtimestamp(ticks,
    tz=datetime.UTC).date()`` directly.

    Note that the wire layer's UNIXTIME decoder
    (``_datetime_from_unixtime``) returns UTC-aware datetimes; storing
    a value produced by this constructor on a UNIXTIME-typed column
    and reading it back will shift by the host's UTC offset. Use
    ISO8601 (TEXT) columns for faithful round-trip of naive values.
    """
    coerced = _validate_ticks(ticks)
    try:
        return datetime.date.fromtimestamp(coerced)
    except (OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid timestamp ticks {ticks}: {e}") from e


def TimeFromTicks(ticks: float) -> datetime.time:
    """Construct a time from a Unix timestamp.

    Returns a naive time interpreted as the host's **local** time zone,
    matching stdlib ``sqlite3.dbapi2.TimeFromTicks``. Near midnight in
    non-UTC locales the wall-clock time differs from the UTC time;
    callers that need UTC should use
    ``datetime.datetime.fromtimestamp(ticks, tz=datetime.UTC).time()``.

    See ``DateFromTicks`` for the tz asymmetry with the UNIXTIME
    decoder on readback.
    """
    coerced = _validate_ticks(ticks)
    try:
        return datetime.datetime.fromtimestamp(coerced).time()
    except (OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid timestamp ticks {ticks}: {e}") from e


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    """Construct a timestamp from a Unix timestamp.

    Returns a naive datetime interpreted as the host's **local** time
    zone, matching stdlib ``sqlite3.dbapi2.TimestampFromTicks`` (and
    PEP 249's own convention). For UTC-aware values, call
    ``datetime.datetime.fromtimestamp(ticks, tz=datetime.UTC)``
    directly.

    See ``DateFromTicks`` for the tz asymmetry with the UNIXTIME
    decoder on readback.
    """
    coerced = _validate_ticks(ticks)
    try:
        return datetime.datetime.fromtimestamp(coerced)
    except (OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid timestamp ticks {ticks}: {e}") from e


# PEP 249 §3 "Binary(string) — construct an object capable of holding
# a binary (long) string value." Stdlib ``sqlite3.Binary = memoryview``
# (Python 3.13); aiosqlite inherits. Alias directly so ports from
# stdlib sqlite3 stay drop-in (``isinstance(Binary(b), memoryview)``
# holds on both, zero-copy wrap on both). The wire encoder accepts
# memoryview for BLOB columns, so no conversion is needed on the
# bind path.
Binary = memoryview


# Type objects for column type checking.
#
# PEP 249: "These objects represent a data type as represented in the
# database. The module exports these objects: STRING, BINARY, NUMBER,
# DATETIME, ROWID. The module should export a comparison for these types
# and the object returned in Cursor.description[i][1]."
#
# Cursor.description[i][1] here is a wire-level ``ValueType`` integer
# (e.g. 10 for ISO8601). The type objects below compare equal to both
# the uppercase SQL type name strings (for declared-type matching) and
# the matching ``ValueType`` ints.
class _DBAPIType:
    """Base type for DB-API type objects. Compares equal to matching
    uppercase SQL type names (str) and wire-level ``ValueType`` codes
    (int).

    Deliberately **unhashable**: use these objects only with ``==``
    against ``description[i][1]`` — do not use them as dict keys or
    ``set`` members. ``NUMBER`` / ``DATETIME`` wrap multiple wire codes
    (e.g. INTEGER + FLOAT + BOOLEAN), so a hash satisfying the Python
    hash-eq invariant does not exist: any canonical-representative hash
    would make ``{NUMBER: x}[FLOAT_CODE]`` raise ``KeyError`` despite
    ``NUMBER == FLOAT_CODE`` being True. Refusing to hash turns that
    silent miss into a noisy ``TypeError``.

    **Caller idiom** — introspecting ``description[i][1]`` against
    type sentinels: use chained equality, NOT set membership::

        type_code = cur.description[i][1]
        if type_code == STRING or type_code == NUMBER:
            ...

    Set/dict membership (``type_code in {STRING, NUMBER}``) raises
    ``TypeError: unhashable type`` for the reason above. The
    chained-``==`` form is the PEP 249 idiom and works against this
    driver and stdlib ``sqlite3`` (which does not export these
    sentinels at all).
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

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        return self._name or f"_DBAPIType({sorted(self.values, key=str)!r})"


STRING = _DBAPIType("TEXT", "VARCHAR", "CHAR", "CLOB", ValueType.TEXT, _name="STRING")
BINARY = _DBAPIType("BLOB", "BINARY", "VARBINARY", ValueType.BLOB, _name="BINARY")
NUMBER = _DBAPIType(
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
DATETIME = _DBAPIType(
    "DATE",
    "TIME",
    "TIMESTAMP",
    "DATETIME",
    ValueType.ISO8601,
    ValueType.UNIXTIME,
    _name="DATETIME",
)
ROWID = _DBAPIType("ROWID", "INTEGER PRIMARY KEY", ValueType.INTEGER, _name="ROWID")


# Internal conversion helpers.
#
# The wire codec deals only in primitives (ISO8601 → str, UNIXTIME → int64).
# PEP 249 specifies that drivers SHOULD return datetime objects for date/time
# columns — and every major Python driver (psycopg, mysqlclient, asyncpg, ...)
# does. These helpers implement that conversion at the driver (DBAPI) layer,
# matching Go's database/sql driver split.


# Display cap on server-controlled text embedded into exception
# messages. Wire ``decode_text`` accepts up to
# ``_MAX_TEXT_VALUE_SIZE`` (64 MiB); without truncation, an
# unparseable ISO8601 cell from a hostile or compromised server
# would inflate every ``DataError`` by ~2× via ``{text!r}`` quoting,
# AND be preserved across pickle / structured logging via
# ``Error.__reduce__``. Mirrors ``sqlalchemy-dqlite/base.py`` which
# truncates server-controlled strings before logging.
_MAX_DATA_ERROR_TEXT_DISPLAY: Final[int] = 200


def _truncate_for_message(text: str) -> str:
    """Bound a server-controlled string before interpolating into a
    DataError message. The truncation marker carries the original
    length so a triaging operator knows the original size class
    without exposing the full payload."""
    if len(text) <= _MAX_DATA_ERROR_TEXT_DISPLAY:
        return text
    return (
        f"{text[:_MAX_DATA_ERROR_TEXT_DISPLAY]}"
        f"... [truncated, {len(text) - _MAX_DATA_ERROR_TEXT_DISPLAY} chars]"
    )


def _format_utc_offset(offset: datetime.timedelta) -> str:
    """Format a UTC offset as ``±HH:MM`` (common) or ``±HH:MM:SS``.

    Historical IANA LMT entries (Europe/Dublin pre-1916, Africa/Lagos
    pre-1914, several Pacific zones) carry sub-minute offsets.
    ``datetime.fromisoformat`` / ``time.fromisoformat`` on Python 3.11+
    round-trip ``±HH:MM:SS`` so emitting the seconds component keeps
    the round-trip through a TEXT column exact. Common whole-minute
    offsets stay in the narrower ``±HH:MM`` form byte-identical with
    the pre-sub-minute encoder output.

    Rejects two broken-tzinfo inputs (CPython's own ``timezone()``
    constructor rejects the same conditions — these paths are only
    reachable through hand-rolled tzinfo subclasses that bypass the
    stdlib's input validation):

    - ``|offset| >= 24h`` — would emit an out-of-range ``±HH:MM:SS``
      token that ``datetime.fromisoformat`` / peer decoders reject.
    - Sub-second precision — ``int(offset.total_seconds())`` truncates
      toward zero, so a negative fractional offset flips sign and
      zeros magnitude. Round to whole-second and require the result
      match the input.
    """
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

    Uses the space-separated layout so values are byte-for-byte comparable
    with what Go and the C client produce. Accepts both naive and
    timezone-aware datetimes — naive values round-trip as naive (matching
    pysqlite semantics), aware values preserve the offset.

    Known limitations:

    - ``datetime.fold`` is **not** encoded. ISO 8601 has no fold
      notation, so a round-trip of ``datetime(..., fold=1, tzinfo=...)``
      at the DST "fall back" hour silently produces ``fold=0`` on
      decode. Applications that straddle DST transitions should store
      UTC instants (or a UTC-relative marker column) rather than
      wall-clock datetimes. This matches stdlib ``sqlite3``'s datetime
      adapter; any change here would diverge from that reference.
    """
    if isinstance(value, datetime.datetime):
        base = f"{value.year:04d}" + value.strftime("-%m-%d %H:%M:%S")
        if value.microsecond:
            base += f".{value.microsecond:06d}"
        if value.tzinfo is None:
            return base
        # tzinfo is set (checked above), so utcoffset() returns timedelta.
        # A None here means the tzinfo subclass declared itself but
        # cannot resolve an offset for this datetime — be explicit
        # and reject rather than silently demoting to naive (which
        # would lose the user's tz-awareness intent without warning).
        offset = value.utcoffset()
        if offset is None:
            raise DataError(
                f"datetime is tz-aware but tzinfo.utcoffset() returned None for "
                f"{value!r}; cannot encode without a resolvable UTC offset"
            )
        return base + _format_utc_offset(offset)
    # datetime.date (must come after datetime check — datetime is a subclass).
    return value.isoformat()


def _iso8601_from_time(value: datetime.time) -> str:
    """Format a ``datetime.time`` as an ISO 8601 string.

    Symmetric with the datetime/date encoder so the PEP 249 ``Time()``
    and ``TimeFromTicks()`` constructors — which return
    ``datetime.time`` — produce values the DB-API bind path can
    consume. Naive times emit ``HH:MM:SS[.ffffff]``; aware times
    append the ``±HH:MM`` (or ``±HH:MM:SS`` for sub-minute offsets)
    suffix shared with the datetime encoder.
    """
    base = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
    if value.microsecond:
        base += f".{value.microsecond:06d}"
    if value.tzinfo is None:
        return base
    offset = value.utcoffset()
    if offset is None:
        raise DataError(
            f"time is tz-aware but tzinfo.utcoffset() returned None for "
            f"{value!r}; cannot encode without a resolvable UTC offset"
        )
    return base + _format_utc_offset(offset)


def _datetime_from_iso8601(text: str) -> datetime.datetime | datetime.time | None:
    """Parse an ISO 8601 string into ``datetime.datetime`` / ``.time``.

    Returns ``None`` for the empty string — pre-null-patch dqlite servers
    sometimes emit empty text for NULL datetime cells, and the modern
    server still tolerates empty ISO8601 values. Returning None matches
    PEP 249 NULL semantics.

    Tries, in order:

    1. ``datetime.datetime.fromisoformat`` — ``YYYY-MM-DD HH:MM:SS[…]``
    2. ``datetime.time.fromisoformat`` — ``HH:MM:SS[.ffffff][±HH:MM]``,
       matching the ``_iso8601_from_time`` bind-path encoder so a
       ``datetime.time`` bound via the driver round-trips as
       ``datetime.time`` on readback rather than raising ``DataError``.
    3. ``datetime.date.fromisoformat`` — ``YYYY-MM-DD`` (widened to
       ``datetime.datetime`` on return; see below).

    Naive input round-trips as naive; aware input preserves the offset.
    Python 3.11+ ``datetime.fromisoformat`` accepts a trailing ``Z``
    natively; no pre-substitution is needed.

    **``date`` widens to ``datetime`` on round-trip.** A ``datetime.date``
    passed to PEP 249 ``Date()`` serializes via ``isoformat()`` as
    ``"YYYY-MM-DD"`` (no time component). The decoder's fallback path
    parses the string with ``datetime.date.fromisoformat`` and returns
    a ``datetime.datetime(year, month, day)`` — the value widens from
    date to datetime. This matches pysqlite's default behaviour (stdlib
    ``sqlite3`` with ``detect_types`` does the same widen). Callers who
    need a strict ``date`` on readback should narrow via ``.date()`` or
    use the SQLAlchemy ``_DqliteDate`` type that does the narrowing at
    the ORM layer.

    ``datetime.time`` does NOT widen — ``HH:MM:SS`` has no date
    component so widening would require an arbitrary sentinel date.

    A malformed string from the server (bug, corruption, or MitM) would
    otherwise escape as a raw ``ValueError``; wrap as ``DataError`` to
    satisfy PEP 249's "all DB errors funnel through Error" contract.
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
        # ``datetime.fromisoformat`` on Python 3.11+ accepts every
        # form ``date.fromisoformat`` accepts (bare ``YYYY-MM-DD``,
        # ISO week dates), so the only remaining failure shape is
        # input that ``time.fromisoformat`` also rejects — i.e.
        # genuine garbage. Surface as ``DataError``.
        #
        # Truncate the offending text before interpolation. A hostile
        # peer can return a wire TEXT cell up to ``_MAX_TEXT_VALUE_SIZE``
        # (64 MiB); embedding the full payload in the exception message
        # — which is then preserved across pickle, logged, and copied
        # into ``raw_message`` — produces a ~128 MiB payload per
        # malformed cell. The SA result_processor sites already truncate
        # via ``_truncate_for_log``; this is the dbapi-layer twin that
        # surfaces when SA is bypassed.
        raise DataError(
            f"Cannot parse ISO 8601 datetime from server: {_truncate_for_message(text)!r}"
        ) from exc


# Maximum UNIXTIME value that ``datetime.fromtimestamp(..., tz=UTC)``
# accepts on every supported platform (year 9999-12-31T23:59:59Z, the
# upper boundary of ``datetime.MAX``). Computed as a literal so import
# does not depend on the host's libc behavior at module-load time —
# 32-bit Windows would otherwise OverflowError on the
# ``datetime.MAX.timestamp()`` round-trip.
_MAX_UNIXTIME_SECONDS: Final[int] = (
    253402300799  # = datetime(9999,12,31,23,59,59,tz=UTC).timestamp()
)


def _datetime_from_unixtime(value: int) -> datetime.datetime:
    """Decode a UNIXTIME int64 into a UTC-aware ``datetime.datetime``.

    UNIXTIME is unambiguously seconds-since-epoch in UTC, so returning a
    UTC-aware value is faithful. Callers that want local time can convert.

    This UTC-aware result is asymmetric with the PEP 249 ``*FromTicks``
    constructors, which return naive local time (matching stdlib
    sqlite3). Storing a ``TimestampFromTicks`` value on a UNIXTIME
    column and reading it back shifts by the host's UTC offset; use
    an ISO8601 (TEXT) column for faithful round-trip of naive values.

    Range: ``0 <= value <= _MAX_UNIXTIME_SECONDS``. Negative values
    (pre-1970) and values past year 9999 are rejected uniformly with
    ``DataError``. The wire allows int64 (so negatives are
    spec-compliant), but ``datetime.fromtimestamp`` is platform-
    inconsistent on negatives — Linux glibc accepts, Windows
    ``_gmtime64_s`` rejects — so the same byte stream behaves
    differently depending on host. Forcing a uniform rejection
    eliminates that surprise. dqlite servers do not emit pre-1970
    UNIXTIME today.

    A corrupt server or MitM-modified bytes could deliver a non-integer
    or in-range value that still trips the underlying stdlib; wrap any
    surviving stdlib exception as ``DataError``.
    """
    if (
        isinstance(value, int)
        and not isinstance(value, bool)
        and not (0 <= value <= _MAX_UNIXTIME_SECONDS)
    ):
        raise DataError(
            f"UNIXTIME value {value!r} out of representable range "
            f"(0..{_MAX_UNIXTIME_SECONDS}); pre-1970 and post-9999 not supported"
        )
    try:
        return datetime.datetime.fromtimestamp(value, tz=datetime.UTC)
    except (TypeError, OverflowError, OSError, ValueError) as e:
        raise DataError(f"Invalid UNIXTIME from server: {value!r}") from e


# Stdlib-parity ``register_adapter`` registry: maps Python type →
# adapter callable. Consulted by ``_convert_bind_param`` BEFORE the
# built-in datetime branches so a caller-supplied adapter can override
# even datetime handling. Module-scope per stdlib pre-3.12 sqlite3
# semantics (per-Connection scope was added in stdlib 3.12 but
# requires more state plumbing — the module-scope shape is the more
# common ergonomic on the existing ecosystem).
_ADAPTERS: dict[type, "Any"] = {}


def register_adapter(type_: type, adapter: "Any") -> None:
    """Register a Python-side adapter callable for ``type_``.

    Mirrors stdlib ``sqlite3.register_adapter``: when a parameter of
    type ``type_`` reaches the bind layer, ``adapter(value)`` runs
    in the driver before the wire encode. Common uses:

    - ``register_adapter(decimal.Decimal, str)`` — bind ``Decimal``
      via TEXT.
    - ``register_adapter(uuid.UUID, lambda u: u.bytes)`` — bind
      UUID via BLOB.
    - ``register_adapter(pathlib.Path, str)``,
      ``register_adapter(MyEnum, lambda e: e.value)``, etc.

    Symmetric with stdlib's ``register_converter`` is NOT
    implemented: dqlite's wire protocol does not carry declared
    column types, so type-name-keyed converters cannot be
    dispatched on read. Callers wanting per-row decoding can use
    ``Cursor.row_factory`` (when implemented) or post-fetch
    coercion in user code.

    **Scope: process-global.** Adapters live in a single module-
    level dict shared by every sync and async connection in the
    process — registering on either ``dqlitedbapi`` or
    ``dqlitedbapi.aio`` mutates the same dict. This matches stdlib
    ``sqlite3.register_adapter`` pre-3.12 semantics. psycopg3-style
    per-connection scoping (``Connection.adapters``) is NOT
    supported: registering here affects every connection in the
    process. Tests that register adapters should clean up by
    explicitly removing the entry afterwards. Note that calling
    ``register_adapter(type_, None)`` does NOT unregister — it
    raises ``TypeError`` because ``None`` fails the
    callable-check above. To remove an adapter, access the
    private registry directly:
    ``from dqlitedbapi.types import _ADAPTERS;
    _ADAPTERS.pop(type_, None)``. This is internal and acceptable
    for test cleanup; production code should not rely on it.
    """
    if not callable(adapter):
        raise TypeError(f"adapter must be callable, got {type(adapter).__name__}")
    if not isinstance(type_, type):
        raise TypeError(f"type_ must be a class, got {type(type_).__name__}")
    _ADAPTERS[type_] = adapter


def _convert_bind_param(value: Any) -> Any:
    """Map driver-level Python types to wire primitives.

    The wire codec accepts only bool/int/float/str/bytes/None; datetime,
    date, and time are driver-level conveniences that we stringify to
    ISO 8601 before handing off. Everything else passes through
    unchanged.

    A user-registered adapter (via ``register_adapter``) takes
    precedence — it can override the built-in datetime / date / time
    handlers and is the canonical hook for binding Decimal, UUID,
    Path, Enum, etc.
    """
    # User-registered adapter takes precedence. ``type(value)`` not
    # isinstance: stdlib's contract is exact-class match (subclasses
    # do not inherit the parent class's adapter unless explicitly
    # registered). This keeps the contract predictable and matches
    # ``sqlite3.register_adapter``.
    adapter = _ADAPTERS.get(type(value))
    if adapter is not None:
        value = adapter(value)
    # ``datetime.datetime`` is a subclass of ``datetime.date`` but not
    # of ``datetime.time``, so the datetime/date check must fire first
    # for datetime inputs. ``datetime.time`` falls through to its own
    # branch.
    if isinstance(value, datetime.datetime | datetime.date):
        return _iso8601_from_datetime(value)
    if isinstance(value, datetime.time):
        return _iso8601_from_time(value)
    return value
