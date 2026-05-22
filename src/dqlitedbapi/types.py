"""PEP 249 type objects and constructors for dqlite."""

import datetime
import math
from collections.abc import Callable
from typing import Any, Final, final

from dqlitedbapi.exceptions import DataError, ProgrammingError
from dqlitewire import ValueType

# PEP 249 §3: type objects + constructors. ``DescriptionTuple`` is
# the public alias for the cursor.description row shape — exported so
# downstream typed wrappers (``sqlalchemy-dqlite``) don't have to
# import the underscore-prefixed alias across a package boundary.
__all__ = [
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
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

# Shape of ``cursor.description`` per PEP 249 §6.1.2: a sequence of
# 7-tuples ``(name, type_code, display_size, internal_size, precision,
# scale, null_ok)``. dqlite populates only ``name`` and ``type_code``
# (the wire ``ValueType`` int); the other five are always ``None``.
# Live here so sync/async cursors and the sqlalchemy adapter share one
# shape instead of repeating the inline tuple at every site.
# PEP 695 ``type X = ...`` syntax matches the rest of the workspace's
# public type-alias declarations (wire/types.py, client/_dial.py,
# client/cluster.py, sqlalchemydqlite/aio.py).
type DescriptionTuple = tuple[str, int | None, None, None, None, None, None]
type _Description = tuple[DescriptionTuple, ...] | None

# stdlib ``sqlite3``-style row factory callable. Invoked as
# ``factory(cursor, row_tuple)`` (see ``Connection.row_factory`` /
# ``Cursor.row_factory`` docstrings) and may return any object — the
# value replaces the raw tuple in the cursor's result stream.
# ``Callable[..., Any]`` avoids the forward-reference circularity
# between the cursor and connection types; the setter on both
# ``Connection.row_factory`` and ``Cursor.row_factory`` validates
# ``callable(value)`` at runtime, so the call shape is enforced
# operationally. Used as the canonical type across the four mirror
# sites (sync/async × connection/cursor).
type RowFactory = Callable[..., Any]


# Type constructors
#
# PEP 249 §7 mandates every error from a driver call subclass ``Error``.
# Stdlib's ``datetime.{date,time,datetime}.__init__`` raise bare
# ``ValueError`` / ``TypeError`` on invalid inputs (month=13, hour=25,
# non-numeric arguments). Wrap each constructor to translate those into
# ``DataError`` so cross-driver code patterns
# (``try: dqlitedbapi.Date(...) except dqlitedbapi.Error: ...``) are
# closed-form. Mirrors the discipline already in ``DateFromTicks`` /
# ``TimeFromTicks`` / ``TimestampFromTicks``.


def Date(year: int, month: int, day: int) -> datetime.date:
    """Construct a date value.

    Note: this is a function wrapper (not a class alias as in stdlib
    ``sqlite3.dbapi2``); ``isinstance(v, dqlitedbapi.Date)`` therefore
    raises ``TypeError`` because functions are not classes. Use
    ``isinstance(v, datetime.date)`` for cross-driver code that runs
    against both stdlib ``sqlite3`` and ``dqlitedbapi``. The wrapper
    exists so out-of-range inputs raise PEP 249 ``DataError`` rather
    than bare ``ValueError``.
    """
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

    Accepts optional ``microsecond`` and ``tzinfo`` for parity with
    stdlib ``datetime.time``. PEP 249 does not require this,
    but mixing the driver's ``Time()`` with ``datetime.time`` would
    otherwise drop sub-second precision silently.

    Note: this is a function wrapper (not a class alias);
    ``isinstance(v, dqlitedbapi.Time)`` raises ``TypeError``. Use
    ``isinstance(v, datetime.time)`` for cross-driver porting code.
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
    """Construct a timestamp value.

    Note: this is a function wrapper (not a class alias);
    ``isinstance(v, dqlitedbapi.Timestamp)`` raises ``TypeError``. Use
    ``isinstance(v, datetime.datetime)`` for cross-driver porting code.
    """
    try:
        return datetime.datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo)
    except (TypeError, ValueError) as e:
        raise DataError(
            f"Timestamp({year!r}, {month!r}, {day!r}, {hour!r}, "
            f"{minute!r}, {second!r}) invalid: {e}"
        ) from e


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
    like stdlib ``sqlite3.dbapi2.DateFromTicks``. For an explicit UTC
    interpretation, call ``datetime.datetime.fromtimestamp(ticks,
    tz=datetime.UTC).date()`` directly.

    Implementation uses ``datetime.date.fromtimestamp`` rather than
    stdlib's ``time.localtime(ticks)[:3]``; sub-second precision in
    ``ticks`` is dropped because the return type is date-only.

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
    like stdlib ``sqlite3.dbapi2.TimeFromTicks``. Near midnight in
    non-UTC locales the wall-clock time differs from the UTC time;
    callers that need UTC should use
    ``datetime.datetime.fromtimestamp(ticks, tz=datetime.UTC).time()``.

    Implementation uses ``datetime.datetime.fromtimestamp(ticks).time()``;
    unlike stdlib's ``time.localtime(ticks)[3:6]`` (which truncates
    sub-second precision), this **preserves** the microsecond component
    of fractional ``ticks``.

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
    zone, like stdlib ``sqlite3.dbapi2.TimestampFromTicks`` (and
    PEP 249's own convention). For UTC-aware values, call
    ``datetime.datetime.fromtimestamp(ticks, tz=datetime.UTC)``
    directly.

    Implementation uses ``datetime.datetime.fromtimestamp(ticks)``;
    unlike stdlib's ``time.localtime(ticks)[:6]`` (which truncates
    sub-second precision), this **preserves** the microsecond component
    of fractional ``ticks`` — e.g. ``TimestampFromTicks(1700000000.5)``
    returns ``datetime(..., microsecond=500_000)`` whereas stdlib
    returns ``microsecond=0``.

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
#
# **Limitation vs sibling type-constructors**: ``Date`` / ``Time`` /
# ``Timestamp`` / ``*FromTicks`` wrap stdlib ``TypeError`` /
# ``ValueError`` on bad input as ``DataError`` (per project
# discipline that PEP 249 §3 type-constructors stay inside the
# ``dbapi.Error`` hierarchy). ``Binary`` does NOT — bad input
# (``Binary("not bytes")``, ``Binary(123)``, ``Binary(None)``)
# leaks bare ``TypeError`` from the underlying ``memoryview``
# constructor, *outside* the dbapi error hierarchy. This is a
# **deliberate stdlib-parity tradeoff**: wrapping ``Binary`` in a
# function would break ``isinstance(Binary(b), memoryview)``,
# which cross-driver porting code from stdlib ``sqlite3`` and
# aiosqlite relies on. Callers who want PEP 249 §7 hierarchy
# purity for binary input should wrap their own ``try`` /
# ``except (TypeError, ValueError)`` and re-raise as
# ``dqlitedbapi.DataError``.
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
@final
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


STRING: Final[_DBAPIType] = _DBAPIType(
    "TEXT", "VARCHAR", "CHAR", "CLOB", ValueType.TEXT, _name="STRING"
)
# NOTE: ``ValueType.ISO8601`` is intentionally NOT in ``STRING`` — even
# though the wire cell IS text-encoded, the dbapi layer converts it to
# ``datetime.datetime`` / ``datetime.time`` before the user sees the
# result. Type-code introspection against ``STRING`` for an ISO8601
# column returns False (the right answer post-conversion); against
# ``DATETIME`` it returns True. The wire-level text-encoding is an
# implementation detail invisible at the dbapi surface. The
# declared-type-name vocabulary (``"TEXT"``, ``"VARCHAR"``, ``"CHAR"``,
# ``"CLOB"``) is a separate matching surface for ``description[i][1]``
# decltype strings — independent of the wire ``ValueType`` matching.
# psycopg2/3 use the same partition (timestamp types match DATETIME,
# not STRING).
BINARY: Final[_DBAPIType] = _DBAPIType(
    "BLOB", "BINARY", "VARBINARY", ValueType.BLOB, _name="BINARY"
)
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
# NOTE: ``ROWID.values`` deliberately overlaps ``NUMBER.values`` on the
# wire-level ``ValueType.INTEGER`` code. For any INTEGER column ``i``,
# BOTH ``cur.description[i][1] == NUMBER`` AND
# ``cur.description[i][1] == ROWID`` return ``True``. The wire protocol
# carries no "this column is a rowid alias" hint, so the dbapi cannot
# distinguish a generic INTEGER column from a rowid; the type sentinels
# advertise the union. PEP 249 §3 does not outlaw overlap between Type
# Objects. Cross-driver callers iterating type sentinels with the
# chained-``==`` idiom (the documented PEP 249 form) get both
# predicates true for INTEGER columns; psycopg2 and stdlib ``sqlite3``
# treat ``ROWID`` / ``NUMBER`` disjointly (sqlite3 does not export
# ``ROWID`` at all), so this is a deliberate cross-driver portability
# caveat. Match in order (``ROWID`` first if the distinction matters)
# or consult the decltype string via ``description[i][0]`` against the
# declared-type vocabulary.
ROWID: Final[_DBAPIType] = _DBAPIType(
    "ROWID", "INTEGER PRIMARY KEY", ValueType.INTEGER, _name="ROWID"
)


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
    DataError message. The truncation marker carries the OVERFLOW
    codepoint count (number of characters dropped past the cap) so a
    triaging operator knows the size class without exposing the full
    payload.

    Local re-implementation of the wire-layer ``_cap_raw_message``
    SSOT shape (overflow-count suffix vocabulary). Kept independent
    because the suffix wording here (``"... [truncated, N chars]"``)
    differs from the wire SSOT's ``"... [raw_message truncated, N
    codepoints]"`` — both report the same CPython quantity
    (``len(str)`` counts codepoints), and the divergence is intentional
    context-specific wording. See the cross-package divergence index
    in ``dqlitewire/_truncate.py``.
    """
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
        # A raising tzinfo (custom subclass with a buggy utcoffset)
        # must surface as DataError too — symmetric with the
        # ``_validate_ticks`` and ``_datetime_from_unixtime`` discipline,
        # so every plausible tzinfo failure stays inside the
        # ``dbapi.Error`` hierarchy.
        try:
            offset = value.utcoffset()
        except (TypeError, ValueError) as exc:
            raise DataError(f"tzinfo.utcoffset() raised for {value!r}: {exc}") from exc
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
    # See ``_iso8601_from_datetime`` for the wrap rationale: a raising
    # custom tzinfo must surface as DataError, not bare exception class.
    try:
        offset = value.utcoffset()
    except (TypeError, ValueError) as exc:
        raise DataError(f"tzinfo.utcoffset() raised for {value!r}: {exc}") from exc
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

    **Silent NULL collision.** An empty ISO8601 cell ``""`` decodes to
    ``None`` — indistinguishable from a wire NULL. This is a deliberate
    tolerance for pre-null-patch dqlite servers, but the same empty
    decoding fires for legitimate empty-string projections on the modern
    server: a ``COALESCE(date_col, '')`` projection where the NULL
    branch is taken, or a ``CASE WHEN x THEN '' ELSE date_col END``
    expression on a column the server tags as ISO8601, both decode to
    ``None`` here. The wire-layer TEXT decoder distinguishes NULL from
    the empty string by tagging the cell type, but ISO8601-tagged cells
    go through this decoder and lose that distinction (flattened to
    ``None``). Callers needing to distinguish "the
    column was NULL" from "the projection produced an empty string"
    must avoid ISO8601-tagged columns for that pattern (e.g. use a
    ``CAST(... AS TEXT)`` projection so the cell carries ``TEXT`` on
    the wire instead).

    Two-step fallback (in order):

    1. ``datetime.datetime.fromisoformat`` — covers full
       ``YYYY-MM-DD HH:MM:SS[.ffffff][±HH:MM]`` plus bare
       ``YYYY-MM-DD``. On Python 3.11+ ``datetime.fromisoformat``
       widened to accept every shape ``date.fromisoformat`` accepts,
       so a bare-date input lands here (returning midnight datetime
       — see the date-widens paragraph below).
    2. ``datetime.time.fromisoformat`` — ``HH:MM:SS[.ffffff][±HH:MM]``,
       matching the ``_iso8601_from_time`` bind-path encoder so a
       ``datetime.time`` bound via the driver round-trips as
       ``datetime.time`` on readback rather than raising ``DataError``.

    There is intentionally NO third ``date.fromisoformat`` arm — step 1
    already covers bare ``YYYY-MM-DD`` on every supported Python.
    Ordinal-date form ``YYYY-OOO`` (which ``date.fromisoformat``
    accepts but ``datetime.fromisoformat`` rejects) is therefore
    rejected here too; no upstream emits ordinal dates today, so the
    asymmetry is benign.

    Naive input round-trips as naive; aware input preserves the offset.
    Python 3.11+ ``datetime.fromisoformat`` accepts a trailing ``Z``
    natively; no pre-substitution is needed.

    **``date`` widens to ``datetime`` on round-trip.** A ``datetime.date``
    passed to PEP 249 ``Date()`` serializes via ``isoformat()`` as
    ``"YYYY-MM-DD"`` (no time component). Step 1 above parses that
    bare-date string and returns ``datetime.datetime(year, month, day)``
    at midnight — the value widens from date to datetime. This matches
    pysqlite's default behaviour (stdlib ``sqlite3`` with
    ``detect_types`` does the same widen). Callers who need a strict
    ``date`` on readback should narrow via ``.date()`` or use the
    SQLAlchemy ``_DqliteDate`` type that does the narrowing at the
    ORM layer.

    ``datetime.time`` does NOT widen — ``HH:MM:SS`` has no date
    component so widening would require an arbitrary sentinel date.

    **Sub-microsecond fractional seconds are silently truncated.**
    CPython's ``datetime.fromisoformat`` (widened in 3.11 via gh-80010
    to accept any fractional-digit count) drops digits beyond the
    sixth without rounding — e.g. ``"2026-05-21 12:34:56.123456789"``
    decodes to ``datetime(2026, 5, 21, 12, 34, 56, 123456)`` and
    ``".999999999"`` decodes to ``...microsecond=999999`` (NOT
    1_000_000 — truncation does NOT carry into the second). Peer
    encoders that emit nanosecond-precision text (Go
    ``time.RFC3339Nano``, Litestream-style millisecond triggers
    widened with extra zeros, custom SQLite triggers via loadable
    extensions, mixed-source clusters that round-trip through other
    drivers) therefore do not round-trip byte-identically through
    this decoder: the companion encoder ``_iso8601_from_datetime``
    emits exactly six fractional digits, so a peer-written 9-digit
    value read here and re-written becomes a 6-digit value on the
    wire. Callers needing sub-microsecond precision must store the
    raw text via a non-ISO8601 ``TEXT`` cell and parse client-side.

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

    Subsecond precision is structurally absent at this wire layer
    (UNIXTIME is integer seconds — see ``ValueType.UNIXTIME`` in
    ``dqlitewire.constants``). Callers needing microsecond precision
    should ensure the server emits ISO8601 (TEXT-storage DATETIME)
    instead.

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
_ADAPTERS: dict[type, Callable[[Any], Any]] = {}


class PrepareProtocol:
    """Stdlib ``sqlite3.PrepareProtocol`` parity sentinel.

    A value's ``__conform__(self, protocol)`` hook is consulted with
    this class as the second argument when ``_convert_bind_param``
    cannot find a matching ``register_adapter`` entry. Mirrors the
    stdlib lookup order: explicit registration first, then the value's
    own ``__conform__``. The class itself carries no behaviour; it is
    used purely as the protocol identity object.
    """

    pass


def register_adapter(type_: type, adapter: Callable[[Any], Any]) -> None:
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
    ``Cursor.row_factory`` (or inherit from
    ``Connection.row_factory``) or post-fetch coercion in user code.

    **Scope: process-global.** Adapters live in a single module-
    level dict shared by every sync and async connection in the
    process — registering on either ``dqlitedbapi`` or
    ``dqlitedbapi.aio`` mutates the same dict. This matches stdlib
    ``sqlite3.register_adapter`` pre-3.12 semantics. psycopg3-style
    per-connection scoping (``Connection.adapters``) is NOT
    supported: registering here affects every connection in the
    process. Tests that register adapters should clean up via
    :func:`unregister_adapter`.
    """
    # PEP 249 §3 / §7: errors raised by the module subclass ``Error``
    # so cross-driver code's ``except dbapi.Error:`` catches uniformly.
    # Stdlib's ``sqlite3.register_adapter`` accepts anything silently;
    # this driver tightens validation but keeps the rejection inside
    # the PEP 249 hierarchy via ``ProgrammingError`` (the canonical
    # class for "wrong argument shape").
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


_BUILT_IN_ADAPTER_TYPES: frozenset[type] = frozenset(
    {datetime.date, datetime.datetime, datetime.time}
)


def unregister_adapter(type_: type) -> None:
    """Remove a previously-registered adapter for ``type_``.

    No-op if no adapter was registered for ``type_``. Counterpart to
    :func:`register_adapter`. Module-scoped, mirroring the registration
    side. Safe to call from test cleanup helpers (``finally:`` blocks,
    ``pytest`` fixtures) without first checking whether the type is in
    the registry.

    ``datetime.date`` / ``datetime.datetime`` / ``datetime.time`` are
    rejected with ``ProgrammingError``: their ISO 8601 encoding is
    hardcoded inside ``_convert_bind_param`` rather than going through
    the ``_ADAPTERS`` registry, so a silent ``_ADAPTERS.pop()`` no-op
    would mislead the caller into thinking the default was uninstalled.
    Use :func:`register_adapter` to override the built-in encoding
    instead.
    """
    if type_ in _BUILT_IN_ADAPTER_TYPES:
        from dqlitedbapi.exceptions import ProgrammingError

        raise ProgrammingError(
            f"cannot unregister built-in adapter for {type_.__name__}; "
            f"the default ISO 8601 encoding is hardcoded inside "
            f"_convert_bind_param. Use register_adapter("
            f"{type_.__name__}, custom_fn) to override the default "
            f"instead — register_adapter wins over the hardcoded "
            f"isinstance branch.",
            code=None,
        )
    _ADAPTERS.pop(type_, None)


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

    **Adapter output chains into the built-in datetime arm.** A
    user-registered adapter (or a ``__conform__`` hook) that returns
    a ``datetime.datetime`` / ``datetime.date`` / ``datetime.time``
    is further processed by the built-in ISO 8601 stringifier below.
    Diverges from stdlib ``sqlite3.register_adapter``, which performs
    a single pass and rejects non-primitive adapter output with
    ``ProgrammingError("type 'X' is not supported")``. To match
    stdlib semantics, ensure adapters return a wire primitive
    (``int`` / ``float`` / ``str`` / ``bytes`` / ``None``) directly;
    the dqlite leniency is a deliberate convenience (a
    ``register_adapter(Money, money_to_aware_datetime)`` is the
    natural shape on this driver) but cross-driver code is
    silently driver-specific.

    **datetime / date / time subclasses match via isinstance.** The
    built-in arms below use ``isinstance(value, datetime.datetime |
    datetime.date)`` (not exact-type), so a user-defined
    ``class MyDate(datetime.date): pass`` binds silently via ISO 8601
    encoding. Diverges from stdlib's exact-type adapter lookup, which
    raises ``ProgrammingError`` for subclasses with no exact-type
    adapter registered. To get stdlib-parity rejection, register a
    no-op adapter for the subclass that raises ``DataError``
    explicitly. To get a custom encoding for ``MyDate``, register an
    adapter for the **exact** subclass via
    ``register_adapter(MyDate, ...)`` — adapter lookup at the
    registry is exact-type-keyed (matches stdlib), so a parent-class
    registration does NOT cover subclasses. Two leniencies stack:
    the isinstance built-in arm fires when no exact-type adapter
    matches, then chains the result (the adapter-output chain above)
    if the produced value is itself a datetime/date/time.
    """
    # User-registered adapter takes precedence. ``type(value)`` not
    # isinstance: stdlib's contract is exact-class match (subclasses
    # do not inherit the parent class's adapter unless explicitly
    # registered). This keeps the contract predictable and matches
    # ``sqlite3.register_adapter``.
    adapter = _ADAPTERS.get(type(value))
    if adapter is not None:
        value = adapter(value)
    else:
        # Stdlib parity: fall back to the value's ``__conform__``
        # hook with ``PrepareProtocol`` as the requested protocol
        # (CPython ``Modules/_sqlite/microprotocols.c`` calls
        # ``PyObject_GetAttrString(obj, "__conform__")``, which
        # consults the instance first and then walks the class via
        # the descriptor protocol). Use ``getattr(value, ...)`` so
        # an instance-bound ``__conform__`` is honoured (matches
        # stdlib). ``__conform__`` may legitimately return ``None``
        # to decline; in that case the value is left unchanged so
        # the wire encoder's normal type rejection runs.
        #
        # A raising ``__conform__`` propagates to the caller —
        # matching stdlib `sqlite3.Cursor.execute`'s behaviour:
        #
        #     >>> class Bad:
        #     ...     def __conform__(self, protocol):
        #     ...         raise RuntimeError("boom")
        #     >>> import sqlite3
        #     >>> sqlite3.connect(":memory:").execute("SELECT ?", (Bad(),))
        #     Traceback (most recent call last):
        #       ...
        #     RuntimeError: boom
        #
        # Verified against CPython's
        # ``Modules/_sqlite/microprotocols.c::_pysqlite_microprotocols_adapt``:
        # a NULL return with an exception set returns NULL to the
        # bind-param machinery, which propagates the exception
        # unwrapped. The previous silent-swallow disposition diverged
        # from stdlib AND hid programmer bugs in user-defined
        # ``__conform__`` implementations behind a wire-encode error.
        proto_method = getattr(value, "__conform__", None)
        if proto_method is not None:
            adapted = proto_method(PrepareProtocol)
            if adapted is not None:
                value = adapted
    # ``datetime.datetime`` is a subclass of ``datetime.date`` but not
    # of ``datetime.time``, so the datetime/date check must fire first
    # for datetime inputs. ``datetime.time`` falls through to its own
    # branch.
    if isinstance(value, datetime.datetime | datetime.date):
        return _iso8601_from_datetime(value)
    if isinstance(value, datetime.time):
        return _iso8601_from_time(value)
    return value
