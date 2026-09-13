"""PEP 249 type objects and constructors: comparison, hashing, validators, and ticks/tz policy."""

from __future__ import annotations

import datetime
import itertools
import math
from decimal import Decimal

import pytest

import dqlitedbapi
from dqlitedbapi import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    DataError,
    Date,
    Time,
    Timestamp,
    register_adapter,
    unregister_adapter,
)
from dqlitedbapi.types import (
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
    _validate_ticks,
    adapt_bind_param,
    datetime_from_iso8601,
    datetime_from_unixtime,
    format_utc_offset,
)
from dqlitewire.constants import ValueType


def test_date_constructor_returns_stdlib_date() -> None:
    result = Date(2024, 1, 1)
    assert result == datetime.date(2024, 1, 1)
    assert type(result) is datetime.date


def test_time_constructor_returns_stdlib_time() -> None:
    result = Time(12, 0, 0)
    assert result == datetime.time(12, 0, 0)
    assert type(result) is datetime.time


def test_timestamp_constructor_returns_stdlib_datetime() -> None:
    result = Timestamp(2024, 1, 1, 12, 0, 0)
    assert result == datetime.datetime(2024, 1, 1, 12, 0, 0)
    assert type(result) is datetime.datetime


def test_constructors_are_callable_not_type_singletons() -> None:
    """Date/Time/Timestamp are callable functions, not equal to the DATETIME singleton."""
    assert callable(Date)
    assert callable(Time)
    assert callable(Timestamp)
    assert Date is not DATETIME
    assert Time is not DATETIME
    assert Timestamp is not DATETIME


def test_pep249_type_singletons_are_pairwise_inequal() -> None:
    """Each singleton wraps a distinct value set, so pairwise comparison is False.

    NUMBER and ROWID both include INTEGER but differ in the rest of their sets.
    """
    singletons = (STRING, BINARY, NUMBER, DATETIME, ROWID)
    for a, b in itertools.combinations(singletons, 2):
        assert a != b, (
            f"PEP 249 type singletons must be pairwise inequal; "
            f"{a!r} == {b!r} unexpectedly returned True"
        )


@pytest.mark.parametrize(
    "ctor,args",
    [
        (dqlitedbapi.Date, (2026, 13, 1)),
        (dqlitedbapi.Date, (2026, 1, 32)),
        (dqlitedbapi.Time, (25, 0, 0)),
        (dqlitedbapi.Time, (0, 60, 0)),
        (dqlitedbapi.Timestamp, (2026, 1, 1, 25, 0, 0)),
        (dqlitedbapi.Timestamp, (2026, 13, 1, 0, 0, 0)),
    ],
)
def test_invalid_args_raise_dataerror(ctor: object, args: tuple[object, ...]) -> None:
    with pytest.raises(DataError):
        ctor(*args)  # type: ignore[operator]
    # Must not leak bare ValueError outside dbapi.Error.
    with pytest.raises(dqlitedbapi.Error):
        ctor(*args)  # type: ignore[operator]


def test_valid_args_succeed() -> None:
    assert dqlitedbapi.Date(2026, 5, 5).day == 5
    assert dqlitedbapi.Time(12, 0, 0).hour == 12
    assert dqlitedbapi.Timestamp(2026, 5, 5, 12, 0, 0).year == 2026


def test_non_int_args_also_wrapped() -> None:
    """TypeError from non-int args must also surface as DataError (PEP 249 §7)."""
    with pytest.raises(DataError):
        dqlitedbapi.Date("2026", 5, 5)  # type: ignore[arg-type]


def test_pep249_type_objects_are_final_annotated() -> None:
    import dqlitedbapi.types as T

    annotations = T.__annotations__
    for name in ("STRING", "BINARY", "NUMBER", "DATETIME", "ROWID"):
        ann = annotations.get(name)
        assert ann is not None, (
            f"dqlitedbapi.types.{name} must be Final-annotated; "
            f"workspace discipline mirrors __version__-Final precedent."
        )
        assert "Final[" in str(ann), (
            f"dqlitedbapi.types.{name} annotation is not Final[...] — got {ann!r}"
        )


def test_pep249_type_objects_runtime_values_unchanged() -> None:
    """Final adds no runtime semantics; the type objects still compare as before."""
    import dqlitedbapi.types as T
    from dqlitewire.constants import ValueType

    assert T.STRING == "TEXT"
    assert T.BINARY == ValueType.BLOB
    assert T.NUMBER == ValueType.INTEGER
    assert T.DATETIME == "TIMESTAMP"
    assert T.ROWID == ValueType.INTEGER


class TestDbapiTypesHashable:
    """PEP 249 type objects must hash so SA can memo by type_code."""

    @pytest.mark.parametrize("obj", [STRING, BINARY, NUMBER, DATETIME, ROWID])
    def test_hashable(self, obj: object) -> None:
        h1 = hash(obj)
        h2 = hash(obj)
        assert h1 == h2

    def test_can_be_set_members(self) -> None:
        s = {NUMBER, STRING}
        assert NUMBER in s
        assert STRING in s

    def test_can_be_dict_keys(self) -> None:
        d = {NUMBER: "n", STRING: "s"}
        assert d[NUMBER] == "n"
        assert d[STRING] == "s"

    def test_distinct_singletons_hash_to_different_values(self) -> None:
        assert hash(STRING) != hash(NUMBER)
        assert hash(NUMBER) != hash(ROWID)
        assert hash(BINARY) != hash(DATETIME)


class TestHashEqInvariantRelaxation:
    """Use ``==`` against module-level type objects, not set membership
    against bare wire ints (the hash-eq invariant is relaxed there)."""

    def test_chained_equality_is_the_documented_idiom(self) -> None:
        from dqlitewire.constants import ValueType

        type_code = int(ValueType.TEXT)
        assert type_code == STRING or type_code == NUMBER  # noqa: PLR1714

        type_code = int(ValueType.INTEGER)
        assert type_code == STRING or type_code == NUMBER  # noqa: PLR1714

    def test_bare_int_set_membership_silently_misses_does_not_raise(self) -> None:
        # Bare-int membership silently misses (hash lookup never reaches
        # __eq__) even though ``==`` against a set member holds.
        from dqlitewire.constants import ValueType

        integer_code = int(ValueType.INTEGER)
        assert integer_code == NUMBER
        assert (integer_code in {NUMBER}) is False
        assert (integer_code in {STRING, NUMBER}) is False
        assert NUMBER in {STRING, NUMBER}


class TestDbapiTypesDistinct:
    """Distinct _DBAPIType instances must not collide under eq."""

    def test_string_not_equal_to_number(self) -> None:
        assert STRING != NUMBER

    def test_binary_not_equal_to_string(self) -> None:
        assert BINARY != STRING

    def test_identity_self_equal(self) -> None:
        assert STRING == STRING  # noqa: PLR0124 -- intentional self-eq


class TestStringType:
    def test_equals_sql_type_names(self) -> None:
        assert STRING == "TEXT"
        assert STRING == "varchar"
        assert STRING == "CLOB"

    def test_equals_wire_text_value_type(self) -> None:
        assert STRING == ValueType.TEXT
        assert int(ValueType.TEXT) == STRING

    def test_does_not_equal_non_text_wire_types(self) -> None:
        assert STRING != ValueType.INTEGER
        assert STRING != ValueType.BLOB
        assert STRING != ValueType.ISO8601


class TestBinaryType:
    def test_equals_blob_wire_type(self) -> None:
        assert BINARY == ValueType.BLOB
        assert int(ValueType.BLOB) == BINARY
        assert BINARY == "BLOB"


class TestNumberType:
    @pytest.mark.parametrize("vt", [ValueType.INTEGER, ValueType.FLOAT, ValueType.BOOLEAN])
    def test_equals_numeric_wire_types(self, vt: ValueType) -> None:
        assert vt == NUMBER
        assert int(vt) == NUMBER

    def test_does_not_equal_text(self) -> None:
        assert NUMBER != ValueType.TEXT


class TestDatetimeType:
    @pytest.mark.parametrize("vt", [ValueType.ISO8601, ValueType.UNIXTIME])
    def test_equals_datetime_wire_types(self, vt: ValueType) -> None:
        assert vt == DATETIME
        assert int(vt) == DATETIME

    def test_equals_declared_type_names(self) -> None:
        assert DATETIME == "DATETIME"
        assert DATETIME == "DATE"
        assert DATETIME == "timestamp"


class TestRowidType:
    def test_equals_integer_wire_type(self) -> None:
        assert ROWID == ValueType.INTEGER
        assert int(ValueType.INTEGER) == ROWID


class TestHashability:
    def test_types_are_hashable_by_name(self) -> None:
        # Hashable (by ``_name``) because SQLAlchemy memoises type_codes as
        # dict keys; the hash-eq invariant vs bare wire ints is relaxed.
        for obj in (STRING, BINARY, NUMBER, DATETIME, ROWID):
            assert isinstance(hash(obj), int)


# A future intentionally-exempt ValueType goes here; do NOT broaden a DBAPI
# type-object to silence the test or columns get mis-classified for user code.
EXEMPT_VALUE_TYPES: frozenset[ValueType] = frozenset({ValueType.NULL})

DBAPI_TYPE_OBJECTS = (STRING, BINARY, NUMBER, DATETIME, ROWID)


class TestValueTypeMappingExhaustiveness:
    """Every wire ``ValueType`` except ``NULL`` is covered by at least one
    DBAPI type object, so no column the wire produces compares to nothing."""

    @pytest.mark.parametrize(
        "value_type",
        [v for v in ValueType if v not in EXEMPT_VALUE_TYPES],
        ids=lambda v: f"{v.name}_{int(v)}",
    )
    def test_every_non_exempt_value_type_has_dbapi_type_coverage(
        self, value_type: ValueType
    ) -> None:
        matched = [t for t in DBAPI_TYPE_OBJECTS if t == value_type]
        assert matched, (
            f"ValueType.{value_type.name} ({int(value_type)}) is not covered "
            f"by any DBAPI type object. Add it to the appropriate "
            f"_DBAPIType(...) call in dqlitedbapi/types.py per the PEP 249 "
            f"§6.1.2 type_code contract."
        )

    def test_null_value_type_is_intentionally_exempt(self) -> None:
        """NULL is Python ``None`` per PEP 249; no type object covers it."""
        for t in DBAPI_TYPE_OBJECTS:
            assert t != ValueType.NULL, (
                f"DBAPI type object {t!r} unexpectedly compares equal to "
                f"ValueType.NULL — NULL has no DBAPI type-object per PEP 249."
            )


class TestDBAPITypeEqFallthrough:
    """``__eq__`` returns ``NotImplemented`` (not ``False``) for unrelated types
    so reflected comparison runs; the bool guard stops ``NUMBER == True``."""

    @pytest.mark.parametrize("other", [None, [], {}, (), object(), 1.5, {1, 2}])
    @pytest.mark.parametrize("type_obj", DBAPI_TYPE_OBJECTS)
    def test_not_equal_to_unrelated_types(self, type_obj: object, other: object) -> None:
        assert type_obj != other
        assert other != type_obj

    @pytest.mark.parametrize("type_obj", DBAPI_TYPE_OBJECTS)
    @pytest.mark.parametrize("value", [True, False])
    def test_not_equal_to_bool_even_if_integer_match(self, type_obj: object, value: bool) -> None:
        # bool subclasses int; without the guard ``NUMBER == True`` would be True.
        assert type_obj != value
        assert value != type_obj

    def test_types_are_hashable_by_singleton_name(self) -> None:
        # Hashes by ``_name``; the hash-eq invariant vs bare wire ints is relaxed
        # (``NUMBER == FLOAT_CODE`` but a dict lookup by FLOAT_CODE misses).
        assert isinstance(hash(STRING), int)
        assert isinstance(hash(NUMBER), int)
        assert hash(STRING) != hash(NUMBER)


class TestDBAPITypeAcceptRejectMatrix:
    """Case-insensitive name matching is intentional, but typos must not match;
    pin both halves so a comparator tweak can't silently break either."""

    @pytest.mark.parametrize(
        ("type_obj", "accepts", "rejects"),
        [
            (
                DATETIME,
                [
                    "DATETIME",
                    "datetime",
                    "DateTime",
                    "DATE",
                    "TIME",
                    "TIMESTAMP",
                    ValueType.ISO8601,
                    ValueType.UNIXTIME,
                    int(ValueType.ISO8601),
                ],
                ["", "DATETIEM", "datetimes", "TIMESTAMPS", 99, "INTEGER"],
            ),
            (
                STRING,
                ["TEXT", "varchar", "CHAR", "CLOB", ValueType.TEXT],
                ["", "TXT", "STRINGS", 99, "INTEGER"],
            ),
            (
                BINARY,
                ["BLOB", "blob", "BINARY", "VARBINARY", ValueType.BLOB],
                ["", "BIN", "BLOBS", 99],
            ),
            (
                NUMBER,
                [
                    "INTEGER",
                    "INT",
                    "REAL",
                    "FLOAT",
                    ValueType.INTEGER,
                    ValueType.FLOAT,
                    ValueType.BOOLEAN,
                ],
                ["", "NUMERICS", "INTGER", 99],
            ),
            (
                ROWID,
                ["ROWID", "rowid", "INTEGER PRIMARY KEY", ValueType.INTEGER],
                ["", "ROWIDS", 99],
            ),
        ],
    )
    def test_dbapi_type_singleton_accept_reject_matrix(
        self,
        type_obj: object,
        accepts: list[object],
        rejects: list[object],
    ) -> None:
        for v in accepts:
            assert type_obj == v, f"{type_obj!r} should accept {v!r} but does not"
        for v in rejects:
            assert type_obj != v, f"{type_obj!r} should reject {v!r} but accepted it"


def test_rowid_and_number_both_match_integer_column() -> None:
    type_code = int(ValueType.INTEGER)
    assert type_code == NUMBER
    assert type_code == ROWID
    assert NUMBER != ROWID  # sentinels remain distinct objects despite overlap


def test_rowid_does_not_match_float_or_text() -> None:
    assert int(ValueType.FLOAT) != ROWID
    assert int(ValueType.TEXT) != ROWID


def test_number_matches_float_and_boolean_rowid_does_not() -> None:
    # NUMBER spans INTEGER + FLOAT + BOOLEAN; ROWID is INTEGER-only.
    assert int(ValueType.FLOAT) == NUMBER
    assert int(ValueType.BOOLEAN) == NUMBER
    assert int(ValueType.FLOAT) != ROWID
    assert int(ValueType.BOOLEAN) != ROWID


def test_string_sentinel_does_not_match_iso8601_wire_code_pin() -> None:
    iso = int(ValueType.ISO8601)
    assert iso != STRING
    assert iso == DATETIME


def test_string_decltype_strings_independent_of_wire_codes_pin() -> None:
    for decl in ("TEXT", "VARCHAR", "CHAR", "CLOB", "text", "varchar"):
        assert decl == STRING, f"{decl} should match STRING"


def test_string_does_not_match_blob_or_integer_codes_pin() -> None:
    for vt in (ValueType.BLOB, ValueType.INTEGER, ValueType.FLOAT):
        assert int(vt) != STRING


def test_iso8601_empty_string_decodes_to_none_silent_null_collision_pin() -> None:
    assert datetime_from_iso8601("") is None


def test_iso8601_non_empty_invalid_string_raises_data_error() -> None:
    from dqlitedbapi import DataError

    with pytest.raises(DataError):
        datetime_from_iso8601("not-a-date")


def test_adapter_returning_datetime_chains_through_iso8601_pin() -> None:
    """Adapter returning datetime is ISO-stringified (stdlib raises instead)."""

    class M:
        pass

    register_adapter(M, lambda _m: datetime.datetime(2024, 1, 1, 12, 0, 0))
    try:
        result = adapt_bind_param(M())
        assert result == "2024-01-01 12:00:00"
    finally:
        unregister_adapter(M)


def test_adapter_returning_time_chains_through_iso8601_pin() -> None:
    class T:
        pass

    register_adapter(T, lambda _t: datetime.time(12, 0, 0))
    try:
        assert adapt_bind_param(T()) == "12:00:00"
    finally:
        unregister_adapter(T)


def test_adapter_returning_date_chains_through_iso8601_pin() -> None:
    class D:
        pass

    register_adapter(D, lambda _d: datetime.date(2024, 1, 1))
    try:
        assert adapt_bind_param(D()) == "2024-01-01"
    finally:
        unregister_adapter(D)


def test_datetime_date_subclass_isinstance_match_diverges_from_stdlib_exact_type_pin() -> None:
    """date subclasses bind via the isinstance arm (stdlib uses exact-type lookup)."""

    class MyDate(datetime.date):
        pass

    assert adapt_bind_param(MyDate(2024, 1, 1)) == "2024-01-01"


def test_datetime_datetime_subclass_isinstance_match_pin() -> None:
    class MyDateTime(datetime.datetime):
        pass

    result = adapt_bind_param(MyDateTime(2024, 1, 1, 12, 0, 0))
    assert result == "2024-01-01 12:00:00"


def test_datetime_time_subclass_isinstance_match_pin() -> None:
    class MyTime(datetime.time):
        pass

    assert adapt_bind_param(MyTime(12, 0, 0)) == "12:00:00"


def test_register_adapter_for_parent_does_not_fire_for_subclass_pin() -> None:
    """Registry is exact-type-keyed: registering Base does not cover Child(Base)."""

    class Base:
        def __init__(self, n: int) -> None:
            self.n = n

    class Child(Base):
        pass

    register_adapter(Base, lambda b: f"custom:{b.n}")
    try:
        assert adapt_bind_param(Base(5)) == "custom:5"
        # Child misses the exact-type lookup; the wire-primitive guard rejects it
        # as an unsupported type (ProgrammingError, matching stdlib sqlite3).
        import pytest

        from dqlitedbapi.exceptions import ProgrammingError

        with pytest.raises(ProgrammingError, match="Child"):
            adapt_bind_param(Child(5))
    finally:
        unregister_adapter(Base)


def test_register_adapter_for_exact_subclass_takes_precedence_over_isinstance_arm_pin() -> None:
    """Exact-type registration for a subclass beats the built-in isinstance arm."""

    class MyDate(datetime.date):
        pass

    register_adapter(MyDate, lambda d: f"custom:{d.isoformat()}")
    try:
        result = adapt_bind_param(MyDate(2024, 1, 1))
        assert result == "custom:2024-01-01"
    finally:
        unregister_adapter(MyDate)


class TestFromTicksOverflowWrapping:
    def test_time_from_ticks_overflow_wraps_as_data_error(self) -> None:
        """Out-of-range tick: fromtimestamp's error wraps as ``DataError``."""
        with pytest.raises(DataError) as excinfo:
            TimeFromTicks(1e30)
        assert "Invalid timestamp ticks" in str(excinfo.value)

    def test_timestamp_from_ticks_overflow_wraps_as_data_error(self) -> None:
        with pytest.raises(DataError) as excinfo:
            TimestampFromTicks(1e30)
        assert "Invalid timestamp ticks" in str(excinfo.value)


class _LyingTzinfo(datetime.tzinfo):
    """``utcoffset`` returns an arbitrary offset, bypassing CPython's
    ``timezone()`` validation so ``format_utc_offset`` branches are reachable."""

    def __init__(self, offset: datetime.timedelta) -> None:
        self._offset = offset

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        return self._offset

    def tzname(self, dt: datetime.datetime | None) -> str:
        return "_LyingTzinfo"

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        return None


class TestFormatUtcOffsetRejection:
    def test_rejects_24h_or_greater_offset(self) -> None:
        """``|offset| >= 24h`` would emit a token ``fromisoformat`` rejects."""
        with pytest.raises(DataError) as excinfo:
            format_utc_offset(datetime.timedelta(hours=25))
        assert "tzinfo offset out of range" in str(excinfo.value)

    def test_rejects_negative_24h_offset(self) -> None:
        with pytest.raises(DataError) as excinfo:
            format_utc_offset(datetime.timedelta(hours=-25))
        assert "tzinfo offset out of range" in str(excinfo.value)

    def test_rejects_subsecond_precision_offset(self) -> None:
        """Sub-second offsets rejected: the wire is whole-second and truncation
        flips sign on negative fractional offsets."""
        with pytest.raises(DataError) as excinfo:
            format_utc_offset(datetime.timedelta(seconds=10, microseconds=500_000))
        assert "sub-second precision" in str(excinfo.value)


class TestDatetimeFromIso8601Edges:
    def test_empty_text_returns_none(self) -> None:
        """Empty ISO 8601 → ``None`` (pre-null-patch servers emit empty for NULL)."""
        assert datetime_from_iso8601("") is None

    def test_date_only_returns_datetime(self) -> None:
        """Bare ``YYYY-MM-DD`` widens to ``datetime`` regardless of parser path."""
        result = datetime_from_iso8601("2024-01-15")
        assert isinstance(result, datetime.datetime)
        assert result == datetime.datetime(2024, 1, 15, 0, 0, 0)

    def test_malformed_text_wraps_as_data_error(self) -> None:
        """Malformed ISO raises ``DataError``, not a raw ``ValueError``."""
        with pytest.raises(DataError) as excinfo:
            datetime_from_iso8601("not-a-date")
        assert "Cannot parse ISO 8601 datetime" in str(excinfo.value)


def test_timestamp_from_ticks_is_naive_local_time() -> None:
    """``TimestampFromTicks`` returns a naive datetime, as local time."""
    ticks = 1700000000  # 2023-11-14 22:13:20 UTC
    result = TimestampFromTicks(ticks)
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is None
    assert result == datetime.datetime.fromtimestamp(ticks)


def test_date_from_ticks_is_naive_local_date() -> None:
    ticks = 1700000000
    result = DateFromTicks(ticks)
    assert isinstance(result, datetime.date)
    assert not isinstance(result, datetime.datetime)  # narrower type
    assert result == datetime.date.fromtimestamp(ticks)


def test_time_from_ticks_is_naive_local_time() -> None:
    ticks = 1700000000
    result = TimeFromTicks(ticks)
    assert isinstance(result, datetime.time)
    assert result.tzinfo is None
    assert result == datetime.datetime.fromtimestamp(ticks).time()


class TestValidateTicksRejectsBool:
    def test_timestamp_from_ticks_rejects_true(self) -> None:
        with pytest.raises(DataError, match="bool"):
            TimestampFromTicks(True)

    def test_timestamp_from_ticks_rejects_false(self) -> None:
        with pytest.raises(DataError, match="bool"):
            TimestampFromTicks(False)

    def test_date_from_ticks_rejects_bool(self) -> None:
        with pytest.raises(DataError, match="bool"):
            DateFromTicks(True)

    def test_time_from_ticks_rejects_bool(self) -> None:
        with pytest.raises(DataError, match="bool"):
            TimeFromTicks(True)


class TestValidateTicksRejectsDecimalNan:
    def test_timestamp_from_ticks_rejects_decimal_nan(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(Decimal("NaN"))  # type: ignore[arg-type]

    def test_timestamp_from_ticks_rejects_decimal_inf(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(Decimal("Infinity"))  # type: ignore[arg-type]


class TestValidateTicksRejectsFloatNanInf:
    def test_rejects_nan(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(math.nan)

    def test_rejects_inf(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(math.inf)


class TestValidateTicksRejectsNonNumeric:
    def test_rejects_str(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks("1700000000")  # type: ignore[arg-type]

    def test_rejects_none(self) -> None:
        with pytest.raises(DataError):
            TimestampFromTicks(None)  # type: ignore[arg-type]


class TestValidateTicksHappyPath:
    def test_int_accepted(self) -> None:
        assert TimestampFromTicks(1700000000).year == 2023

    def test_float_accepted_preserving_fraction(self) -> None:
        assert TimestampFromTicks(1700000000.5).microsecond == 500_000

    def test_decimal_finite_accepted(self) -> None:
        result = TimestampFromTicks(Decimal("1700000000"))  # type: ignore[arg-type]
        assert result.year == 2023


def test_validate_ticks_rejects_python_bool() -> None:
    with pytest.raises(DataError, match="bool"):
        _validate_ticks(True)
    with pytest.raises(DataError, match="bool"):
        _validate_ticks(False)


def test_validate_ticks_rejects_non_numeric() -> None:
    """Reject a non-int/float even if its ``__float__`` yields a finite value."""

    class FakeBool:
        """Mimics numpy.bool_: not a bool subclass, but ``float()`` yields 1.0."""

        def __float__(self) -> float:
            return 1.0

    with pytest.raises(DataError):
        _validate_ticks(FakeBool())  # type: ignore[arg-type]


def test_validate_ticks_accepts_int_and_float() -> None:
    assert _validate_ticks(0) == 0.0
    assert _validate_ticks(1234567890) == 1234567890.0
    assert _validate_ticks(1.5) == 1.5
    assert _validate_ticks(-1.5) == -1.5


def test_validate_ticks_wraps_overflow_from_float_coercion() -> None:
    """An ``OverflowError`` from ``__float__`` surfaces as ``DataError``, not a
    bare ``OverflowError``. Defensive: today CPython saturates to ``inf``."""
    from decimal import Decimal

    class RaisingDecimal(Decimal):
        """Decimal lineage passes the isinstance guard; ``__float__`` overflows."""

        def __float__(self) -> float:
            raise OverflowError("custom overflow from __float__")

    with pytest.raises(DataError, match="overflow"):
        _validate_ticks(RaisingDecimal("1"))  # type: ignore[arg-type]


def test_datetime_from_unixtime_rejects_bool() -> None:
    """Bool slipped past the range check and produced epoch-based datetimes."""
    with pytest.raises(DataError, match="bool"):
        datetime_from_unixtime(True)
    with pytest.raises(DataError, match="bool"):
        datetime_from_unixtime(False)


def test_datetime_from_unixtime_rejects_non_int() -> None:
    with pytest.raises(DataError, match="must be int"):
        datetime_from_unixtime(1.5)  # type: ignore[arg-type]
    with pytest.raises(DataError, match="must be int"):
        datetime_from_unixtime("1234")  # type: ignore[arg-type]


def test_datetime_from_unixtime_accepts_int_in_range() -> None:
    dt = datetime_from_unixtime(0)
    assert dt.year == 1970
    dt = datetime_from_unixtime(1700000000)
    assert dt.year == 2023
