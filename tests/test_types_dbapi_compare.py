"""PEP 249 type objects must compare equal to whatever ``description[i][1]``
carries; we carry the wire ``ValueType`` int, so equality covers ints too."""

import pytest

from dqlitedbapi import BINARY, DATETIME, NUMBER, ROWID, STRING
from dqlitewire.constants import ValueType


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
