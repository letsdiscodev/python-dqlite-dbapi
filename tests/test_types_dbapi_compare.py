"""PEP 249 type-object comparison tests.

Per PEP 249 the module's type objects (STRING, BINARY, NUMBER, DATETIME,
ROWID) must compare equal to whatever `cursor.description[i][1]` carries.
We carry the wire `ValueType` integer there, so the type objects have to
compare equal to the integer code too — not just to uppercase SQL type
name strings.
"""

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
    def test_types_still_hashable_after_int_mix(self) -> None:
        # Mixed str/int contents mustn't break dict-key or set-member use.
        types_set = {STRING, BINARY, NUMBER, DATETIME, ROWID}
        assert len(types_set) == 5
        d = {STRING: "s", NUMBER: "n"}
        assert d[STRING] == "s"


# Maintenance note: if a future ValueType is intentionally exempt
# (e.g. ``ValueType.RESERVED`` for protocol negotiation), add it to
# ``EXEMPT_VALUE_TYPES`` here — do NOT broaden a DBAPI type-object
# just to silence this test, or you will silently mis-classify a
# column for user code.
EXEMPT_VALUE_TYPES: frozenset[ValueType] = frozenset({ValueType.NULL})

DBAPI_TYPE_OBJECTS = (STRING, BINARY, NUMBER, DATETIME, ROWID)


class TestValueTypeMappingExhaustiveness:
    """Pin the cross-package contract: every wire ``ValueType`` (except
    ``NULL``, which PEP 249 represents as ``None``) is covered by at
    least one module-level DBAPI type object so
    ``cursor.description[i][1] == STRING / BINARY / NUMBER / DATETIME /
    ROWID`` comparisons in user code do not silently fall through to
    "none of the above" for any column the wire can produce.

    A regression would surface if either:
    - a new ValueType is added in wire/constants.py without
      corresponding updates to the dbapi type objects, or
    - an existing ValueType is removed from a type object by an
      over-zealous "cleanup".
    """

    @pytest.mark.parametrize(
        "value_type",
        [v for v in ValueType if v not in EXEMPT_VALUE_TYPES],
        ids=lambda v: f"{v.name}_{int(v)}",
    )
    def test_every_non_exempt_value_type_has_dbapi_type_coverage(
        self, value_type: ValueType
    ) -> None:
        """The PEP 249 comparison surface must cover every wire
        ValueType. ``cursor.description[i][1]`` is a ValueType int;
        user code compares it against STRING / BINARY / NUMBER /
        DATETIME / ROWID. At least one of those must compare equal.
        """
        matched = [t for t in DBAPI_TYPE_OBJECTS if t == value_type]
        assert matched, (
            f"ValueType.{value_type.name} ({int(value_type)}) is not covered "
            f"by any DBAPI type object. Add it to the appropriate "
            f"_DBAPIType(...) call in dqlitedbapi/types.py — see ISSUE-395 "
            f"for the contract."
        )

    def test_null_value_type_is_intentionally_exempt(self) -> None:
        """PEP 249 specifies SQL NULL is the Python ``None`` singleton;
        the DBAPI type-object surface intentionally has no entry for
        NULL. A regression that adds NULL to one of the type objects
        would imply the contract changed; this test makes that change
        explicit so it cannot land silently.
        """
        for t in DBAPI_TYPE_OBJECTS:
            assert t != ValueType.NULL, (
                f"DBAPI type object {t!r} unexpectedly compares equal to "
                f"ValueType.NULL — see ISSUE-395 for the exempt-NULL contract"
            )
