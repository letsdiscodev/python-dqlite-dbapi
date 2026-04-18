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
