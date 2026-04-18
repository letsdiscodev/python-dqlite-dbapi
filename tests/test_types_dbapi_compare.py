"""PEP 249 type-object comparison tests (ISSUE-06).

Per PEP 249 the module's type objects (STRING, BINARY, NUMBER, DATETIME,
ROWID) must compare equal to whatever `cursor.description[i][1]` carries.
We carry the wire `ValueType` integer there, so the type objects have to
compare equal to the integer code too — not just to uppercase SQL type
name strings.
"""

import pytest
from dqlitewire.constants import ValueType

from dqlitedbapi import BINARY, DATETIME, NUMBER, ROWID, STRING


class TestStringType:
    def test_equals_sql_type_names(self) -> None:
        assert STRING == "TEXT"
        assert STRING == "varchar"
        assert STRING == "CLOB"

    def test_equals_wire_text_value_type(self) -> None:
        assert STRING == ValueType.TEXT
        assert STRING == int(ValueType.TEXT)

    def test_does_not_equal_non_text_wire_types(self) -> None:
        assert not (STRING == ValueType.INTEGER)
        assert not (STRING == ValueType.BLOB)
        assert not (STRING == ValueType.ISO8601)


class TestBinaryType:
    def test_equals_blob_wire_type(self) -> None:
        assert BINARY == ValueType.BLOB
        assert BINARY == int(ValueType.BLOB)
        assert BINARY == "BLOB"


class TestNumberType:
    @pytest.mark.parametrize("vt", [ValueType.INTEGER, ValueType.FLOAT, ValueType.BOOLEAN])
    def test_equals_numeric_wire_types(self, vt: ValueType) -> None:
        assert NUMBER == vt
        assert NUMBER == int(vt)

    def test_does_not_equal_text(self) -> None:
        assert not (NUMBER == ValueType.TEXT)


class TestDatetimeType:
    @pytest.mark.parametrize("vt", [ValueType.ISO8601, ValueType.UNIXTIME])
    def test_equals_datetime_wire_types(self, vt: ValueType) -> None:
        assert DATETIME == vt
        assert DATETIME == int(vt)

    def test_equals_declared_type_names(self) -> None:
        assert DATETIME == "DATETIME"
        assert DATETIME == "DATE"
        assert DATETIME == "timestamp"


class TestRowidType:
    def test_equals_integer_wire_type(self) -> None:
        assert ROWID == ValueType.INTEGER
        assert ROWID == int(ValueType.INTEGER)


class TestHashability:
    def test_types_still_hashable_after_int_mix(self) -> None:
        # Mixed str/int contents mustn't break dict-key or set-member use.
        types_set = {STRING, BINARY, NUMBER, DATETIME, ROWID}
        assert len(types_set) == 5
        d = {STRING: "s", NUMBER: "n"}
        assert d[STRING] == "s"
