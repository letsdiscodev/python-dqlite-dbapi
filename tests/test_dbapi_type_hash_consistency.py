"""``_DBAPIType.__hash__`` must be consistent with ``__eq__`` for the
canonical ValueType integer — enough to make the PEP 249
``description[i][1]`` dict-dispatch pattern work:

    {STRING: convert_text, NUMBER: convert_num, BINARY: convert_blob}[desc[i][1]]

The hash-eq invariant is NOT fully satisfiable for multi-ValueType
aggregates like ``NUMBER`` (INTEGER + FLOAT + BOOLEAN); those
residual mismatches are a documented limitation.
"""

from __future__ import annotations

from dqlitedbapi.types import BINARY, DATETIME, NUMBER, ROWID, STRING
from dqlitewire.constants import ValueType


class TestCanonicalHashEqConsistency:
    def test_string_in_set_of_canonical_valuetype(self) -> None:
        assert STRING in {int(ValueType.TEXT)}

    def test_binary_in_set_of_canonical_valuetype(self) -> None:
        assert BINARY in {int(ValueType.BLOB)}

    def test_number_in_set_of_integer_canonical(self) -> None:
        # Canonical representative for NUMBER is ValueType.INTEGER (int 1),
        # the smallest wire code in its set.
        assert NUMBER in {int(ValueType.INTEGER)}

    def test_rowid_in_set_of_integer(self) -> None:
        assert ROWID in {int(ValueType.INTEGER)}

    def test_datetime_in_set_of_unixtime_canonical(self) -> None:
        # Canonical representative for DATETIME is ValueType.UNIXTIME
        # (int 9) — the smallest wire code in its set (UNIXTIME=9,
        # ISO8601=10).
        assert DATETIME in {int(ValueType.UNIXTIME)}

    def test_description_dispatch_pattern(self) -> None:
        """The PEP 249 dict-dispatch pattern must work for canonical wire types."""
        converters = {
            STRING: "text_converter",
            NUMBER: "num_converter",
            BINARY: "blob_converter",
        }
        # description[i][1] is an int (ValueType enum value) per dqlitedbapi.
        assert converters[int(ValueType.TEXT)] == "text_converter"
        assert converters[int(ValueType.INTEGER)] == "num_converter"
        assert converters[int(ValueType.BLOB)] == "blob_converter"


class TestDbapiTypesDistinct:
    """Distinct _DBAPIType instances must not collide under eq."""

    def test_string_not_equal_to_number(self) -> None:
        assert STRING != NUMBER

    def test_binary_not_equal_to_string(self) -> None:
        assert BINARY != STRING

    def test_identity_self_equal(self) -> None:
        assert STRING == STRING  # noqa: PLR0124 -- intentional self-eq
