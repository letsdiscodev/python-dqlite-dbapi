"""Pin: ``NUMBER`` Type Object includes ``ValueType.BOOLEAN`` (diverges from psycopg2)."""

from __future__ import annotations

from dqlitedbapi import NUMBER
from dqlitewire import ValueType


def test_number_compares_equal_to_boolean_wire_value() -> None:
    assert NUMBER == ValueType.BOOLEAN
