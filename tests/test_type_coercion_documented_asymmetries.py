"""Pin deliberate divergences from stdlib ``sqlite3`` type coercion; if any
is tightened, delete the matching pin AND update the docstring in types.py."""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi import DATETIME, STRING, register_adapter, unregister_adapter
from dqlitedbapi.types import adapt_bind_param, datetime_from_iso8601
from dqlitewire.constants import ValueType


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
