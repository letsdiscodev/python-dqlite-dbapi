"""Pin the documented asymmetries between dqlite's type-coercion
surface and stdlib ``sqlite3``.

Four documented divergences are pinned here as deliberate behaviour
so a future refactor cannot quietly regress them:

1. ``STRING`` excludes ``ValueType.ISO8601`` even though the wire
   cell is text-encoded — the dbapi layer converts to datetime so
   the post-fetch value type aligns with ``DATETIME``.
2. ``_datetime_from_iso8601("")`` returns ``None`` (silent collision
   with wire NULL on ISO8601-tagged columns).
3. ``register_adapter`` output that is itself a datetime/date/time
   chains through the built-in ISO 8601 stringifier; stdlib raises
   ``ProgrammingError`` on the same input.
4. ``datetime.date`` / ``datetime.datetime`` / ``datetime.time``
   subclasses bind silently via the built-in isinstance arm; stdlib
   rejects subclasses with no exact-type adapter.

If a future change tightens any of these, delete the corresponding
pin AND update the docstring in types.py.
"""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi import DATETIME, STRING, register_adapter, unregister_adapter
from dqlitedbapi.types import _convert_bind_param, _datetime_from_iso8601
from dqlitewire.constants import ValueType

# --- STRING vs ISO8601 partition --------------------------------------


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


# --- empty ISO8601 cell silent NULL collision -------------------------


def test_iso8601_empty_string_decodes_to_none_silent_null_collision_pin() -> None:
    assert _datetime_from_iso8601("") is None


def test_iso8601_non_empty_invalid_string_raises_data_error() -> None:
    from dqlitedbapi import DataError

    with pytest.raises(DataError):
        _datetime_from_iso8601("not-a-date")


# --- adapter output chains into datetime handler ---------------------


def test_adapter_returning_datetime_chains_through_iso8601_pin() -> None:
    """Pin: adapter returning datetime is ISO-stringified by the built-in
    handler. Diverges from stdlib (which raises ProgrammingError). If
    this changes to stdlib parity, delete this pin AND remove the
    documentation note in _convert_bind_param."""

    class M:
        pass

    register_adapter(M, lambda _m: datetime.datetime(2024, 1, 1, 12, 0, 0))
    try:
        result = _convert_bind_param(M())
        assert result == "2024-01-01 12:00:00"
    finally:
        unregister_adapter(M)


def test_adapter_returning_time_chains_through_iso8601_pin() -> None:
    class T:
        pass

    register_adapter(T, lambda _t: datetime.time(12, 0, 0))
    try:
        assert _convert_bind_param(T()) == "12:00:00"
    finally:
        unregister_adapter(T)


def test_adapter_returning_date_chains_through_iso8601_pin() -> None:
    class D:
        pass

    register_adapter(D, lambda _d: datetime.date(2024, 1, 1))
    try:
        # date widens through _iso8601_from_datetime
        assert _convert_bind_param(D()) == "2024-01-01"
    finally:
        unregister_adapter(D)


# --- datetime subclass isinstance acceptance --------------------------


def test_datetime_date_subclass_isinstance_match_diverges_from_stdlib_exact_type_pin() -> None:
    """Pin: subclasses of datetime.date bind silently via the isinstance
    arm. Diverges from stdlib's exact-type adapter lookup. See
    types.py:_convert_bind_param docstring."""

    class MyDate(datetime.date):
        pass

    assert _convert_bind_param(MyDate(2024, 1, 1)) == "2024-01-01"


def test_datetime_datetime_subclass_isinstance_match_pin() -> None:
    class MyDateTime(datetime.datetime):
        pass

    result = _convert_bind_param(MyDateTime(2024, 1, 1, 12, 0, 0))
    assert result == "2024-01-01 12:00:00"


def test_datetime_time_subclass_isinstance_match_pin() -> None:
    class MyTime(datetime.time):
        pass

    assert _convert_bind_param(MyTime(12, 0, 0)) == "12:00:00"


def test_register_adapter_for_parent_does_not_fire_for_subclass_pin() -> None:
    """The registry is exact-type-keyed (matches stdlib); registering for
    a parent class (``Base``) does NOT cover ``Child(Base)`` — the
    registry lookup at ``type(value)`` misses, ``__conform__`` is
    absent, and the value falls through to the wire encoder
    unchanged."""

    class Base:
        def __init__(self, n: int) -> None:
            self.n = n

    class Child(Base):
        pass

    register_adapter(Base, lambda b: f"custom:{b.n}")
    try:
        # Base instance — registry hit
        assert _convert_bind_param(Base(5)) == "custom:5"
        # Child instance — registry MISS (exact-type lookup), no
        # __conform__, no datetime isinstance match. The post-chain
        # wire-primitive guard rejects the non-primitive with
        # ``ProgrammingError`` (stdlib parity).
        import pytest

        from dqlitedbapi.exceptions import DataError

        with pytest.raises(DataError, match="Child"):
            _convert_bind_param(Child(5))
    finally:
        unregister_adapter(Base)


def test_register_adapter_for_exact_subclass_takes_precedence_over_isinstance_arm_pin() -> None:
    """Exact-type registration for the subclass beats the built-in
    isinstance arm — this is the recommended pattern for custom subclass
    encoding."""

    class MyDate(datetime.date):
        pass

    register_adapter(MyDate, lambda d: f"custom:{d.isoformat()}")
    try:
        result = _convert_bind_param(MyDate(2024, 1, 1))
        assert result == "custom:2024-01-01"
    finally:
        unregister_adapter(MyDate)
