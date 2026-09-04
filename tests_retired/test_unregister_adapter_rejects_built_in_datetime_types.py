"""``unregister_adapter`` removes a user override on a built-in datetime type,
and raises ``ProgrammingError`` when no adapter is registered."""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.exceptions import ProgrammingError
from dqlitedbapi.types import (
    _convert_bind_param,
    register_adapter,
    unregister_adapter,
)


@pytest.mark.parametrize("type_", [datetime.date, datetime.datetime, datetime.time])
def test_unregister_adapter_raises_when_no_override(type_: type) -> None:
    """Built-in type without a prior override: ``ProgrammingError``."""
    with pytest.raises(ProgrammingError, match="no adapter registered"):
        unregister_adapter(type_)


def test_unregister_adapter_removes_user_override_for_builtin_type() -> None:
    """register installs an override on a built-in type; unregister restores it."""

    def custom(value: datetime.date) -> str:
        return f"CUSTOM({value.year})"

    register_adapter(datetime.date, custom)
    try:
        # Override wins over the hardcoded ISO branch.
        out = _convert_bind_param(datetime.date(2024, 1, 1))
        assert out == "CUSTOM(2024)"
    finally:
        unregister_adapter(datetime.date)

    out = _convert_bind_param(datetime.date(2024, 1, 1))
    assert out == "2024-01-01"


def test_unregister_adapter_raises_on_unknown_user_type() -> None:
    """User type with no prior override raises ``ProgrammingError``."""

    class MyType:
        pass

    with pytest.raises(ProgrammingError, match="no adapter registered"):
        unregister_adapter(MyType)


def test_unregister_adapter_round_trip_for_user_type() -> None:
    """register / unregister a user type round-trips cleanly."""

    class MyType:
        pass

    def custom(value: MyType) -> str:
        return "MY"

    register_adapter(MyType, custom)
    unregister_adapter(MyType)
    with pytest.raises(ProgrammingError, match="no adapter registered"):
        unregister_adapter(MyType)
