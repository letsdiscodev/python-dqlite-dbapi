"""Pin: ``unregister_adapter`` removes a user-installed override for
a built-in datetime type and raises ``ProgrammingError`` when no
adapter is registered.

Before this fix the unconditional rejection of built-in types left
user overrides permanently installed with no public removal path —
``register_adapter(datetime.date, custom)`` was accepted (and the
registry consult won over the hardcoded ISO 8601 branch), but
``unregister_adapter(datetime.date)`` raised even when the user had
explicitly installed an override.

Aligned with stdlib ``sqlite3.unregister_adapter`` (Python 3.13+):
unregister any type that has a registry entry; raise
``ProgrammingError`` for "no adapter to remove."
"""

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
    """Calling ``unregister_adapter`` on a built-in type WITHOUT a
    prior user override raises ``ProgrammingError`` ("no adapter
    registered") — there's nothing to remove."""
    with pytest.raises(ProgrammingError, match="no adapter registered"):
        unregister_adapter(type_)


def test_unregister_adapter_removes_user_override_for_builtin_type() -> None:
    """Pin the symmetric removal path: register_adapter installs a
    user override on a built-in type; unregister_adapter removes it,
    restoring the built-in default."""

    def custom(value: datetime.date) -> str:
        return f"CUSTOM({value.year})"

    register_adapter(datetime.date, custom)
    try:
        # Override wins over the hardcoded ISO branch.
        out = _convert_bind_param(datetime.date(2024, 1, 1))
        assert out == "CUSTOM(2024)"
    finally:
        unregister_adapter(datetime.date)

    # After unregister, the built-in default is restored.
    out = _convert_bind_param(datetime.date(2024, 1, 1))
    assert out == "2024-01-01"


def test_unregister_adapter_raises_on_unknown_user_type() -> None:
    """Negative: a user type with no prior override raises
    ``ProgrammingError`` (matches stdlib's "no adapter to remove"
    cross-driver-portable signal)."""

    class MyType:
        pass

    with pytest.raises(ProgrammingError, match="no adapter registered"):
        unregister_adapter(MyType)


def test_unregister_adapter_round_trip_for_user_type() -> None:
    """Pin: register / unregister a user type works as a clean pair
    (the canonical test-cleanup idiom)."""

    class MyType:
        pass

    def custom(value: MyType) -> str:
        return "MY"

    register_adapter(MyType, custom)
    unregister_adapter(MyType)
    # Second unregister raises — clean removal completed.
    with pytest.raises(ProgrammingError, match="no adapter registered"):
        unregister_adapter(MyType)
