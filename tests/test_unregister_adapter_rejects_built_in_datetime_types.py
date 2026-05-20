"""Pin: ``unregister_adapter`` raises ``ProgrammingError`` when
called on a built-in datetime type whose default encoding is
hardcoded inside ``_convert_bind_param``.

The previous silent ``_ADAPTERS.pop(type_, None)`` no-op misled
callers: ``unregister_adapter(datetime.date)`` returned cleanly but
the next ``execute("SELECT ?", (datetime.date.today(),))`` still
emitted the ISO 8601 string because the hardcoded ``isinstance``
branch in ``_convert_bind_param`` runs after the (now-empty)
registry lookup.

Reject explicitly. Document the override pattern (``register_adapter``
wins over the hardcoded branch — that's how stdlib parity works on
this driver).
"""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.exceptions import ProgrammingError
from dqlitedbapi.types import (
    _BUILT_IN_ADAPTER_TYPES,
    _convert_bind_param,
    register_adapter,
    unregister_adapter,
)


@pytest.mark.parametrize("type_", [datetime.date, datetime.datetime, datetime.time])
def test_unregister_adapter_rejects_built_in_datetime_types(type_: type) -> None:
    """Calling ``unregister_adapter`` on a built-in datetime type
    must raise ``ProgrammingError`` rather than silently no-op."""
    with pytest.raises(ProgrammingError, match="built-in adapter"):
        unregister_adapter(type_)


def test_built_in_adapter_types_frozenset_lists_three_classes() -> None:
    """The rejection set covers date / datetime / time and nothing
    else."""
    expected = frozenset({datetime.date, datetime.datetime, datetime.time})
    assert expected == _BUILT_IN_ADAPTER_TYPES  # noqa: SIM300


def test_register_adapter_override_still_wins() -> None:
    """Pin the override discipline named in the rejection diagnostic:
    ``register_adapter(datetime.date, custom_fn)`` takes precedence
    over the hardcoded ISO 8601 branch."""

    def custom(value: datetime.date) -> str:
        return f"CUSTOM({value.year})"

    register_adapter(datetime.date, custom)
    try:
        out = _convert_bind_param(datetime.date(2024, 1, 1))
        assert out == "CUSTOM(2024)"
    finally:
        # Clean up by directly popping from the registry — bypasses
        # the unregister_adapter rejection because this is test
        # cleanup, not a user contract violation.
        from dqlitedbapi.types import _ADAPTERS

        _ADAPTERS.pop(datetime.date, None)


def test_unregister_adapter_on_user_type_still_silently_noops() -> None:
    """Negative twin: an unregistered user type still no-ops
    silently (the contract for non-built-in types is unchanged)."""

    class MyType:
        pass

    # No raise — silent no-op.
    unregister_adapter(MyType)
