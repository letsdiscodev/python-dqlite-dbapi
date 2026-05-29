"""Pin: ``_convert_bind_param`` rejects non-primitive adapter/``__conform__`` output
at the dbapi layer, naming both the original input type and the produced type so
operators can locate the misregistration site.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import _convert_bind_param


def test_adapter_returning_non_primitive_rejected_with_original_type_in_message() -> None:
    """A non-primitive adapter output is rejected, naming both the input and produced type."""

    class Money:
        def __init__(self, cents: int) -> None:
            self.cents = cents

    class _NotAPrimitive:
        pass

    dqlitedbapi.register_adapter(Money, lambda _m: _NotAPrimitive())
    try:
        with pytest.raises(DataError) as excinfo:
            _convert_bind_param(Money(100))
        assert "Money" in str(excinfo.value), (
            f"diagnostic must name the original input type so the "
            f"operator can locate the adapter registration site; "
            f"got: {excinfo.value!r}"
        )
        assert "_NotAPrimitive" in str(excinfo.value), (
            f"diagnostic must also name the adapter-produced type; got: {excinfo.value!r}"
        )
    finally:
        dqlitedbapi.unregister_adapter(Money)


def test_adapter_returning_wire_primitive_succeeds() -> None:
    """Positive sibling: an adapter returning a wire primitive passes the guard."""

    class Money:
        def __init__(self, cents: int) -> None:
            self.cents = cents

    dqlitedbapi.register_adapter(Money, lambda m: m.cents)
    try:
        assert _convert_bind_param(Money(100)) == 100
    finally:
        dqlitedbapi.unregister_adapter(Money)


@pytest.mark.parametrize(
    "primitive",
    [
        42,
        3.14,
        "hello",
        b"bytes",
        bytearray(b"ba"),
        memoryview(b"mv"),
        True,
        None,
    ],
)
def test_wire_primitives_pass_through(primitive: object) -> None:
    """All wire primitives bypass the post-chain guard."""
    assert _convert_bind_param(primitive) == primitive
