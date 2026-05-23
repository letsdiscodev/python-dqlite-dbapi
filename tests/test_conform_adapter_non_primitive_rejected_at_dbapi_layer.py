"""Pin: ``_convert_bind_param`` rejects non-primitive adapter /
``__conform__`` output at the dbapi layer with ``ProgrammingError``
naming the original input type, mirroring stdlib's microprotocols-
layer rejection.

Before this fix, a non-primitive value (e.g. a ``register_adapter``
returning a custom class instance, or a value with no adapter and
no ``__conform__``) flowed through to the wire encoder where it
surfaced as ``EncodeError`` → ``DataError`` naming the post-chain
type. The new gate names the original input type AND the
adapter-produced type so operators can locate the misregistration
site.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import _convert_bind_param


def test_adapter_returning_non_primitive_rejected_with_original_type_in_message() -> None:
    """``register_adapter(MyClass, lambda x: object())`` produces a
    non-primitive at the adapter site; the post-chain guard rejects
    it with a message naming MyClass (so the operator can find the
    registration), AND naming the produced type."""

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
    """Sibling positive: an adapter returning a wire primitive passes
    through the post-chain guard cleanly."""

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
