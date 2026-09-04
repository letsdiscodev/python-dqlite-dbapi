"""Pin: ``adapt_bind_param`` honours ``__conform__``/``PrepareProtocol`` discovery
as a fallback after the explicit ``register_adapter`` registry (stdlib lookup order)."""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi import PrepareProtocol
from dqlitedbapi.types import adapt_bind_param, register_adapter, unregister_adapter


def test_prepareprotocol_symbol_exported_from_top_level() -> None:
    assert hasattr(dqlitedbapi, "PrepareProtocol")
    assert dqlitedbapi.PrepareProtocol is PrepareProtocol


def test_prepareprotocol_symbol_exported_from_aio_surface() -> None:
    from dqlitedbapi import aio as aio_mod

    assert hasattr(aio_mod, "PrepareProtocol")
    assert aio_mod.PrepareProtocol is PrepareProtocol


def test_conform_fallback_returns_wire_encodable_value() -> None:
    """A class with ``__conform__`` returning a wire-encodable value
    binds successfully through the fallback path."""

    class Money:
        def __init__(self, value: int) -> None:
            self.value = value

        def __conform__(self, protocol: type) -> object:
            if protocol is PrepareProtocol:
                return f"${self.value}"
            return None

    out = adapt_bind_param(Money(42))
    assert out == "$42"


def test_register_adapter_precedence_over_conform() -> None:
    """An explicit ``register_adapter`` entry must take precedence
    over a class's ``__conform__`` hook (stdlib lookup order)."""

    class Money:
        def __init__(self, value: int) -> None:
            self.value = value

        def __conform__(self, protocol: type) -> object:
            if protocol is PrepareProtocol:
                return "from-conform"
            return None

    register_adapter(Money, lambda m: "from-adapter")
    try:
        out = adapt_bind_param(Money(7))
        assert out == "from-adapter"
    finally:
        unregister_adapter(Money)


def test_conform_returning_none_rejected_at_dbapi_layer() -> None:
    """``__conform__`` returning None leaves the value unchanged; the wire-primitive
    guard then rejects the non-primitive (stdlib parity, microprotocols layer)."""
    import pytest

    from dqlitedbapi.exceptions import ProgrammingError

    class Opaque:
        def __conform__(self, protocol: type) -> object:
            return None

    with pytest.raises(ProgrammingError, match="Opaque"):
        adapt_bind_param(Opaque())


def test_no_conform_method_no_adapter_rejected_at_dbapi_layer() -> None:
    """No adapter, no ``__conform__``, not a wire primitive -> rejected (stdlib parity)."""
    import pytest

    from dqlitedbapi.exceptions import ProgrammingError

    class Plain:
        pass

    with pytest.raises(ProgrammingError, match="Plain"):
        adapt_bind_param(Plain())


def test_instance_level_conform_is_honoured() -> None:
    """Stdlib parity: instance-level ``__conform__`` is honoured (CPython's
    microprotocols.c does ``getattr(obj, "__conform__")``, consulting the instance)."""

    class Plain:
        pass

    obj = Plain()
    obj.__conform__ = lambda protocol: "instance-bound" if protocol is PrepareProtocol else None  # type: ignore[attr-defined]
    out = adapt_bind_param(obj)
    assert out == "instance-bound"


def test_class_level_conform_still_honoured_after_instance_lookup_change() -> None:
    """Class-side ``__conform__`` still works after the lookup moved to
    ``getattr(value, ...)`` — getattr binds the class method via the descriptor protocol."""

    class WithClassConform:
        def __conform__(self, protocol: type) -> object:
            if protocol is PrepareProtocol:
                return "class-bound"
            return None

    out = adapt_bind_param(WithClassConform())
    assert out == "class-bound"


def test_raising_conform_propagates_unwrapped() -> None:
    """A raising ``__conform__`` propagates to the caller (stdlib parity; the prior
    silent-swallow erased the traceback)."""
    import pytest

    class Boom:
        def __conform__(self, protocol: type) -> object:
            raise RuntimeError("conform exploded")

    with pytest.raises(RuntimeError, match="conform exploded"):
        adapt_bind_param(Boom())
