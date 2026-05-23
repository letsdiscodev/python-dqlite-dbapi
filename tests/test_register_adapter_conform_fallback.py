"""Pin: ``_convert_bind_param`` honours stdlib's ``__conform__`` /
``PrepareProtocol`` discovery as a fallback after the explicit
``register_adapter`` registry, matching stdlib ``sqlite3``'s lookup
order.

Stdlib ``sqlite3`` documents two parallel adapter-discovery
mechanisms:
1. ``register_adapter(type_, adapter)`` — explicit registry (precedence)
2. ``__conform__(self, protocol)`` on a value, where ``protocol`` is
   ``sqlite3.PrepareProtocol`` — a class-side hook a value can implement
   to opt into binding without explicit registration.

Without this pin a class with ``__conform__`` works under stdlib but
silently fails under dqlite, surfacing only when the wire encoder
rejects the unknown type.
"""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi import PrepareProtocol
from dqlitedbapi.types import _convert_bind_param, register_adapter, unregister_adapter


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

    out = _convert_bind_param(Money(42))
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
        out = _convert_bind_param(Money(7))
        assert out == "from-adapter"
    finally:
        unregister_adapter(Money)


def test_conform_returning_none_rejected_at_dbapi_layer() -> None:
    """If ``__conform__`` returns None for the asked-for protocol the
    value is left unchanged; the post-chain wire-primitive guard
    then rejects the non-primitive with ``ProgrammingError``
    (stdlib parity — non-primitive adapter output rejects at the
    microprotocols layer, not the wire encoder)."""
    import pytest

    from dqlitedbapi.exceptions import DataError

    class Opaque:
        def __conform__(self, protocol: type) -> object:
            return None

    with pytest.raises(DataError, match="Opaque"):
        _convert_bind_param(Opaque())


def test_no_conform_method_no_adapter_rejected_at_dbapi_layer() -> None:
    """A value without ``__conform__`` and no registered adapter
    fails the post-chain wire-primitive guard with
    ``ProgrammingError`` -- stdlib parity for "no adapter / no
    conform / not a wire primitive"."""
    import pytest

    from dqlitedbapi.exceptions import DataError

    class Plain:
        pass

    with pytest.raises(DataError, match="Plain"):
        _convert_bind_param(Plain())


def test_instance_level_conform_is_honoured() -> None:
    """Stdlib parity: ``__conform__`` set on an instance (not the
    class) is honoured. CPython's
    ``Modules/_sqlite/microprotocols.c`` calls
    ``PyObject_GetAttrString(obj, "__conform__")`` which consults
    the instance first; the dqlite implementation must match."""

    class Plain:
        pass

    obj = Plain()
    obj.__conform__ = lambda protocol: "instance-bound" if protocol is PrepareProtocol else None  # type: ignore[attr-defined]
    out = _convert_bind_param(obj)
    assert out == "instance-bound"


def test_class_level_conform_still_honoured_after_instance_lookup_change() -> None:
    """Regression guard: class-side ``__conform__`` continues to work
    after the lookup change to instance-level ``getattr(value, ...)``.
    ``getattr`` walks the descriptor protocol so a class-defined
    method is bound and called as ``method(PrepareProtocol)``."""

    class WithClassConform:
        def __conform__(self, protocol: type) -> object:
            if protocol is PrepareProtocol:
                return "class-bound"
            return None

    out = _convert_bind_param(WithClassConform())
    assert out == "class-bound"


def test_raising_conform_propagates_unwrapped() -> None:
    """A ``__conform__`` that raises propagates the exception to the
    caller, matching stdlib ``sqlite3.Cursor.execute``:

        >>> class Bad:
        ...     def __conform__(self, protocol):
        ...         raise RuntimeError("boom")
        >>> import sqlite3
        >>> sqlite3.connect(":memory:").execute("SELECT ?", (Bad(),))
        RuntimeError: boom

    The previous silent-swallow disposition diverged from stdlib and
    erased the caller's traceback. Full parametric coverage in
    ``test_convert_bind_param_conform_exception_propagates.py``."""
    import pytest

    class Boom:
        def __conform__(self, protocol: type) -> object:
            raise RuntimeError("conform exploded")

    with pytest.raises(RuntimeError, match="conform exploded"):
        _convert_bind_param(Boom())
