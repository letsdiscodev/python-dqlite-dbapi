"""Adapter registry: register/unregister, conform fallback, binary adaptation, aio re-exports."""

from __future__ import annotations

import datetime

import pytest

import dqlitedbapi
import dqlitedbapi.aio
import dqlitedbapi.aio as dqlite_aio
from dqlitedbapi import PrepareProtocol
from dqlitedbapi.exceptions import DataError, NotSupportedError, ProgrammingError
from dqlitedbapi.types import _ADAPTERS, adapt_bind_param, register_adapter, unregister_adapter


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


def test_register_adapter_sync_and_aio_are_same_function_object() -> None:
    """The two namespaces re-export the SAME callable."""
    assert dqlitedbapi.register_adapter is dqlite_aio.register_adapter
    assert dqlitedbapi.unregister_adapter is dqlite_aio.unregister_adapter


def test_register_via_aio_visible_to_sync_registry_dict() -> None:
    """Registering on the aio namespace mutates the same ``_ADAPTERS`` dict."""

    class _Tag:
        pass

    def _adapt(_t: _Tag) -> int:
        return 42

    dqlite_aio.register_adapter(_Tag, _adapt)
    try:
        assert _ADAPTERS[_Tag] is _adapt
    finally:
        dqlite_aio.unregister_adapter(_Tag)


def test_register_via_sync_visible_to_aio_registry_dict() -> None:
    """Mirror: registering on the sync namespace is visible via the aio re-export."""

    class _Tag:
        pass

    def _adapt(_t: _Tag) -> int:
        return 7

    dqlitedbapi.register_adapter(_Tag, _adapt)
    try:
        assert _ADAPTERS[_Tag] is _adapt
        # The aio unregister callable mutates the same dict.
        dqlite_aio.unregister_adapter(_Tag)
        assert _Tag not in _ADAPTERS
    finally:
        _ADAPTERS.pop(_Tag, None)


def test_register_adapter_rejects_keyword_args() -> None:
    with pytest.raises(TypeError):
        dqlitedbapi.register_adapter(type_=int, adapter=str)  # type: ignore[call-arg]


def test_unregister_adapter_rejects_keyword_args() -> None:
    with pytest.raises(TypeError):
        dqlitedbapi.unregister_adapter(type_=int)  # type: ignore[call-arg]


def test_register_adapter_positional_still_works() -> None:
    """Regression: the canonical positional form remains supported."""

    class _Marker:
        pass

    dqlitedbapi.register_adapter(_Marker, lambda v: "x")
    try:
        from dqlitedbapi.types import _ADAPTERS

        assert _Marker in _ADAPTERS
    finally:
        dqlitedbapi.unregister_adapter(_Marker)


class _Foo:
    pass


def test_unregister_adapter_round_trip() -> None:
    dqlitedbapi.register_adapter(_Foo, lambda f: 42)
    assert _Foo in _ADAPTERS
    dqlitedbapi.unregister_adapter(_Foo)
    assert _Foo not in _ADAPTERS


def test_unregister_adapter_raises_when_no_adapter_registered() -> None:
    """Unregister on a never-registered type raises ``AdapterLookupError``."""
    import pytest

    from dqlitedbapi.exceptions import ProgrammingError

    class _Bar:
        pass

    with pytest.raises(ProgrammingError, match="no adapter registered"):
        dqlitedbapi.unregister_adapter(_Bar)
    assert _Bar not in _ADAPTERS


def test_unregister_adapter_unknown_type_catchable_as_lookuperror() -> None:
    """Parity: stdlib raises ``KeyError`` (a ``LookupError``); dqlite's
    ``AdapterLookupError`` multi-inherits ``LookupError`` to stay catchable."""
    import pytest

    from dqlitedbapi.exceptions import AdapterLookupError

    class _Quux:
        pass

    with pytest.raises(LookupError, match="no adapter registered"):
        dqlitedbapi.unregister_adapter(_Quux)
    # Also catchable as the concrete class and as ``dqlitedbapi.Error``.
    with pytest.raises(AdapterLookupError):
        dqlitedbapi.unregister_adapter(_Quux)
    with pytest.raises(dqlitedbapi.Error):
        dqlitedbapi.unregister_adapter(_Quux)
    assert issubclass(AdapterLookupError, dqlitedbapi.ProgrammingError)
    assert issubclass(AdapterLookupError, LookupError)


def test_adapter_lookup_error_exported() -> None:
    """``AdapterLookupError`` is published on both the sync and async surfaces."""
    import dqlitedbapi.aio
    import dqlitedbapi.exceptions

    assert "AdapterLookupError" in dqlitedbapi.__all__
    assert "AdapterLookupError" in dqlitedbapi.aio.__all__
    assert "AdapterLookupError" in dqlitedbapi.exceptions.__all__
    assert dqlitedbapi.AdapterLookupError is dqlitedbapi.exceptions.AdapterLookupError
    assert dqlitedbapi.aio.AdapterLookupError is dqlitedbapi.AdapterLookupError


def test_unregister_adapter_publicly_exported() -> None:
    import dqlitedbapi.aio
    import dqlitedbapi.types

    assert "unregister_adapter" in dqlitedbapi.__all__
    assert dqlitedbapi.unregister_adapter is dqlitedbapi.types.unregister_adapter
    assert "unregister_adapter" in dqlitedbapi.aio.__all__
    assert dqlitedbapi.aio.unregister_adapter is dqlitedbapi.unregister_adapter


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
        out = adapt_bind_param(datetime.date(2024, 1, 1))
        assert out == "CUSTOM(2024)"
    finally:
        unregister_adapter(datetime.date)

    out = adapt_bind_param(datetime.date(2024, 1, 1))
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


def test_binary_is_memoryview_alias() -> None:
    """Pin the stdlib-parity alias so it cannot drift to a wrapper."""
    assert dqlitedbapi.Binary is memoryview


def test_binary_round_trips_bytes() -> None:
    mv = dqlitedbapi.Binary(b"hello")
    assert isinstance(mv, memoryview)
    assert bytes(mv) == b"hello"


def test_binary_bad_input_raises_bare_typeerror_not_dbapi_error() -> None:
    """Deliberate: bare TypeError preserves stdlib drop-in parity."""
    with pytest.raises(TypeError):
        dqlitedbapi.Binary("not bytes")  # type: ignore[arg-type]


def test_binary_int_raises_bare_typeerror() -> None:
    with pytest.raises(TypeError):
        dqlitedbapi.Binary(123)  # type: ignore[arg-type]


def test_binary_none_raises_bare_typeerror() -> None:
    with pytest.raises(TypeError):
        dqlitedbapi.Binary(None)  # type: ignore[arg-type]


def test_binary_typeerror_is_not_caught_by_dbapi_error_hierarchy() -> None:
    """Documented escape: porting callers must wrap their own try/except."""
    with pytest.raises(TypeError):
        try:
            dqlitedbapi.Binary("not bytes")  # type: ignore[arg-type]
        except dqlitedbapi.Error:
            pytest.fail("Binary unexpectedly inside dbapi.Error hierarchy")


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
            adapt_bind_param(Money(100))
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
        assert adapt_bind_param(Money(100)) == 100
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
    assert adapt_bind_param(primitive) == primitive


def test_conform_runtimeerror_propagates_unwrapped() -> None:
    class Bad:
        def __conform__(self, protocol: object) -> object:
            raise RuntimeError("boom from conform")

    with pytest.raises(RuntimeError, match="boom from conform"):
        adapt_bind_param(Bad())


def test_conform_typeerror_propagates_unwrapped() -> None:
    """A __conform__ raising TypeError surfaces with the original exception, not swallowed."""

    class Bad:
        def __conform__(self, protocol: object) -> object:
            raise TypeError("wrong signature")

    with pytest.raises(TypeError, match="wrong signature"):
        adapt_bind_param(Bad())


def test_conform_returning_none_rejects_non_primitive_at_dbapi_layer() -> None:
    """__conform__ returning None leaves the value unchanged; the wire-primitive guard then
    rejects it at the dbapi layer with the original type named in the diagnostic."""
    from dqlitedbapi.exceptions import ProgrammingError

    class Declines:
        def __conform__(self, protocol: object) -> object:
            return None

    with pytest.raises(ProgrammingError, match="Declines"):
        adapt_bind_param(Declines())


def test_conform_returning_adapted_value_used() -> None:
    """__conform__ returning a wire-encodable value: the adapted value is used."""

    class HasConform:
        def __conform__(self, protocol: object) -> object:
            return "adapted!"

    assert adapt_bind_param(HasConform()) == "adapted!"


def test_aio_exports_register_adapter() -> None:
    from dqlitedbapi.aio import register_adapter

    assert callable(register_adapter)


def test_aio_register_adapter_is_in_all() -> None:
    import dqlitedbapi.aio

    assert "register_adapter" in dqlitedbapi.aio.__all__


def test_aio_register_adapter_shares_registry_with_sync() -> None:
    """Mutating via the async namespace affects the sync namespace (shared registry)."""
    import dqlitedbapi
    import dqlitedbapi.aio

    class _Probe:
        pass

    def _adapter(_: _Probe) -> str:
        return "probe"

    dqlitedbapi.aio.register_adapter(_Probe, _adapter)

    assert dqlitedbapi.aio.register_adapter is dqlitedbapi.register_adapter

    from dqlitedbapi.types import _ADAPTERS

    assert _Probe in _ADAPTERS

    del _ADAPTERS[_Probe]  # avoid leaking global state to sibling tests


def test_aio_register_converter_raises_notsupported() -> None:
    with pytest.raises(NotSupportedError):
        dqlitedbapi.aio.register_converter("DATE", lambda b: b)


def test_aio_complete_statement_raises_notsupported() -> None:
    with pytest.raises(NotSupportedError):
        dqlitedbapi.aio.complete_statement("SELECT 1;")


def test_aio_enable_callback_tracebacks_raises_notsupported() -> None:
    with pytest.raises(NotSupportedError):
        dqlitedbapi.aio.enable_callback_tracebacks(True)


def test_aio_all_includes_the_three_stubs() -> None:
    assert "register_converter" in dqlitedbapi.aio.__all__
    assert "complete_statement" in dqlitedbapi.aio.__all__
    assert "enable_callback_tracebacks" in dqlitedbapi.aio.__all__


class _Probe:
    pass


def _adapter(_: _Probe) -> str:
    return "probe"


def test_step_one_register_then_leak_simulates_test_failure() -> None:
    """Step 1: register an adapter for the fixture to restore."""
    register_adapter(_Probe, _adapter)
    assert _Probe in _ADAPTERS


def test_step_two_registry_is_clean_after_prior_test() -> None:
    """Step 2: the registry must not contain the prior test's probe."""
    assert _Probe not in _ADAPTERS
