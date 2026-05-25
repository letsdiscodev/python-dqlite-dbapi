"""Pin: ``unregister_adapter`` removes a previously-registered adapter.

Counterpart to ``register_adapter``. Without this, test cleanup needed
to reach into the private ``dqlitedbapi.types._ADAPTERS`` dict — a
private-API leak.
"""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi.types import _ADAPTERS


class _Foo:
    pass


def test_unregister_adapter_round_trip() -> None:
    dqlitedbapi.register_adapter(_Foo, lambda f: 42)
    assert _Foo in _ADAPTERS
    dqlitedbapi.unregister_adapter(_Foo)
    assert _Foo not in _ADAPTERS


def test_unregister_adapter_raises_when_no_adapter_registered() -> None:
    """Calling unregister on a never-registered type raises
    ``AdapterLookupError`` — which inherits from both
    ``ProgrammingError`` (PEP 249 hierarchy) and stdlib ``LookupError``
    (matches stdlib ``sqlite3.unregister_adapter`` (Python 3.13+) that
    raises ``KeyError``).
    """
    import pytest

    from dqlitedbapi.exceptions import ProgrammingError

    class _Bar:
        pass

    with pytest.raises(ProgrammingError, match="no adapter registered"):
        dqlitedbapi.unregister_adapter(_Bar)
    assert _Bar not in _ADAPTERS


def test_unregister_adapter_unknown_type_catchable_as_lookuperror() -> None:
    """Cross-driver parity: stdlib ``sqlite3.unregister_adapter``
    raises ``KeyError`` (a ``LookupError`` subclass) on the same
    condition. Cross-driver code that catches ``LookupError`` for the
    "no adapter to remove" case must also catch against dqlite — the
    multi-inheritance on ``AdapterLookupError`` is the parity hook.
    """
    import pytest

    from dqlitedbapi.exceptions import AdapterLookupError

    class _Quux:
        pass

    with pytest.raises(LookupError, match="no adapter registered"):
        dqlitedbapi.unregister_adapter(_Quux)
    # Same exception is also catchable as the concrete class and as
    # ``dqlitedbapi.Error`` (PEP 249 hierarchy purity).
    with pytest.raises(AdapterLookupError):
        dqlitedbapi.unregister_adapter(_Quux)
    with pytest.raises(dqlitedbapi.Error):
        dqlitedbapi.unregister_adapter(_Quux)
    assert issubclass(AdapterLookupError, dqlitedbapi.ProgrammingError)
    assert issubclass(AdapterLookupError, LookupError)


def test_adapter_lookup_error_exported() -> None:
    """``AdapterLookupError`` is published on both the sync and async
    surfaces so cross-driver code can introspect the concrete class
    without reaching into ``dqlitedbapi.exceptions``.
    """
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
    # async surface re-exports the same callable.
    assert "unregister_adapter" in dqlitedbapi.aio.__all__
    assert dqlitedbapi.aio.unregister_adapter is dqlitedbapi.unregister_adapter
