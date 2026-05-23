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
    ``ProgrammingError`` — matches stdlib ``sqlite3.unregister_adapter``
    (Python 3.13+) which surfaces "no adapter to remove" as a clear
    contract violation rather than silently no-op'ing."""
    import pytest

    from dqlitedbapi.exceptions import ProgrammingError

    class _Bar:
        pass

    with pytest.raises(ProgrammingError, match="no adapter registered"):
        dqlitedbapi.unregister_adapter(_Bar)
    assert _Bar not in _ADAPTERS


def test_unregister_adapter_publicly_exported() -> None:
    import dqlitedbapi.aio
    import dqlitedbapi.types

    assert "unregister_adapter" in dqlitedbapi.__all__
    assert dqlitedbapi.unregister_adapter is dqlitedbapi.types.unregister_adapter
    # async surface re-exports the same callable.
    assert "unregister_adapter" in dqlitedbapi.aio.__all__
    assert dqlitedbapi.aio.unregister_adapter is dqlitedbapi.unregister_adapter
