"""Pin: ``register_adapter``/``unregister_adapter`` are process-global — the sync and
async namespaces re-export the SAME callable over the same ``_ADAPTERS`` dict."""

from __future__ import annotations

import dqlitedbapi
import dqlitedbapi.aio as dqlite_aio
from dqlitedbapi.types import _ADAPTERS


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
