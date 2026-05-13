"""Pin: ``register_adapter`` / ``unregister_adapter`` are process-
global module-scope callables — the sync and async namespaces
re-export the SAME function object on the same underlying
``_ADAPTERS`` dict, mirroring stdlib ``sqlite3.register_adapter``
pre-3.12 semantics.

Companion to the autouse ``_restore_adapters`` fixture in conftest
that snapshots/restores ``_ADAPTERS`` per test. The fixture
neutralises test-pollution; these tests pin the documented cross-
namespace identity + mutation-visibility contract that a future
refactor introducing per-namespace dicts would silently violate.

The contract is documented at ``types.py`` (the registry's
docstring) but unpinned by a test today.
"""

from __future__ import annotations

import dqlitedbapi
import dqlitedbapi.aio as dqlite_aio
from dqlitedbapi.types import _ADAPTERS


def test_register_adapter_sync_and_aio_are_same_function_object() -> None:
    """The two namespaces re-export the SAME callable. A future
    refactor that introduces per-namespace registries would diverge
    these identities."""
    assert dqlitedbapi.register_adapter is dqlite_aio.register_adapter
    assert dqlitedbapi.unregister_adapter is dqlite_aio.unregister_adapter


def test_register_via_aio_visible_to_sync_registry_dict() -> None:
    """Registering on ``dqlitedbapi.aio`` mutates the same
    ``_ADAPTERS`` dict the sync bind path consults — proving the
    documented process-global contract end-to-end at the registry
    layer."""

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
    """Mirror: registering on ``dqlitedbapi`` makes the adapter
    visible to anything that goes through the aio re-export, since
    both consult the same dict."""

    class _Tag:
        pass

    def _adapt(_t: _Tag) -> int:
        return 7

    dqlitedbapi.register_adapter(_Tag, _adapt)
    try:
        # The aio namespace re-exports the same dict via the same
        # module-level callable; assert the registry's view directly
        # (the unregister_adapter path on the aio side would confirm
        # the same thing — symmetric below).
        assert _ADAPTERS[_Tag] is _adapt
        # The aio unregister callable mutates the same dict.
        dqlite_aio.unregister_adapter(_Tag)
        assert _Tag not in _ADAPTERS
    finally:
        _ADAPTERS.pop(_Tag, None)
