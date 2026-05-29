"""Pytest configuration for dqlite-dbapi tests."""

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _clear_resolve_leader_cache() -> Iterator[None]:
    """Clear the process-wide ``_resolve_leader`` cache between tests; it
    is keyed by (address, governors) so two tests mocking ``ClusterClient``
    against the same seed would share a stale instance."""
    from dqlitedbapi import connection as _conn_mod

    _conn_mod._RESOLVE_LEADER_CACHE.clear()
    yield
    _conn_mod._RESOLVE_LEADER_CACHE.clear()


@pytest.fixture(autouse=True)
def _restore_adapters() -> Iterator[None]:
    """Snapshot/restore the process-global ``_ADAPTERS`` dict between
    tests so a body that asserts mid-mutation cannot leak state."""
    from dqlitedbapi.types import _ADAPTERS

    snapshot = dict(_ADAPTERS)
    try:
        yield
    finally:
        _ADAPTERS.clear()
        _ADAPTERS.update(snapshot)


# Add the sibling python-dqlite-dev testlib (``dqlitetestlib``) to
# sys.path when present; harmless if the sibling repo is absent.
_TESTLIB = Path(__file__).resolve().parent.parent.parent / "python-dqlite-dev" / "testlib"
if _TESTLIB.exists() and str(_TESTLIB) not in sys.path:
    sys.path.insert(0, str(_TESTLIB))

# Pytest 8+ requires ``pytest_plugins`` at the top-level conftest;
# only register when the testlib path resolved.
if _TESTLIB.exists():
    pytest_plugins = ["dqlitetestlib.fixtures"]
