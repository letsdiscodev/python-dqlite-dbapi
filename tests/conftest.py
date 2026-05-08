"""Pytest configuration for dqlite-dbapi tests."""

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _clear_resolve_leader_cache() -> Iterator[None]:
    """Clear the process-wide ``_resolve_leader`` ClusterClient cache
    between tests. The cache is keyed by (address, governors) so two
    tests that mock ``ClusterClient`` against the same seed address
    would otherwise share a stale cached instance from the first
    test's patch context.
    """
    from dqlitedbapi import connection as _conn_mod

    _conn_mod._RESOLVE_LEADER_CACHE.clear()
    yield
    _conn_mod._RESOLVE_LEADER_CACHE.clear()


@pytest.fixture(autouse=True)
def _restore_adapters() -> Iterator[None]:
    """Snapshot and restore the process-global ``_ADAPTERS`` dict
    between tests.

    The registry mirrors stdlib ``sqlite3.register_adapter`` semantics
    (process-global), but tests that mutate it via ``register_adapter``
    / ``unregister_adapter`` and rely on manual cleanups can leak state
    if a body asserts mid-test. Snapshot/restore keeps the production
    module-global semantics while preventing test-pollution leakage.
    """
    from dqlitedbapi.types import _ADAPTERS

    snapshot = dict(_ADAPTERS)
    try:
        yield
    finally:
        _ADAPTERS.clear()
        _ADAPTERS.update(snapshot)


# Add python-dqlite-dev's testlib to sys.path so tests (in particular
# the leader-redirect integration suite) can import shared utilities
# from ``dqlitetestlib``. ``python-dqlite-dev`` is expected as a
# sibling of this checkout — see ``python-dqlite-dev/testlib/README.md``.
# The insertion is harmless when the sibling repo is absent.
_TESTLIB = Path(__file__).resolve().parent.parent.parent / "python-dqlite-dev" / "testlib"
if _TESTLIB.exists() and str(_TESTLIB) not in sys.path:
    sys.path.insert(0, str(_TESTLIB))

# Pytest 8+ requires ``pytest_plugins`` at the top-level conftest.
# Only register the testlib's fixtures plugin when the path resolved
# so consumers running unit tests without the sibling repo see no
# difference.
if _TESTLIB.exists():
    pytest_plugins = ["dqlitetestlib.fixtures"]
