"""Pytest configuration for dqlite-dbapi tests."""

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _restore_adapters() -> Iterator[None]:
    """Snapshot and restore the process-global adapter registry around each test."""
    from dqlitedbapi.types import _ADAPTERS

    snapshot = dict(_ADAPTERS)
    try:
        yield
    finally:
        _ADAPTERS.clear()
        _ADAPTERS.update(snapshot)


# The sibling python-dqlite-dev checkout provides the ``cluster_control`` fixture.
_TESTLIB = Path(__file__).resolve().parent.parent.parent / "python-dqlite-dev" / "testlib"
if _TESTLIB.exists() and str(_TESTLIB) not in sys.path:
    sys.path.insert(0, str(_TESTLIB))

if _TESTLIB.exists():
    pytest_plugins = ["dqlitetestlib.fixtures"]
