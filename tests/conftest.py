"""Pytest configuration for dqlite-dbapi tests."""

import sys
from pathlib import Path

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
