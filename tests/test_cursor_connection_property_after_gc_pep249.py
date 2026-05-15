"""Pin: ``Cursor.connection`` property translates a ``ReferenceError``
from the ``weakref.proxy(self._connection)`` swap into a PEP 249
``InterfaceError`` so cross-driver code wrapping cursor introspection
in ``except dbapi.Error:`` continues to match after the parent
``Connection`` has been garbage-collected.

Companion to ``test_cursor_closed_after_connection_gc_pep249`` which
covers the 11 other public cursor methods; the ``connection``
property is the asymmetric outlier whose translation arm is pinned
here.

The probe attribute access at ``cursor.py`` (``_ = self._connection
.address``) is load-bearing: a refactor that replaces the probe with
one that does NOT trigger weakref resolution (e.g. ``type(self
._connection)``) would silently return a dead proxy that blows up on
the caller's first attribute access — outside the dbapi.Error
hierarchy. These tests pin both the translation arm and the
chained-cause discipline.
"""

from __future__ import annotations

import gc

import pytest

import dqlitedbapi
from dqlitedbapi.connection import Connection


def _open_closed_cursor_with_gcd_connection() -> dqlitedbapi.Cursor:
    conn = Connection("localhost:9001", timeout=1.0)
    cur = conn.cursor()
    cur.close()
    del conn
    gc.collect()
    return cur


def test_connection_property_after_parent_gc_raises_interfaceerror() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.InterfaceError, match="garbage-collected"):
        _ = cur.connection


def test_connection_property_after_parent_gc_chains_referenceerror() -> None:
    """``__cause__`` carries the underlying ``ReferenceError`` so an
    operator triaging the InterfaceError sees the original signal."""
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.InterfaceError) as exc_info:
        _ = cur.connection
    assert isinstance(exc_info.value.__cause__, ReferenceError)


def test_connection_property_caught_by_dbapi_error_hierarchy() -> None:
    """The translation is the load-bearing PEP 249 hierarchy defence:
    a generic ``except dbapi.Error:`` must match."""
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        _ = cur.connection
