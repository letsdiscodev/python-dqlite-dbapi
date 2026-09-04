"""Cursor.connection translates the ReferenceError from a stale
weakref.proxy into InterfaceError so except dbapi.Error: keeps matching
after the parent Connection is GC'd. The probe access (_ =
self._connection.address) must trigger weakref resolution; replacing it
with one that does not (e.g. type(self._connection)) would return a dead
proxy that blows up later outside the hierarchy."""

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
    """__cause__ carries the underlying ReferenceError."""
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.InterfaceError) as exc_info:
        _ = cur.connection
    assert isinstance(exc_info.value.__cause__, ReferenceError)


def test_connection_property_caught_by_dbapi_error_hierarchy() -> None:
    """A generic except dbapi.Error: must match the translated error."""
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        _ = cur.connection
