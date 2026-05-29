"""Every public Cursor method on a closed cursor whose parent Connection
was GC'd must raise inside the PEP 249 Error hierarchy, not bleed the
ReferenceError from the stale weakref.proxy back-reference. The prelude
must check _closed before touching the proxied connection."""

from __future__ import annotations

import gc

import pytest

import dqlitedbapi
from dqlitedbapi.connection import Connection


def _open_closed_cursor_with_gcd_connection() -> dqlitedbapi.Cursor:
    """Closed cursor holding a stale weakref.proxy to a GC'd Connection."""
    conn = Connection("localhost:9001", timeout=1.0)
    cur = conn.cursor()
    cur.close()
    del conn
    gc.collect()
    return cur


def test_fetchone_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.fetchone()


def test_fetchmany_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.fetchmany(10)


def test_fetchall_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.fetchall()


def test_execute_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.execute("SELECT 1")


def test_executemany_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])


def test_callproc_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.callproc("anything")


def test_nextset_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.nextset()


def test_scroll_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.scroll(0)


def test_executescript_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.executescript("SELECT 1")


def test_setinputsizes_on_closed_cursor_after_connection_gc_does_not_raise() -> None:
    """setinputsizes is a permissive no-op (PEP 249 §6.2); must not leak
    ReferenceError on a closed cursor with a GC'd parent."""
    cur = _open_closed_cursor_with_gcd_connection()
    cur.setinputsizes([None])


def test_setoutputsize_on_closed_cursor_after_connection_gc_does_not_raise() -> None:
    """setoutputsize mirrors setinputsizes: permissive no-op (PEP 249 §6.2)."""
    cur = _open_closed_cursor_with_gcd_connection()
    cur.setoutputsize(100)
