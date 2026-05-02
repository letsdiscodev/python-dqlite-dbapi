"""Pin: every public ``Cursor`` method on a closed cursor whose parent
``Connection`` has been GC'd must raise inside the PEP 249 ``Error``
hierarchy — not bleed ``ReferenceError`` from the
``weakref.proxy(self._connection)`` swap done by ``Cursor.close()``.

PEP 249 §6.1.2: "any method of a closed cursor will raise an exception."
The expected exception is ``InterfaceError`` (subclass of ``Error``).

The closed cursor swaps ``self._connection`` to a ``weakref.proxy`` to
release the strong back-reference (see
``test_cursor_close_releases_connection_pin.py``). Once the parent
``Connection`` is GC'd, the proxy is stale and any attribute access
raises ``ReferenceError``. The guard prelude in every cursor method
calls ``self._connection._check_thread()`` BEFORE
``self._check_closed()`` — so ``ReferenceError`` escapes the
PEP 249 ``Error`` hierarchy on a reachable, real-world path.

Fix: the prelude must check ``self._closed`` first; ``_check_closed``
reads only the cursor's own ``_closed`` slot, never the proxied
connection.
"""

from __future__ import annotations

import gc

import pytest

import dqlitedbapi
from dqlitedbapi.connection import Connection


def _open_closed_cursor_with_gcd_connection() -> dqlitedbapi.Cursor:
    """Construct a closed cursor whose parent Connection has been GC'd.

    Returns the cursor (still alive) holding a stale ``weakref.proxy``
    back-reference.
    """
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
    """``setinputsizes`` is documented as permissive on closed cursors
    (PEP 249 §6.2 latitude). On a closed cursor with GC'd parent, it
    must not raise ``ReferenceError`` — the documented intent is a
    silent no-op."""
    cur = _open_closed_cursor_with_gcd_connection()
    # Must not raise.
    cur.setinputsizes([None])


def test_setoutputsize_on_closed_cursor_after_connection_gc_does_not_raise() -> None:
    """``setoutputsize`` mirrors ``setinputsizes``: permissive on
    closed cursors per PEP 249 §6.2."""
    cur = _open_closed_cursor_with_gcd_connection()
    # Must not raise.
    cur.setoutputsize(100)
