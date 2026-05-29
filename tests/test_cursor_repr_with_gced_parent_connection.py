"""``Cursor.__repr__`` must not leak ``ReferenceError`` on a GC'd parent connection:
after close, ``_connection`` is a ``weakref.proxy`` whose attribute access raises
before getattr's default fires, and ``ReferenceError`` is outside the Error hierarchy."""

from __future__ import annotations

import gc
import weakref
from unittest.mock import MagicMock

from dqlitedbapi.cursor import Cursor


def _make_cursor() -> tuple[Cursor, MagicMock]:
    cur = Cursor.__new__(Cursor)
    parent = MagicMock()
    parent._address = "localhost:9001"
    parent._check_thread = MagicMock()
    parent._max_total_rows = None
    cur._connection = parent
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur._row_factory = None
    cur._completed_iterations = 0
    cur.messages = []
    return cur, parent


def test_cursor_repr_does_not_leak_referenceerror_on_gced_parent() -> None:
    cur, parent = _make_cursor()
    cur.close()
    cur._connection = weakref.proxy(parent)  # type: ignore[assignment]
    del parent
    gc.collect()
    s = repr(cur)
    assert "Cursor" in s
    assert "closed" in s


def test_cursor_repr_address_falls_back_to_placeholder_when_proxy_dead() -> None:
    """Stale-proxy address read falls back to the ``'?'`` placeholder."""
    cur, parent = _make_cursor()
    cur.close()
    cur._connection = weakref.proxy(parent)  # type: ignore[assignment]
    del parent
    gc.collect()
    s = repr(cur)
    assert "address='?'" in s or 'address="?"' in s
