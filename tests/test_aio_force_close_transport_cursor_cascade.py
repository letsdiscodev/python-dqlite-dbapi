"""Pin: ``AsyncConnection.force_close_transport`` cascades the
closed state to every tracked cursor, symmetric with the sync
sibling ``Connection.force_close_transport``'s
``_cascade_cursors`` invocation.

Without the cascade, an SA non-greenlet finalize path
(``do_terminate``, GC sweep, atexit) leaves every cursor in
``_cursors`` with ``_closed = False``, populated ``_rows`` /
``_description`` / ``_rowcount``, AND a strong ``_connection``
reference to the dead ``AsyncConnection`` — defeating the
``weakref.proxy`` swap that the close-orchestrated cascade
performs.
"""

from __future__ import annotations

import weakref

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


def _make_async_connection_with_cursor() -> tuple[AsyncConnection, AsyncCursor]:
    """Construct an AsyncConnection plus an AsyncCursor that's
    tracked in ``_cursors``. Pre-populate cursor state so the
    cascade has visible work to do."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    conn._async_conn = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._rowcount = 2
    cur._lastrowid = 99
    cur._row_index = 1
    cur._arraysize = 1
    cur.messages = []
    cur._executing_task = None
    cur._completed_iterations = 0
    cur._connection = conn

    conn._cursors.add(cur)
    return conn, cur


def test_force_close_transport_cascades_closed_state_to_cursors() -> None:
    """After force_close_transport, every tracked cursor must show
    closed state. Critical for SA non-greenlet finalize paths where
    the rich async close cannot run."""
    conn, cur = _make_async_connection_with_cursor()

    conn.force_close_transport()

    assert cur._closed is True
    assert cur._rows == []
    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._lastrowid is None
    assert cur._row_index == 0


def test_force_close_transport_swaps_cursor_connection_to_weakref_proxy() -> None:
    """The cascade swaps the cursor's ``_connection`` strong ref to
    a ``weakref.proxy`` so the cursor no longer pins the dead
    ``AsyncConnection``. Mirrors the sync sibling's discipline."""
    conn, cur = _make_async_connection_with_cursor()

    conn.force_close_transport()

    assert type(cur._connection) in weakref.ProxyTypes


def test_force_close_transport_clears_cursor_set() -> None:
    """After the cascade, the ``_cursors`` WeakSet is empty so the
    AsyncConnection no longer holds references that would keep
    the cursors alive past the close."""
    conn, _cur = _make_async_connection_with_cursor()

    conn.force_close_transport()

    assert len(conn._cursors) == 0


def test_force_close_transport_no_cursors_set_tolerated() -> None:
    """Pre-init fixtures construct an AsyncConnection via ``__new__``
    without setting ``_cursors``. The cascade must tolerate the
    missing attribute (``getattr(self, "_cursors", None)``)."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    conn._async_conn = None
    conn.messages = []

    # Must not raise.
    conn.force_close_transport()
