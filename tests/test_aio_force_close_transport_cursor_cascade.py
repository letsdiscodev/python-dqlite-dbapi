"""AsyncConnection.force_close_transport cascades closed state to every tracked cursor,
matching the sync sibling, so SA non-greenlet finalize paths don't leak live cursors."""

from __future__ import annotations

import weakref

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


def _make_async_connection_with_cursor() -> tuple[AsyncConnection, AsyncCursor]:
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
    conn, cur = _make_async_connection_with_cursor()

    conn.force_close_transport()

    assert cur._closed is True
    assert cur._rows == []
    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._lastrowid is None
    assert cur._row_index == 0


def test_force_close_transport_swaps_cursor_connection_to_weakref_proxy() -> None:
    """The cascade swaps cursor._connection to a weakref.proxy so it no longer pins
    the dead AsyncConnection."""
    conn, cur = _make_async_connection_with_cursor()

    conn.force_close_transport()

    assert type(cur._connection) in weakref.ProxyTypes


def test_force_close_transport_clears_cursor_set() -> None:
    conn, _cur = _make_async_connection_with_cursor()

    conn.force_close_transport()

    assert len(conn._cursors) == 0


def test_force_close_transport_no_cursors_set_tolerated() -> None:
    """The cascade must tolerate a missing _cursors attribute (pre-init __new__ fixtures)."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    conn._async_conn = None
    conn.messages = []

    conn.force_close_transport()
