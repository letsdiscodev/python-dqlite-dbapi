"""Pin: ``force_close_transport()`` nulls the lazy loop-bound slots
(``_connect_lock`` / ``_op_lock`` / ``_loop_ref`` / ``_transaction_owner``)
on every exit path, matching ``close()``, so the dead loop is not kept
reachable until the ``AsyncConnection`` is GC'd.
"""

from __future__ import annotations

import asyncio
import weakref

from dqliteclient import get_current_pid
from dqlitedbapi.aio.connection import AsyncConnection


def _make_conn_with_bound_locks() -> tuple[AsyncConnection, asyncio.AbstractEventLoop]:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = "host:1234"
    conn._database = "x"
    conn._closed_flag = [False]
    conn._closed = False
    conn._async_conn = None
    conn._creator_pid = get_current_pid()
    loop = asyncio.new_event_loop()
    conn._connect_lock = asyncio.Lock()
    conn._op_lock = asyncio.Lock()
    conn._loop_ref = weakref.ref(loop)
    conn._transaction_owner = object()  # type: ignore[assignment]
    return conn, loop


def _assert_slots_nulled(conn: AsyncConnection) -> None:
    assert conn._async_conn is None
    assert conn._connect_lock is None
    assert conn._op_lock is None
    assert conn._loop_ref is None
    assert conn._transaction_owner is None


def test_force_close_nulls_slots_on_inner_none_arm() -> None:
    """``_async_conn is None`` early-return arm nulls the loop-bound slots."""
    conn, loop = _make_conn_with_bound_locks()
    try:
        conn._async_conn = None  # hit the inner-None early return
        conn.force_close_transport()
        _assert_slots_nulled(conn)
    finally:
        loop.close()


def test_force_close_nulls_slots_on_fork_arm() -> None:
    """The fork-after-init arm (pid mismatch) nulls the loop-bound slots."""
    conn, loop = _make_conn_with_bound_locks()
    try:
        conn._async_conn = object()  # type: ignore[assignment]  # non-None
        conn._creator_pid = get_current_pid() + 1  # force the pid-mismatch arm
        conn.force_close_transport()
        _assert_slots_nulled(conn)
    finally:
        loop.close()


def test_force_close_nulls_slots_on_main_tail() -> None:
    """The regular non-fork teardown tail nulls the loop-bound slots."""
    conn, loop = _make_conn_with_bound_locks()
    try:
        # Bare object as inner: _protocol / _pending_drain / _finalizer all
        # miss (default None), so teardown reaches the tail without a real transport.
        conn._async_conn = object()  # type: ignore[assignment]
        conn._creator_pid = get_current_pid()  # same pid → not the fork arm
        conn.force_close_transport()
        _assert_slots_nulled(conn)
    finally:
        loop.close()
