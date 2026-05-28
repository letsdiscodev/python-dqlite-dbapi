"""Pin: ``AsyncConnection.force_close_transport()`` nulls the lazy
loop-bound slots (``_connect_lock`` / ``_op_lock`` / ``_loop_ref`` /
``_transaction_owner``) on every exit path, matching ``close()``.

``close()`` deliberately nulls these so a force-closed connection does
not keep the (typically dead) event loop and its ``asyncio.Lock``
primitives reachable. ``force_close_transport`` previously nulled only
``_async_conn``, leaving the dead loop reachable until the
``AsyncConnection`` itself was garbage-collected. This pins parity for
the inner-None early arm, the fork arm, and the main teardown tail.
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
    # Lazy locks + loop weakref, as ``_ensure_locks`` would have created
    # them while the connection was operable.
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
        conn._async_conn = object()  # type: ignore[assignment]  # non-None: pass inner-None check
        conn._creator_pid = get_current_pid() + 1  # force the pid-mismatch arm
        conn.force_close_transport()
        _assert_slots_nulled(conn)
    finally:
        loop.close()


def test_force_close_nulls_slots_on_main_tail() -> None:
    """The regular non-fork teardown tail nulls the loop-bound slots."""
    conn, loop = _make_conn_with_bound_locks()
    try:
        # A bare object as the inner conn: getattr for _protocol /
        # _pending_drain / _finalizer all miss (default None), so the
        # teardown reaches the tail null-out without touching a real
        # transport.
        conn._async_conn = object()  # type: ignore[assignment]
        conn._creator_pid = get_current_pid()  # same pid → not the fork arm
        conn.force_close_transport()
        _assert_slots_nulled(conn)
    finally:
        loop.close()
