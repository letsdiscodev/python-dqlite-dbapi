"""``close()``'s ``finally`` lets an in-flight CancelledError keep propagating after the
InterfaceError arm's force-close. A ``return`` in ``finally`` silently swallows the
exception, breaking TaskGroup parents' observation of a cancel landing during close."""

from __future__ import annotations

import asyncio
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def _prime_connection_with_in_use_inner() -> AsyncConnection:
    """AsyncConnection whose inner raises InterfaceError on close (in-use sibling-task
    scenario)."""
    import os as _os

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._async_conn = None  # set per-test
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []
    conn._timeout = 5.0
    conn._close_timeout = 0.5
    conn._creator_pid = _os.getpid()
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    return conn


async def test_close_finally_does_not_swallow_cancelled_error_from_body() -> None:
    """Body close raises CancelledError, finally's shielded close raises InterfaceError
    and force-closes; the original CancelledError must keep propagating out of close()."""
    conn = _prime_connection_with_in_use_inner()
    conn._ensure_locks()

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    # Body's close raises CancelledError; the finally's shielded close raises InterfaceError.
    inner.close = AsyncMock(
        side_effect=[
            asyncio.CancelledError(),
            InterfaceError("connection still in_use"),
        ]
    )
    conn._async_conn = inner

    with pytest.raises(asyncio.CancelledError):
        await conn.close()

    assert writer.close.call_count >= 1  # force-close ran in the InterfaceError arm


async def test_close_finally_interface_error_arm_drains_pending_drain_task() -> None:
    """The InterfaceError arm must await a pre-existing ``_pending_drain`` before nulling
    ``_async_conn``, else the task is orphaned and warns "Task was destroyed" at GC."""
    conn = _prime_connection_with_in_use_inner()
    conn._ensure_locks()

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto

    # Slow pending_drain on the inner, as a sibling's _invalidate would have scheduled.
    drain_observed = asyncio.Event()

    async def _slow_drain() -> None:
        try:
            await asyncio.sleep(0.5)
        finally:
            drain_observed.set()

    inner._pending_drain = asyncio.create_task(_slow_drain())
    # Body close raises CancelledError (reaching the finally); shielded close raises
    # InterfaceError so the arm under test runs.
    inner.close = AsyncMock(
        side_effect=[
            asyncio.CancelledError(),
            InterfaceError("connection still in_use"),
        ]
    )
    conn._async_conn = inner

    with pytest.raises(asyncio.CancelledError):
        await conn.close()

    assert inner._pending_drain.done(), (
        "InterfaceError arm must await inner._pending_drain before "
        "nulling _async_conn — otherwise the drain task is orphaned."
    )
    assert drain_observed.is_set()
    assert writer.close.call_count >= 1


async def test_close_finally_interface_error_path_clears_lock_state() -> None:
    """When only the shielded close raises InterfaceError, the fall-through still reaches
    the unconditional lock-cleanup tail."""
    conn = _prime_connection_with_in_use_inner()
    conn._ensure_locks()

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    inner.close = AsyncMock()
    conn._async_conn = inner

    await conn.close()

    assert conn._async_conn is None
    assert conn._connect_lock is None
    assert conn._op_lock is None
    assert conn._loop_ref is None
    assert conn._closed is True
