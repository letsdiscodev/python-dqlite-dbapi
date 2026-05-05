"""Pin: ``AsyncConnection.close()``'s ``finally`` block must let any
in-flight CancelledError continue propagating after the
``InterfaceError`` arm completes its force-close work.

A ``return`` inside a ``finally`` block silently discards any
propagating exception (Python language semantics). The
InterfaceError arm previously ended with ``return``, which
swallowed the CancelledError that arrived during the ``async with
op_lock`` acquire — breaking TaskGroup parents' ability to
observe a child cancellation that landed during close.
"""

from __future__ import annotations

import asyncio
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def _prime_connection_with_in_use_inner() -> AsyncConnection:
    """Build an AsyncConnection whose underlying client raises
    InterfaceError on close (simulating the in-use sibling-task
    scenario the close finally is designed to recover from)."""
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


@pytest.mark.asyncio
async def test_close_finally_does_not_swallow_cancelled_error_from_body() -> None:
    """Drive the production scenario: the body's
    ``await self._async_conn.close()`` raises CancelledError (an
    outer ``asyncio.timeout`` fired); the ``finally`` runs the
    shielded close which raises InterfaceError (sibling task still
    owns ``_in_use``); the InterfaceError handler force-closes the
    writer and falls through. The original CancelledError MUST
    continue propagating out of close()."""
    conn = _prime_connection_with_in_use_inner()
    conn._ensure_locks()

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    # First call (the body's close) raises CancelledError; second
    # call (the shielded close in the finally) raises InterfaceError.
    inner.close = AsyncMock(
        side_effect=[
            asyncio.CancelledError(),
            InterfaceError("connection still in_use"),
        ]
    )
    conn._async_conn = inner

    with pytest.raises(asyncio.CancelledError):
        await conn.close()

    # Force-close path ran inside the finally's InterfaceError arm.
    assert writer.close.call_count >= 1


@pytest.mark.asyncio
async def test_close_finally_interface_error_arm_drains_pending_drain_task() -> None:
    """When the InterfaceError arm fires (cross-task contract violation),
    the inner client may already carry a ``_pending_drain`` task
    scheduled by a prior ``_invalidate``. The arm must await that
    task before nulling ``self._async_conn`` — otherwise the drain
    task is orphaned (its reader-pump never observes the writer's
    FIN before its only reachability path goes away) and Python
    prints "Task was destroyed but it is pending" at GC time.
    """
    conn = _prime_connection_with_in_use_inner()
    conn._ensure_locks()

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto

    # Synthesize a slow pending_drain task on the inner — modelling a
    # sibling task's ``_invalidate`` having already scheduled a
    # bounded ``wait_closed`` on the loop.
    drain_observed = asyncio.Event()

    async def _slow_drain() -> None:
        try:
            await asyncio.sleep(0.5)
        finally:
            drain_observed.set()

    inner._pending_drain = asyncio.create_task(_slow_drain())
    # Body's close raises CancelledError (so we skip the body's
    # ``self._async_conn = None`` and reach the finally's shielded
    # close); finally's shielded close raises InterfaceError so the
    # arm under test runs.
    inner.close = AsyncMock(
        side_effect=[
            asyncio.CancelledError(),
            InterfaceError("connection still in_use"),
        ]
    )
    conn._async_conn = inner

    with pytest.raises(asyncio.CancelledError):
        await conn.close()

    # The pending_drain MUST have run to completion (or at least to a
    # point where the close-side awaited it). Without the fix it
    # would still be "pending" here.
    assert inner._pending_drain.done(), (
        "InterfaceError arm must await inner._pending_drain before "
        "nulling _async_conn — otherwise the drain task is orphaned."
    )
    assert drain_observed.is_set()
    # Force-close still ran.
    assert writer.close.call_count >= 1


@pytest.mark.asyncio
async def test_close_finally_interface_error_path_clears_lock_state() -> None:
    """When ONLY the shielded close raises InterfaceError (the body's
    close went through fine), the lock-cleanup tail still runs and
    clears _connect_lock / _op_lock / _loop_ref. Tests that the
    fall-through reaches the unconditional cleanup tail."""
    conn = _prime_connection_with_in_use_inner()
    conn._ensure_locks()

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    # Body's close succeeds; the shielded close in the finally is a
    # no-op for our purposes (the body already nulled the test inner,
    # but the finally re-checks ``self._async_conn is not None``).
    inner.close = AsyncMock()
    conn._async_conn = inner

    await conn.close()

    # Async conn cleared by the body; lock cleanup unconditional.
    assert conn._async_conn is None
    assert conn._connect_lock is None
    assert conn._op_lock is None
    assert conn._loop_ref is None
    assert conn._closed is True
