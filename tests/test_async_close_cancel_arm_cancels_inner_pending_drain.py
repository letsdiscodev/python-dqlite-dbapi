"""Pin: ``AsyncConnection.close()``'s cancel arm cancels the inner
client connection's ``_pending_drain`` task before nulling our
reference to the inner.

The orderly close path drains the inner ``_pending_drain`` under a
bounded resnapshot loop. On the cancel arm (the shielded close itself
was cancelled), the inner conn may still own a pending Task. Without
the cancel, GC of the now-unreachable inner emits the exact
"Task was destroyed but it is pending" warning that
``force_close_transport`` goes to extensive lengths to prevent.
"""

from __future__ import annotations

import asyncio
import gc
import warnings
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _make_conn() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = "host:9001"
    conn._database = "x"
    conn._closed = False
    conn._closed_flag = [False]
    conn._timeout = 0.05
    conn._transaction_owner = None

    import os

    conn._creator_pid = os.getpid()
    import weakref

    conn._connect_lock = asyncio.Lock()
    conn._op_lock = asyncio.Lock()
    conn._loop_ref = weakref.ref(asyncio.get_event_loop())
    conn.messages = []
    return conn


@pytest.mark.asyncio
async def test_cancel_arm_cancels_inner_pending_drain() -> None:
    conn = _make_conn()

    # Inner client conn with a pending drain Task. The Task is parked
    # indefinitely; on cancel-arm cleanup we expect the close to
    # cancel it before nulling the inner reference.
    pending_was_cancelled: list[bool] = []

    async def park_forever() -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            pending_was_cancelled.append(True)
            raise

    pending_task = asyncio.create_task(park_forever())
    inner = MagicMock()
    inner._pending_drain = pending_task

    async def slow_close(*args: object, **kwargs: object) -> None:
        # Park until cancelled — simulates the shielded close being
        # interrupted by an outer cancel landing on the close call.
        await asyncio.sleep(60)

    inner.close = slow_close
    conn._async_conn = inner

    # Wrap close() in a tight outer cancel so the shielded inner-close
    # is interrupted and the cancel arm runs.
    with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
        async with asyncio.timeout(0.02):
            await conn.close()

    # After close()'s cancel arm runs:
    # - inner._pending_drain must have been cancelled.
    # - The inner is no longer reachable from conn (conn._async_conn
    #   is None).
    assert conn._async_conn is None
    # Yield a tick so the cancel propagates into park_forever's body.
    await asyncio.sleep(0)
    assert pending_was_cancelled == [True], (
        f"inner _pending_drain must be cancelled; got {pending_was_cancelled!r}"
    )


@pytest.mark.asyncio
async def test_cancel_arm_no_warning_on_inner_gc() -> None:
    """End-to-end: no ``Task was destroyed but it is pending``
    warning emerges when the inner is GC'd after cancel-arm close."""
    conn = _make_conn()

    pending_task = asyncio.create_task(asyncio.sleep(60))
    inner = MagicMock()
    inner._pending_drain = pending_task

    async def slow_close(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(60)

    inner.close = slow_close
    conn._async_conn = inner

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
            async with asyncio.timeout(0.02):
                await conn.close()
        del inner
        gc.collect()
        # Give the loop a chance to reap the cancelled tasks.
        await asyncio.sleep(0)

    matching = [
        str(w.message) for w in caught if "Task was destroyed but it is pending" in str(w.message)
    ]
    assert not matching, f"unexpected pending-task warning(s): {matching!r}"
