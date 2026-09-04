"""``close()``'s cancel arm cancels the inner conn's ``_pending_drain`` task before
nulling the inner reference; otherwise GC of the unreachable inner emits the
"Task was destroyed but it is pending" warning."""

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
        await asyncio.sleep(60)  # park until the outer cancel interrupts the shield

    inner.close = slow_close
    conn._async_conn = inner

    # Tight outer cancel interrupts the shielded inner-close so the cancel arm runs.
    with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
        async with asyncio.timeout(0.02):
            await conn.close()

    assert conn._async_conn is None
    await asyncio.sleep(0)  # let the cancel propagate into park_forever
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
        await asyncio.sleep(0)  # let the loop reap the cancelled tasks

    matching = [
        str(w.message) for w in caught if "Task was destroyed but it is pending" in str(w.message)
    ]
    assert not matching, f"unexpected pending-task warning(s): {matching!r}"
