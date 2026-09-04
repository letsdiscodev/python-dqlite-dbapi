"""``close()``'s cancel arm calls ``force_close_transport()`` so the writer/FD is reaped
regardless of which cancel arrived. An outer ``asyncio.timeout`` cancel bypasses the
inner ``except TimeoutError``, so without the cancel-arm force-close the FD leaks at GC."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _build_bare_async_conn() -> AsyncConnection:
    """Minimally-wired AsyncConnection: bypasses __init__ (which would dial) and sets only
    the fields the close() cancel arm reads."""
    import os
    import weakref

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._timeout = 1.0
    conn._close_timeout = 1.0
    conn.messages = []
    conn._transaction_owner = None
    conn._creator_pid = os.getpid()
    import weakref as _w

    conn._cursors = _w.WeakSet()
    conn._finalizer = None
    conn._op_lock = asyncio.Lock()
    conn._connect_lock = asyncio.Lock()
    conn._loop_ref = weakref.ref(asyncio.get_running_loop())
    return conn


@pytest.mark.asyncio
async def test_close_cancel_arm_calls_force_close_transport() -> None:
    conn = _build_bare_async_conn()

    # Inner close parks forever. The shield absorbs the first cancel; a second cancel
    # lands on the shield itself and re-delivers CancelledError to the awaiter.
    parked = asyncio.Event()
    drain_entered = asyncio.Event()

    async def _parked_close() -> None:
        drain_entered.set()
        await parked.wait()

    inner = MagicMock()
    inner.close = _parked_close
    inner._pending_drain = None
    inner._protocol = MagicMock()
    inner._protocol._writer = MagicMock()
    inner._closed_flag = [False]
    inner._finalizer = None
    conn._async_conn = inner

    force_close_calls: list[int] = []
    real_force_close = conn.force_close_transport

    def _spy_force_close() -> None:
        force_close_calls.append(1)
        real_force_close()

    conn.force_close_transport = _spy_force_close

    close_task = asyncio.create_task(conn.close())
    await drain_entered.wait()

    close_task.cancel()  # absorbed by the shield
    await asyncio.sleep(0)
    close_task.cancel()  # lands on the shield; re-raises into close()'s cancel arm

    with pytest.raises(asyncio.CancelledError):
        await close_task

    assert force_close_calls, (
        "AsyncConnection.close()'s cancel arm must call "
        "force_close_transport so the transport is reaped under "
        "outer asyncio.timeout / cancel-cascade"
    )
    assert inner._protocol._writer.close.called, (
        "force_close_transport must reap the underlying writer"
    )

    parked.set()  # release the parked inner for clean teardown
