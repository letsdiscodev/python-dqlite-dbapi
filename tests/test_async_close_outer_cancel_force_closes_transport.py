"""Pin: ``AsyncConnection.close()``'s cancel arm calls
``force_close_transport()`` so the writer / FD is reaped regardless
of which cancel delivered us there.

Specifically:
- An OUTER ``asyncio.timeout`` deadline cancels ``close()`` while the
  shielded inner-drain is running. ``asyncio.timeout``'s
  ``CancelledError`` -> ``TimeoutError`` re-classification only fires
  when the INNER scope's deadline expires; on outer cancel the inner
  ``except TimeoutError`` is bypassed entirely.
- Without ``force_close_transport()`` in the cancel arm, the writer
  stays open and the FD leaks to GC ("Task was destroyed but it is
  pending" / un-reaped socket on shutdown).
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _build_bare_async_conn() -> AsyncConnection:
    """Build a minimally-wired ``AsyncConnection`` for cancel-arm
    coverage. Bypasses the real ``__init__`` (which would dial the
    server) and sets only the fields the close()-cancel-arm reads."""
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
    """When the shielded inner-drain in close() is cancelled, the
    cancel arm must invoke ``force_close_transport`` so the writer
    is synchronously closed."""
    conn = _build_bare_async_conn()

    # Stand-in for the inner client conn whose ``close()`` parks
    # forever. The shielded await will absorb the FIRST cancel, but
    # a SECOND cancel (which Python's ``asyncio.shield`` does NOT
    # block) lands on the shield itself and re-delivers the
    # CancelledError to the awaiter.
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

    # Spy on force_close_transport to confirm the cancel arm calls it.
    force_close_calls: list[int] = []
    real_force_close = conn.force_close_transport

    def _spy_force_close() -> None:
        force_close_calls.append(1)
        real_force_close()

    conn.force_close_transport = _spy_force_close

    close_task = asyncio.create_task(conn.close())
    await drain_entered.wait()

    # First cancel: absorbed by the shield.
    close_task.cancel()
    await asyncio.sleep(0)
    # Second cancel: lands on the shield itself; the shielded await
    # re-raises CancelledError into close()'s cancel arm.
    close_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await close_task

    # Load-bearing: the cancel arm called force_close_transport,
    # guaranteeing the writer / FD is reaped regardless of which
    # cancel delivered us here.
    assert force_close_calls, (
        "AsyncConnection.close()'s cancel arm must call "
        "force_close_transport so the transport is reaped under "
        "outer asyncio.timeout / cancel-cascade"
    )
    # And the writer.close() was actually invoked — a future inner
    # refactor that called force_close_transport but failed to reap
    # the writer would defeat the cancel-arm contract.
    assert inner._protocol._writer.close.called, (
        "force_close_transport must reap the underlying writer"
    )

    # Release the parked inner so the orphan close() can finish
    # cleanly (test teardown hygiene).
    parked.set()
