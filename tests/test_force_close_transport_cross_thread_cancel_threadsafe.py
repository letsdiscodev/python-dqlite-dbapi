"""Pin: ``AsyncConnection.force_close_transport`` schedules a
``Task.cancel`` on the task's owning loop via ``call_soon_threadsafe``
when the calling thread is NOT the loop's owner.

``asyncio.Task.cancel()`` is not documented as thread-safe; CPython
implements it via ``loop.call_soon`` which is also not thread-safe.
``loop.call_soon_threadsafe(task.cancel)`` is the correct shape.

The previous implementation called ``pending.cancel()`` directly, which
falls silent when the loop is closed (a CancelledError nobody hears),
but is undefined behaviour against a live loop owned by another
thread (mid-tick of the asyncio ready queue).

This pins the loop-aware schedule path:
- Loop closed  → direct cancel (no-op; matches the old contract).
- Loop alive, this thread is the owner → direct cancel.
- Loop alive, foreign thread → call_soon_threadsafe(cancel).
"""

from __future__ import annotations

import asyncio
import os
import threading
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


def _make_async_connection_with_pending_loop(
    *, loop: asyncio.AbstractEventLoop, loop_closed: bool
) -> tuple[AsyncConnection, MagicMock, MagicMock]:
    """Build an AsyncConnection whose inner._pending_drain is a Mock
    Task whose ``get_loop()`` returns ``loop``."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = None
    aconn._closed_flag = [False]

    inner = MagicMock()
    inner._protocol = None  # exercise the cleanup-tail-only branch

    pending = MagicMock()
    pending.done.return_value = False
    pending.cancel = MagicMock()
    pending.get_loop.return_value = loop
    inner._pending_drain = pending

    if loop_closed:
        # Make the loop's is_closed() report True. Real loops returned by
        # asyncio.new_event_loop() track their own closed state; we can
        # rely on actual `loop.close()` to flip is_closed.
        loop.close()

    aconn._async_conn = inner
    return aconn, inner, pending


def test_cancel_called_directly_when_loop_closed() -> None:
    """SA finalize / atexit / GC path: loop is gone. Cancel falls back
    to direct call (no-op at the asyncio C level)."""
    loop = asyncio.new_event_loop()
    aconn, inner, pending = _make_async_connection_with_pending_loop(loop=loop, loop_closed=True)

    aconn.force_close_transport()

    pending.cancel.assert_called_once()
    assert aconn._async_conn is None


def test_cancel_scheduled_via_call_soon_threadsafe_from_foreign_thread() -> None:
    """Live loop on thread A; force_close_transport invoked from
    thread B. Cancel must be scheduled via call_soon_threadsafe;
    pending.cancel must NOT be called directly from thread B (that
    would race the ready-queue)."""
    loop = asyncio.new_event_loop()

    aconn, inner, pending = _make_async_connection_with_pending_loop(loop=loop, loop_closed=False)

    # Wrap call_soon_threadsafe so we can observe the call shape.
    cstu_calls: list[tuple[object, tuple[object, ...]]] = []
    real_cstu = loop.call_soon_threadsafe

    def _capture_cstu(callback: object, *args: object) -> object:
        cstu_calls.append((callback, args))
        return real_cstu(callback, *args)  # type: ignore[arg-type]

    loop.call_soon_threadsafe = _capture_cstu  # type: ignore[assignment]

    # Run force_close_transport on a foreign thread.
    def _run_from_foreign_thread() -> None:
        aconn.force_close_transport()

    t = threading.Thread(target=_run_from_foreign_thread)
    t.start()
    t.join(timeout=2.0)
    assert not t.is_alive()

    # The cancel must have been scheduled, not called directly.
    pending.cancel.assert_not_called()
    assert len(cstu_calls) == 1
    callback, args = cstu_calls[0]
    assert callback is pending.cancel
    assert args == ()
    assert aconn._async_conn is None

    loop.close()


def test_cancel_called_directly_on_owning_thread() -> None:
    """Live loop, current thread is the owner. Cancel runs directly
    (no need to schedule via call_soon_threadsafe)."""

    async def _drive() -> None:
        loop = asyncio.get_running_loop()
        aconn, inner, pending = _make_async_connection_with_pending_loop(
            loop=loop, loop_closed=False
        )
        aconn.force_close_transport()
        pending.cancel.assert_called_once()
        assert aconn._async_conn is None

    asyncio.run(_drive())
