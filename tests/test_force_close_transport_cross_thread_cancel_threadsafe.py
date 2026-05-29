"""``force_close_transport`` schedules ``Task.cancel`` via ``call_soon_threadsafe`` from
a foreign thread; direct cancel only when the loop is closed or this thread owns it.

``Task.cancel()`` is not thread-safe (CPython routes it via the non-threadsafe
``loop.call_soon``)."""

from __future__ import annotations

import asyncio
import os
import threading
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


def _make_async_connection_with_pending_loop(
    *, loop: asyncio.AbstractEventLoop, loop_closed: bool
) -> tuple[AsyncConnection, MagicMock, MagicMock]:
    """AsyncConnection whose inner._pending_drain is a Mock Task with ``get_loop() -> loop``."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = None
    aconn._closed_flag = [False]

    inner = MagicMock()
    inner._protocol = None  # cleanup-tail-only branch

    pending = MagicMock()
    pending.done.return_value = False
    pending.cancel = MagicMock()
    pending.get_loop.return_value = loop
    inner._pending_drain = pending

    if loop_closed:
        loop.close()

    aconn._async_conn = inner
    return aconn, inner, pending


def test_cancel_called_directly_when_loop_closed() -> None:
    """Loop gone (SA finalize / atexit / GC): cancel falls back to a direct call."""
    loop = asyncio.new_event_loop()
    aconn, inner, pending = _make_async_connection_with_pending_loop(loop=loop, loop_closed=True)

    aconn.force_close_transport()

    pending.cancel.assert_called_once()
    assert aconn._async_conn is None


def test_cancel_scheduled_via_call_soon_threadsafe_from_foreign_thread() -> None:
    """Foreign thread, live loop: cancel scheduled via call_soon_threadsafe, not direct."""
    loop = asyncio.new_event_loop()

    aconn, inner, pending = _make_async_connection_with_pending_loop(loop=loop, loop_closed=False)

    cstu_calls: list[tuple[object, tuple[object, ...]]] = []
    real_cstu = loop.call_soon_threadsafe

    def _capture_cstu(callback: object, *args: object) -> object:
        cstu_calls.append((callback, args))
        return real_cstu(callback, *args)  # type: ignore[arg-type]

    loop.call_soon_threadsafe = _capture_cstu  # type: ignore[assignment]

    def _run_from_foreign_thread() -> None:
        aconn.force_close_transport()

    t = threading.Thread(target=_run_from_foreign_thread)
    t.start()
    t.join(timeout=2.0)
    assert not t.is_alive()

    pending.cancel.assert_not_called()
    assert len(cstu_calls) == 1
    callback, args = cstu_calls[0]
    # Scheduled callback is a wrapper that cancels and absorbs the CancelledError
    # (else asyncio logs "Task exception was never retrieved" at GC); arg is the task.
    assert callable(callback)
    assert args == (pending,)
    assert aconn._async_conn is None

    loop.close()


def test_cancel_called_directly_on_owning_thread() -> None:
    """Live loop owned by the current thread: cancel runs directly."""

    async def _drive() -> None:
        loop = asyncio.get_running_loop()
        aconn, inner, pending = _make_async_connection_with_pending_loop(
            loop=loop, loop_closed=False
        )
        aconn.force_close_transport()
        pending.cancel.assert_called_once()
        assert aconn._async_conn is None

    asyncio.run(_drive())
