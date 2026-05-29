"""Pin: ``force_close_transport`` routes ``writer.close()`` through
``call_soon_threadsafe`` when the bound loop is alive on a foreign thread.
``StreamWriter.close()`` is not thread-safe — a direct foreign-thread call
races the selector's transport-state bookkeeping.
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


def test_force_close_transport_schedules_writer_close_on_owning_thread() -> None:
    """Bound loop alive on a foreign thread: writer.close() is deferred via
    call_soon_threadsafe, not run on the caller's thread."""
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()

    try:
        conn = AsyncConnection.__new__(AsyncConnection)
        conn._closed = False
        conn._closed_flag = [False]
        conn._timeout = 1.0
        conn._close_timeout = 1.0
        conn.messages = []
        conn._transaction_owner = None
        import os

        conn._creator_pid = os.getpid()
        conn._cursors = weakref.WeakSet()
        conn._finalizer = None
        conn._op_lock = None
        conn._connect_lock = None
        conn._loop_ref = weakref.ref(loop)

        inner = MagicMock()
        inner._closed_flag = [False]
        inner._finalizer = None
        inner._pending_drain = None
        inner._protocol = MagicMock()
        writer = MagicMock()
        close_thread: list[int] = []

        def _close_capture() -> None:
            close_thread.append(threading.get_ident())

        writer.close = _close_capture
        inner._protocol._writer = writer
        conn._async_conn = inner

        # Invoke from this thread (not the loop thread); writer.close should
        # run on the loop thread via call_soon_threadsafe.
        caller_thread = threading.get_ident()
        assert caller_thread != loop_thread.ident
        conn.force_close_transport()

        for _ in range(50):
            if close_thread:
                break
            threading.Event().wait(0.01)

        assert close_thread, "writer.close was never invoked"
        assert close_thread[0] == loop_thread.ident, (
            f"writer.close ran on the caller's thread ({caller_thread}) "
            f"instead of the loop-owning thread ({loop_thread.ident}); "
            f"cross-thread selector mutation race."
        )
    finally:
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=2.0)
        if not loop.is_closed():
            loop.close()


def test_force_close_transport_direct_close_on_dead_loop() -> None:
    """Bound loop already closed: writer.close runs directly on the caller's
    thread (call_soon_threadsafe would fail). The finalize / atexit / GC path."""
    loop = asyncio.new_event_loop()
    loop.close()

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._timeout = 1.0
    conn._close_timeout = 1.0
    conn.messages = []
    conn._transaction_owner = None
    import os

    conn._creator_pid = os.getpid()
    conn._cursors = weakref.WeakSet()
    conn._finalizer = None
    conn._op_lock = None
    conn._connect_lock = None
    conn._loop_ref = weakref.ref(loop)

    inner = MagicMock()
    inner._closed_flag = [False]
    inner._finalizer = None
    inner._pending_drain = None
    inner._protocol = MagicMock()
    writer = MagicMock()
    writer.close = MagicMock()
    inner._protocol._writer = writer
    conn._async_conn = inner

    conn.force_close_transport()

    assert writer.close.called
