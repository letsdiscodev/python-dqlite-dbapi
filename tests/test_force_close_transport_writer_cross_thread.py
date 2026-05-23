"""Pin: ``AsyncConnection.force_close_transport`` routes
``writer.close()`` through ``call_soon_threadsafe`` when the bound
loop is alive on a foreign thread.

The typical SA do_terminate / pool.dispose chain calls this method
from a non-loop thread while the bound loop is still alive in
another thread. ``StreamWriter.close()`` mutates
``_SelectorSocketTransport`` state and is documented as not
thread-safe; a direct call from the foreign thread races the
selector's transport-state bookkeeping. The pending-drain cancel
already routed through ``call_soon_threadsafe`` for the same
reason; this pin closes the asymmetric writer.close gap.
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


def test_force_close_transport_schedules_writer_close_on_owning_thread() -> None:
    """When the bound loop is alive on a foreign thread, the
    writer.close() call must be deferred via
    call_soon_threadsafe rather than running directly on the
    caller's thread."""
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()

    try:
        # Build an AsyncConnection bound to `loop`.
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
        # Track which thread close() ran on.
        close_thread: list[int] = []

        def _close_capture() -> None:
            close_thread.append(threading.get_ident())

        writer.close = _close_capture
        inner._protocol._writer = writer
        conn._async_conn = inner

        # Invoke force_close_transport from THIS thread (not the
        # loop thread). The writer.close should run on the loop
        # thread via call_soon_threadsafe.
        caller_thread = threading.get_ident()
        assert caller_thread != loop_thread.ident
        conn.force_close_transport()

        # Give the loop a chance to drain the scheduled callback.
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
    """When the bound loop is already closed, writer.close runs
    directly on the caller's thread (call_soon_threadsafe would
    fail). This is the typical finalize / atexit / GC path."""
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

    # Loop is dead; writer.close runs directly.
    assert writer.close.called
