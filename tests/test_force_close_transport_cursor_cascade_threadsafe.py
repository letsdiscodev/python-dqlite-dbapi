"""Pin: ``AsyncConnection.force_close_transport``'s cursor cascade
routes through ``loop.call_soon_threadsafe`` when called from a
foreign thread while the bound loop is alive.

Before the fix, the cursor cascade mutated each cursor's ``_closed``,
``_rows``, ``_description``, ``_rowcount``, ``_lastrowid``,
``_row_index``, ``messages``, and ``_connection`` attributes
directly from whatever thread invoked ``force_close_transport``.
Individual writes are GIL-atomic but the SEQUENCE is not -- a
sibling task parked mid-fetch on the bound loop could observe a
partially-cascaded snapshot on resume.

The matching writer-close path was already routing through
``call_soon_threadsafe``; the cursor cascade now mirrors that
discipline so the contract is uniform.
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection, _cascade_cursors_closed


def _bare_conn(loop: asyncio.AbstractEventLoop) -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._loop_ref = weakref.ref(loop)
    conn._async_conn = None  # no inner -- skip the transport path
    conn._cursors = weakref.WeakSet()
    conn._finalizer = None
    import os

    conn._creator_pid = os.getpid()
    return conn


def _stub_cursor(connection: AsyncConnection) -> Any:
    cur = MagicMock()
    cur._closed = False
    cur._rows = [(1,), (2,)]
    cur._description = (("col", None, None, None, None, None, None),)
    cur._rowcount = 2
    cur._lastrowid = None
    cur._row_index = 0
    cur.messages = []
    cur._connection = connection
    return cur


def test_cascade_helper_marks_cursors_closed_and_clears_state() -> None:
    """The extracted helper sets each cursor to closed-with-scrubbed
    result state in a single pass."""
    conn = MagicMock()
    cur = _stub_cursor(conn)
    _cascade_cursors_closed([cur])
    assert cur._closed is True
    assert cur._rows == []
    assert cur._description is None
    assert cur._rowcount == -1


def test_force_close_from_foreign_thread_schedules_cascade_via_threadsafe() -> None:
    """When called from a foreign thread while the bound loop is
    alive, the cursor cascade is scheduled via
    ``call_soon_threadsafe`` rather than mutating cursor state from
    the foreign thread directly."""
    loop = asyncio.new_event_loop()

    # Run the loop in its own thread so it's alive while we call
    # force_close from the main thread.
    loop_ready = threading.Event()
    loop_stop = threading.Event()
    stop_call: list[Any] = []

    def _run_loop() -> None:
        asyncio.set_event_loop(loop)
        loop_ready.set()
        # Wait until told to stop; the test schedules stop via
        # call_soon_threadsafe.
        while not loop_stop.is_set():
            loop.run_until_complete(asyncio.sleep(0.01))
        stop_call.append("done")

    t = threading.Thread(target=_run_loop, daemon=True)
    t.start()
    loop_ready.wait()

    try:
        conn = _bare_conn(loop)
        cur = _stub_cursor(conn)
        conn._cursors.add(cur)

        # Track whether call_soon_threadsafe was invoked on the loop.
        scheduled_calls: list[Any] = []
        real_cstsa = loop.call_soon_threadsafe

        def _wrap(callback, *args, **kw):
            scheduled_calls.append((callback, args))
            return real_cstsa(callback, *args, **kw)

        loop.call_soon_threadsafe = _wrap  # type: ignore[assignment]

        conn.force_close_transport()

        assert scheduled_calls, (
            "cursor cascade must route through call_soon_threadsafe when "
            "called from a foreign thread on a live loop"
        )
        # The scheduled callback is _cascade_cursors_closed with the
        # cursor list.
        callback, args = scheduled_calls[0]
        assert callback is _cascade_cursors_closed
        assert len(args[0]) == 1
        assert args[0][0] is cur

        # Wait for the scheduled callback to actually run on the loop.
        for _ in range(50):
            if cur._closed:
                break
            import time

            time.sleep(0.01)
        assert cur._closed is True
    finally:
        loop_stop.set()
        t.join(timeout=2.0)
        loop.close()


@pytest.mark.asyncio
async def test_force_close_from_owning_loop_runs_cascade_synchronously() -> None:
    """When called from the owning loop's thread, the cursor cascade
    runs synchronously -- no scheduling indirection."""
    loop = asyncio.get_running_loop()
    conn = _bare_conn(loop)
    cur = _stub_cursor(conn)
    conn._cursors.add(cur)

    # Track call_soon_threadsafe — it should NOT be invoked on the
    # owning-loop path.
    sentinel: list[Any] = []
    real = loop.call_soon_threadsafe

    def _wrap(callback, *args, **kw):
        sentinel.append(callback)
        return real(callback, *args, **kw)

    loop.call_soon_threadsafe = _wrap  # type: ignore[assignment]

    conn.force_close_transport()

    assert _cascade_cursors_closed not in sentinel
    assert cur._closed is True
