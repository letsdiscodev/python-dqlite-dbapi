"""Pin: ``AsyncConnection.force_close_transport`` from a foreign
thread synchronously pre-sets ``cur._closed = True`` on every
tracked cursor BEFORE deferring the full cursor-state scrub to the
bound loop via ``call_soon_threadsafe``. Without this pre-set, a
sibling task on the bound loop could observe ``conn._closed=True``
while ``cur._closed=False`` for the duration of one
``call_soon_threadsafe`` round-trip — a sibling ``fetchone()``
returned rows from a dead transport in that window.

The cursor scrub itself (description/rows/rowcount/...) remains
deferred to the loop callback because the sequence is not
GIL-atomic — but ``_closed`` is a single attribute write, so the
pre-set is safe and closes the observability gap.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _make_conn_with_loop_ref(
    loop: asyncio.AbstractEventLoop, cursors: list[object]
) -> AsyncConnection:
    import weakref

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed_flag = [False]
    conn._closed = False
    conn._async_conn = None
    conn._finalizer = None
    conn.messages = []
    # Pid-check uses getattr-default; leave at default by not setting it
    conn._address = "host:1234"
    conn._loop_ref = weakref.ref(loop)
    cursors_mock = MagicMock()
    cursors_mock.__iter__ = lambda self: iter(cursors)
    cursors_mock.clear = MagicMock()
    conn._cursors = cursors_mock
    return conn


class _FakeCursor:
    __slots__ = (
        "_closed",
        "_connection",
        "_description",
        "_finalizer",
        "_lastrowid",
        "_row_index",
        "_rowcount",
        "_rows",
        "messages",
    )

    def __init__(self) -> None:
        self._closed = False
        self._rows: list[tuple[object, ...]] = [("prior",)]
        self._description: tuple[tuple[object, ...], ...] | None = (
            ("col", 1, None, None, None, None, None),
        )
        self._rowcount = 1
        self._lastrowid = 5
        self._row_index = 0
        self.messages: list[object] = []
        self._connection = MagicMock()
        self._finalizer = None


@pytest.mark.asyncio
async def test_force_close_foreign_thread_pre_sets_cursor_closed() -> None:
    """Foreign-thread force_close_transport must leave every tracked
    cursor with ``_closed = True`` SYNCHRONOUSLY before the join
    returns, even though the cursor-scrub cascade is deferred via
    ``call_soon_threadsafe``.

    We arrange the routing so the deferred-path is taken: a real
    bound loop is alive on this thread, the foreign thread calls
    force_close_transport — the cascade goes into call_soon_threadsafe
    and only runs when this test's loop pumps next. The pre-set must
    have made ``cur._closed`` True before then.
    """
    cur = _FakeCursor()
    bound_loop = asyncio.get_running_loop()
    conn = _make_conn_with_loop_ref(bound_loop, [cur])

    # Block this loop from pumping during the foreign-thread call by
    # using a sync threading event the foreign thread releases AFTER
    # force_close_transport returns. We sleep on a thread-pool task
    # waiting on the event so the loop has work to do, but the
    # cascade scheduled into call_soon_threadsafe will NOT run until
    # the foreign thread already returned.
    foreign_returned = threading.Event()
    seen_cur_closed_before_cascade_ran = []

    def call_from_foreign_thread() -> None:
        conn.force_close_transport()
        # Sample cur._closed AFTER force_close_transport returned but
        # BEFORE this thread releases the event that lets the bound
        # loop pump the deferred cascade.
        seen_cur_closed_before_cascade_ran.append(cur._closed)
        foreign_returned.set()

    t = threading.Thread(target=call_from_foreign_thread)
    t.start()
    # Wait for the foreign thread to capture its sample.
    await asyncio.get_running_loop().run_in_executor(None, foreign_returned.wait, 5.0)
    t.join(timeout=5.0)

    # The foreign thread's sample is the source of truth: at that
    # moment the cascade had been call_soon_threadsafe'd but had not
    # yet run (we hadn't yielded back to the loop). cur._closed must
    # already be True due to the synchronous pre-set.
    assert seen_cur_closed_before_cascade_ran == [True], (
        "force_close_transport must pre-set cur._closed = True "
        "synchronously, before the deferred cascade runs; "
        f"sample = {seen_cur_closed_before_cascade_ran!r}"
    )
