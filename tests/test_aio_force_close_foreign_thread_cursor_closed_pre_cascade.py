"""force_close_transport from a foreign thread synchronously pre-sets cur._closed=True
before deferring the full scrub via call_soon_threadsafe, closing the window where a
sibling task sees conn._closed=True but cur._closed=False (single-write _closed is safe).
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
    """Foreign-thread force_close_transport pre-sets cur._closed=True synchronously,
    before the deferred call_soon_threadsafe cascade runs on the bound loop."""
    cur = _FakeCursor()
    bound_loop = asyncio.get_running_loop()
    conn = _make_conn_with_loop_ref(bound_loop, [cur])

    # Foreign thread releases this event only AFTER force_close_transport returns,
    # so the deferred cascade cannot run before we sample cur._closed.
    foreign_returned = threading.Event()
    seen_cur_closed_before_cascade_ran = []

    def call_from_foreign_thread() -> None:
        conn.force_close_transport()
        seen_cur_closed_before_cascade_ran.append(cur._closed)
        foreign_returned.set()

    t = threading.Thread(target=call_from_foreign_thread)
    t.start()
    await asyncio.get_running_loop().run_in_executor(None, foreign_returned.wait, 5.0)
    t.join(timeout=5.0)

    assert seen_cur_closed_before_cascade_ran == [True], (
        "force_close_transport must pre-set cur._closed = True "
        "synchronously, before the deferred cascade runs; "
        f"sample = {seen_cur_closed_before_cascade_ran!r}"
    )
