"""Pin: sync ``Connection.force_close_transport`` reaps any pending
``inner._pending_drain`` task before calling ``loop.stop()`` and
``loop.close()``.

Without the reap, a prior ``_invalidate`` (e.g. one scheduled by
``_run_sync`` on a sync timeout) leaves a Task on the inner conn
that is still in flight when the loop closes — ``Task.__del__``
then emits "Task was destroyed but it is pending" via asyncio's
exception handler, and the coroutine frame keeps the
StreamReader/StreamWriter referenced.

The async sibling ``AsyncConnection.force_close_transport`` already
has the bounded re-snapshot reap (cycle-27 R27_1); this restores
sync-vs-async parity.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _build_conn_with_pending_drain_on_inner() -> tuple[
    Connection, MagicMock, asyncio.AbstractEventLoop, list[Any]
]:
    """Build a Connection wired to a live loop+inner where
    ``inner._pending_drain`` is a real ``asyncio.Task`` parked on a
    long sleep — exactly the shape an ``_invalidate``-scheduled
    drain takes."""
    conn = Connection("localhost:9001", database="x")
    loop = conn._ensure_loop()
    assert loop is not None

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto

    # Park a real asyncio.Task on the loop thread that simulates the
    # invalidate-scheduled drain. ``call_soon_threadsafe`` is used to
    # create the Task on the loop thread (Tasks must be constructed
    # on their owning loop).
    drain_holder: list[Any] = []
    created = asyncio.Event()

    async def _long_drain() -> None:
        try:
            await asyncio.sleep(60.0)
        except asyncio.CancelledError:
            raise

    def _make_task() -> None:
        drain_holder.append(loop.create_task(_long_drain()))
        loop.call_soon_threadsafe(created.set)

    loop.call_soon_threadsafe(_make_task)
    # Wait briefly for the task creation to land.
    deadline = time.monotonic() + 1.0
    while not drain_holder and time.monotonic() < deadline:
        time.sleep(0.01)
    assert drain_holder, "drain task was not created on the loop thread"
    inner._pending_drain = drain_holder[0]

    conn._async_conn = inner
    return conn, inner, loop, drain_holder


def test_force_close_transport_cancels_inner_pending_drain() -> None:
    """``force_close_transport`` must cancel ``inner._pending_drain``
    before stopping the loop, so the Task does not orphan past
    ``loop.close()`` and emit a ``Task was destroyed`` diagnostic."""
    conn, inner, loop, drain_holder = _build_conn_with_pending_drain_on_inner()
    drain_task = drain_holder[0]
    conn.force_close_transport()
    # The drain task must have been cancelled (or completed) — not
    # left in the "pending" state when the loop closes.
    assert drain_task.cancelled() or drain_task.done(), (
        f"drain task must be cancelled or done after force_close_transport; "
        f"state: cancelled={drain_task.cancelled()}, done={drain_task.done()}"
    )
    # The slot was nulled.
    assert inner._pending_drain is None


def test_force_close_transport_no_pending_drain_is_noop() -> None:
    """Sanity: when ``inner._pending_drain`` is None, the reap is a
    no-op (no exception, normal path proceeds)."""
    conn = Connection("localhost:9001", database="x")
    loop = conn._ensure_loop()
    assert loop is not None
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    inner._pending_drain = None
    conn._async_conn = inner

    conn.force_close_transport()
    writer.close.assert_called_once_with()
    assert conn._async_conn is None


@pytest.mark.skip(reason="pure smoke for symmetric structure with async sibling pin")
def test_force_close_transport_resnapshot_documented() -> None:
    """The sync path uses a SINGLE-shot reap (not the bounded
    re-snapshot loop the async sibling uses) because the calling
    thread is going to issue ``loop.stop`` immediately after — there
    is no further window for a concurrent ``_invalidate`` to publish
    a fresh task. The async sibling needs the loop because it may be
    running on the loop thread itself, where a concurrent
    ``call_soon_threadsafe(_invalidate)`` from a foreign thread can
    interleave with the snapshot+null."""
    pass
