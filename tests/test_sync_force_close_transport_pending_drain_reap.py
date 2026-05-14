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
has the bounded re-snapshot reap; this restores
sync-vs-async parity.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import MagicMock

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
    """``force_close_transport`` must drive ``inner._pending_drain``
    toward cancellation before stopping the loop. The Task may end
    up in ``cancelling`` (cancel was requested but the task hasn't
    yet stepped through asyncio.sleep's checkpoint to fully
    transition) or in ``cancelled`` / ``done`` (the loop processed
    the step before stop). Both are acceptable: the cancel-and-observe
    done-callback registered in the production code suppresses the
    ``Task exception was never retrieved`` diagnostic regardless of
    which state the task ends in. What we MUST NOT see is a Task in
    plain ``pending`` state with no cancel armed."""
    conn, inner, loop, drain_holder = _build_conn_with_pending_drain_on_inner()
    drain_task = drain_holder[0]
    conn.force_close_transport()
    # Acceptable end-states: cancelling (cancel requested, step not
    # yet processed), cancelled, or done.
    cancelling = (
        getattr(drain_task, "_must_cancel", False) or "cancelling" in repr(drain_task).lower()
    )
    assert drain_task.cancelled() or drain_task.done() or cancelling, (
        f"drain task must be cancelled / done / cancelling after force_close_transport; "
        f"state: cancelled={drain_task.cancelled()}, done={drain_task.done()}, "
        f"repr={drain_task!r}"
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


def test_force_close_transport_resnapshot_loop_handles_concurrent_invalidate() -> None:
    """Pin: the bounded re-snapshot loop iterates up to 3 times when a
    concurrent ``_invalidate`` keeps publishing fresh
    ``_pending_drain`` tasks between the snapshot and the null. This
    matches the async sibling's cap-and-fail-loud discipline at
    ``aio/connection.py:842-911``.

    Driven by a fake inner whose ``_pending_drain`` getter returns a
    sequence of distinct task-shaped objects on each read, simulating
    the foreign-thread ``_invalidate`` that publishes a fresh task
    between our snapshot and null. With the bounded loop, each fresh
    task is observed and cancelled (up to the cap)."""
    conn = Connection("localhost:9001", database="x")
    loop = conn._ensure_loop()
    assert loop is not None

    cancel_calls: list[object] = []

    class _FakeTask:
        def __init__(self, name: str) -> None:
            self._name = name
            self._cancelled = False

        def done(self) -> bool:
            # Always not-done so the loop tries to cancel each one.
            return False

        def cancel(self) -> bool:
            self._cancelled = True
            cancel_calls.append(self._name)
            return True

        def add_done_callback(self, _cb: object) -> None:
            pass

    class _SequencingInner:
        def __init__(self) -> None:
            self._tasks = iter([_FakeTask(f"drain-{i}") for i in range(5)])
            self._closed = False
            self._protocol = None

        @property
        def _pending_drain(self) -> object:
            try:
                return next(self._tasks)
            except StopIteration:
                return None

        @_pending_drain.setter
        def _pending_drain(self, _value: object) -> None:
            # The null-out is a no-op for the test fake — the next read
            # auto-yields the next task in the sequence (simulating a
            # concurrent _invalidate publishing a fresh task between
            # our snapshot and our null).
            pass

    inner = _SequencingInner()
    conn._async_conn = inner  # type: ignore[assignment]

    conn.force_close_transport()

    # The cap is 3 → up to 3 cancellations attempted.
    assert len(cancel_calls) <= 3, (
        f"resnapshot cap should bound to 3 iterations; got {len(cancel_calls)} cancels"
    )
    assert len(cancel_calls) >= 1, "at least one iteration must run when pending is set"
