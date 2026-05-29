"""Sync ``force_close_transport`` reaps any pending ``inner._pending_drain`` task before
``loop.stop()``/``loop.close()``; otherwise ``Task.__del__`` emits "Task was destroyed but
it is pending" and the frame keeps the StreamReader/StreamWriter alive."""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import MagicMock

from dqlitedbapi.connection import Connection


def _build_conn_with_pending_drain_on_inner() -> tuple[
    Connection, MagicMock, asyncio.AbstractEventLoop, list[Any]
]:
    """Connection wired to a live loop+inner where ``inner._pending_drain`` is a real
    ``asyncio.Task`` parked on a long sleep — the shape an ``_invalidate`` drain takes."""
    conn = Connection("localhost:9001", database="x")
    loop = conn._ensure_loop()
    assert loop is not None

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto

    # Tasks must be constructed on their owning loop, so create it via
    # call_soon_threadsafe on the loop thread.
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
    deadline = time.monotonic() + 1.0
    while not drain_holder and time.monotonic() < deadline:
        time.sleep(0.01)
    assert drain_holder, "drain task was not created on the loop thread"
    inner._pending_drain = drain_holder[0]

    conn._async_conn = inner
    return conn, inner, loop, drain_holder


def test_force_close_transport_cancels_inner_pending_drain() -> None:
    """``force_close_transport`` drives ``inner._pending_drain`` toward cancellation before
    stopping the loop. Acceptable end-states: cancelling, cancelled, or done — but never plain
    ``pending`` with no cancel armed."""
    conn, inner, loop, drain_holder = _build_conn_with_pending_drain_on_inner()
    drain_task = drain_holder[0]
    conn.force_close_transport()
    cancelling = (
        getattr(drain_task, "_must_cancel", False) or "cancelling" in repr(drain_task).lower()
    )
    assert drain_task.cancelled() or drain_task.done() or cancelling, (
        f"drain task must be cancelled / done / cancelling after force_close_transport; "
        f"state: cancelled={drain_task.cancelled()}, done={drain_task.done()}, "
        f"repr={drain_task!r}"
    )
    assert inner._pending_drain is None


def test_force_close_transport_no_pending_drain_is_noop() -> None:
    """When ``inner._pending_drain`` is None the reap is a no-op."""
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
    """The bounded re-snapshot loop iterates up to 3 times when a concurrent ``_invalidate``
    keeps publishing fresh ``_pending_drain`` tasks between the snapshot and the null."""
    conn = Connection("localhost:9001", database="x")
    loop = conn._ensure_loop()
    assert loop is not None

    cancel_calls: list[object] = []

    class _FakeTask:
        def __init__(self, name: str) -> None:
            self._name = name
            self._cancelled = False

        def done(self) -> bool:
            return False  # never done, so the loop tries to cancel each one

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
            # No-op null-out: the next read auto-yields the next task,
            # simulating a concurrent _invalidate publishing between
            # snapshot and null.
            pass

    inner = _SequencingInner()
    conn._async_conn = inner  # type: ignore[assignment]

    conn.force_close_transport()

    assert len(cancel_calls) <= 3, (
        f"resnapshot cap should bound to 3 iterations; got {len(cancel_calls)} cancels"
    )
    assert len(cancel_calls) >= 1, "at least one iteration must run when pending is set"
