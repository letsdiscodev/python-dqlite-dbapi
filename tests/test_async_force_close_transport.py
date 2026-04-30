"""Pin: ``AsyncConnection.force_close_transport`` is a public,
synchronous, idempotent, never-raising last-resort cleanup hook.

The SA dialect's async adapter calls this from its non-greenlet
finalize path (GC sweep with no event loop). Walking the
underlying client connection's private ``_protocol._writer``
chain from outside this package broke silently when the chain
shape changed; this hook is the single supported access boundary.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def test_force_close_transport_calls_writer_close() -> None:
    """The hook walks _async_conn → _protocol → _writer and calls
    writer.close()."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner

    conn.force_close_transport()

    writer.close.assert_called_once_with()


def test_force_close_transport_is_idempotent() -> None:
    """Multiple invocations are safe; subsequent calls after the
    first short-circuit on the now-None ``self._async_conn``. The
    writer's close() is invoked exactly once on the first call,
    not redundantly — the first call nulls the inner reference
    so re-entries no-op cleanly."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner

    conn.force_close_transport()
    conn.force_close_transport()
    conn.force_close_transport()

    # Idempotence (per docstring): "Multiple invocations are safe."
    # The post-fix discipline nulls ``self._async_conn`` after the
    # first call, so subsequent calls observe inner=None and return
    # immediately — still safe, no longer redundant.
    assert writer.close.call_count == 1
    assert conn._async_conn is None


def test_force_close_transport_handles_missing_async_conn() -> None:
    """A connection that was never opened (or already closed and
    nulled) absorbs the call without raising."""
    conn = AsyncConnection("localhost:9001", database="x")
    assert conn._async_conn is None  # never connected
    conn.force_close_transport()  # must not raise


def test_force_close_transport_handles_missing_protocol() -> None:
    """An inner connection without ``_protocol`` (mid-construction
    or already torn down) absorbs the call."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock(spec=[])  # no attributes
    conn._async_conn = inner
    conn.force_close_transport()  # must not raise


def test_force_close_transport_swallows_writer_close_exception() -> None:
    """``writer.close()`` raising must not propagate — last-resort
    cleanup must always finish."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    writer.close.side_effect = OSError("transport already closed")
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner

    conn.force_close_transport()  # must not raise
    writer.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_force_close_transport_concurrent_with_async_close() -> None:
    """Pin the docstring's concurrent-safety contract: invoking the
    sync hook while an async ``close()`` is in flight on the same
    connection must not raise. Both paths converge on
    ``writer.close()`` (idempotent on asyncio's StreamWriter).

    Setup:
      * Build an AsyncConnection in the post-_ensure_locks state.
      * The inner client conn's ``close()`` yields once via
        ``asyncio.sleep(0)`` so the async path reaches its first
        await before completing.
      * Start ``conn.close()`` as a task; let it park.
      * Invoke ``conn.force_close_transport()`` synchronously from
        the parent coroutine.
      * Resume the close_task; assert it finished cleanly.
    """
    import asyncio

    conn = AsyncConnection("localhost:9001", database="x")

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    inner.in_transaction = False

    async def slow_close() -> None:
        await asyncio.sleep(0)

    inner.close = AsyncMock(side_effect=slow_close)
    conn._async_conn = inner

    # Use the production lock-binding path. Any future enhancement to
    # ``_ensure_locks`` (loop-lifecycle validation, state-machine
    # flags, etc.) automatically applies to this test.
    conn._ensure_locks()

    # Park the async close inside its first await.
    close_task = asyncio.create_task(conn.close())
    await asyncio.sleep(0)

    # Synchronous hook from the same coroutine — must not raise.
    conn.force_close_transport()

    await close_task

    assert writer.close.call_count >= 1, (
        "writer.close must be called at least once across the two "
        "convergent paths; idempotence ensures multiple calls are safe"
    )
    assert close_task.done() and close_task.exception() is None


@pytest.mark.asyncio
async def test_force_close_transport_cancels_inner_pending_drain() -> None:
    """``force_close_transport`` is the synchronous fallback used by
    SA's non-greenlet finalize path. The canonical async ``close()``
    awaits ``inner._pending_drain`` to completion; the sync helper
    cannot await but MUST cancel and null the task — otherwise the
    drain task is orphaned on the loop and Python prints
    "Task was destroyed but it is pending" once the loop is torn
    down (which is the exact path SA's sync fallback runs on).

    Also pins the symmetric null-out: the fork branch of
    ``force_close_transport`` already nulls ``self._async_conn``;
    the regular sync writer-close path must do the same so the
    AsyncConnection does not pretend to still reference a dead
    inner conn.
    """
    import asyncio

    conn = AsyncConnection("localhost:9001", database="x")

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto

    # Synthesize a pending_drain task; mimics what _invalidate
    # would have set on the inner client at its last invalidation.
    async def _stuck() -> None:
        await asyncio.sleep(60)

    pending = asyncio.create_task(_stuck())
    inner._pending_drain = pending
    conn._async_conn = inner

    conn.force_close_transport()

    # Pump the loop briefly so the cancel can land on the task.
    for _ in range(3):
        await asyncio.sleep(0)

    assert pending.cancelled() or pending.done(), (
        "force_close_transport must cancel inner._pending_drain — the "
        "sync helper has no loop to await on, so the task must be "
        "explicitly reaped or it dangles on the loop's task list."
    )
    assert inner._pending_drain is None, (
        "force_close_transport must null inner._pending_drain after "
        "cancelling so a later finalize / GC pass does not log the "
        "stale reference."
    )
    assert conn._async_conn is None, (
        "force_close_transport's regular sync writer-close path must "
        "null self._async_conn — matching the fork branch's discipline."
    )
