"""Pin: ``AsyncConnection.force_close_transport`` is a public, synchronous,
idempotent, never-raising last-resort cleanup hook (SA's non-greenlet
finalize path, GC sweep with no event loop)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


def test_force_close_transport_calls_writer_close() -> None:
    """The hook walks _async_conn → _protocol → _writer and calls writer.close()."""
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
    """Multiple invocations are safe; the first call nulls the inner reference
    so re-entries short-circuit and writer.close() runs exactly once."""
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

    assert writer.close.call_count == 1
    assert conn._async_conn is None


def test_force_close_transport_handles_missing_async_conn() -> None:
    """A connection never opened (or already closed) absorbs the call."""
    conn = AsyncConnection("localhost:9001", database="x")
    assert conn._async_conn is None
    conn.force_close_transport()


def test_force_close_transport_handles_missing_protocol() -> None:
    """An inner connection without ``_protocol`` absorbs the call."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock(spec=[])  # no attributes
    conn._async_conn = inner
    conn.force_close_transport()


def test_force_close_transport_swallows_writer_close_exception() -> None:
    """``writer.close()`` raising must not propagate; cleanup always finishes."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    writer.close.side_effect = OSError("transport already closed")
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner

    conn.force_close_transport()
    writer.close.assert_called_once_with()


async def test_force_close_transport_concurrent_with_async_close() -> None:
    """Invoking the sync hook while an async ``close()`` is in flight on the
    same connection must not raise; both converge on ``writer.close()``."""
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

    conn._ensure_locks()

    # Park the async close inside its first await.
    close_task = asyncio.create_task(conn.close())
    await asyncio.sleep(0)

    conn.force_close_transport()

    await close_task

    assert writer.close.call_count >= 1, (
        "writer.close must be called at least once across the two "
        "convergent paths; idempotence ensures multiple calls are safe"
    )
    assert close_task.done() and close_task.exception() is None


async def test_force_close_transport_cancels_inner_pending_drain() -> None:
    """The sync helper cannot await ``inner._pending_drain`` so it must cancel
    and null it, else Python logs "Task was destroyed but it is pending" at
    loop teardown. Also nulls ``self._async_conn`` (parity with the fork branch)."""
    import asyncio

    conn = AsyncConnection("localhost:9001", database="x")

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto

    async def _stuck() -> None:
        await asyncio.sleep(60)

    pending = asyncio.create_task(_stuck())
    inner._pending_drain = pending
    conn._async_conn = inner

    conn.force_close_transport()

    # Pump the loop so the cancel can land on the task.
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
