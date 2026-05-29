"""commit/rollback force-close the transport on an outer cancel mid-round-trip,
since SA's is_disconnect classifier does not recognise raw CancelledError and
the slot would otherwise stay pooled with ambiguous server-side state."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _bare_conn() -> Any:
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()  # bind to current loop
    inner = MagicMock()
    inner._protocol = MagicMock()
    inner.in_transaction = True
    inner.execute = MagicMock()
    conn._async_conn = inner
    return conn


@pytest.mark.asyncio
async def test_commit_force_closes_on_outer_cancel_during_wire_call() -> None:
    conn = _bare_conn()

    inner_event = asyncio.Event()

    async def _hang_forever() -> None:
        await inner_event.wait()

    conn._async_conn.execute = MagicMock(return_value=_hang_forever())
    conn.force_close_transport = MagicMock()

    task = asyncio.create_task(conn.commit())
    await asyncio.sleep(0.05)  # let the task reach the inner await
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    conn.force_close_transport.assert_called_once_with()


@pytest.mark.asyncio
async def test_rollback_force_closes_on_outer_cancel_during_wire_call() -> None:
    conn = _bare_conn()
    inner_event = asyncio.Event()

    async def _hang_forever() -> None:
        await inner_event.wait()

    conn._async_conn.execute = MagicMock(return_value=_hang_forever())
    conn.force_close_transport = MagicMock()

    task = asyncio.create_task(conn.rollback())
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    conn.force_close_transport.assert_called_once_with()


@pytest.mark.asyncio
async def test_commit_does_not_force_close_on_normal_success() -> None:
    """Happy path must NOT trigger force-close."""
    conn = _bare_conn()
    conn._async_conn.execute = MagicMock(return_value=AsyncMock()())
    conn.force_close_transport = MagicMock()

    await conn.commit()

    conn.force_close_transport.assert_not_called()


@pytest.mark.asyncio
async def test_commit_does_not_force_close_on_cancel_before_lock_acquire() -> None:
    """A cancel BEFORE the wire call begins must NOT force-close: no wire
    state was partial (request_in_flight stays False)."""
    conn = _bare_conn()
    conn.force_close_transport = MagicMock()

    # Hold op_lock so commit() blocks on lock acquire indefinitely.
    _, op_lock = conn._ensure_locks()
    await op_lock.acquire()
    try:
        task = asyncio.create_task(conn.commit())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        op_lock.release()

    conn.force_close_transport.assert_not_called()
