"""Pin: ``AsyncConnection.commit`` / ``AsyncConnection.rollback``
force-close the transport when an outer cancel / signal / timeout
lands while the wire round-trip is in flight.

Without the force-close, SA's ``is_disconnect`` classifier (which
substring-scans the rendered exception text on the next op) does
not recognise raw ``CancelledError``, so the slot stays in the
pool with ambiguous server-side state -- the COMMIT bytes may have
reached the leader, the Raft log entry may or may not be appended,
the response may or may not have been emitted.

The defensive ``try/except BaseException`` calls
``force_close_transport()`` when ``request_in_flight`` is True so
the slot is reaped on the next pool reclaim. Mirrors the close()
arm's force-close-on-cancel discipline.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _bare_conn() -> Any:
    """Construct an AsyncConnection with the minimum state to drive
    commit/rollback paths without dialling."""
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()  # bind to current loop
    # Fake the inner client so commit/rollback can reach the wire
    # call site.
    inner = MagicMock()
    inner._protocol = MagicMock()
    inner.in_transaction = True
    inner.execute = MagicMock()
    conn._async_conn = inner
    return conn


@pytest.mark.asyncio
async def test_commit_force_closes_on_outer_cancel_during_wire_call() -> None:
    """An outer cancel landing during the COMMIT round-trip must
    trigger force_close_transport()."""
    conn = _bare_conn()

    # Drive the wire call to hang forever, then cancel the task.
    inner_event = asyncio.Event()

    async def _hang_forever() -> None:
        await inner_event.wait()

    conn._async_conn.execute = MagicMock(return_value=_hang_forever())
    conn.force_close_transport = MagicMock()

    task = asyncio.create_task(conn.commit())
    # Yield so the task reaches the inner await.
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    conn.force_close_transport.assert_called_once_with()


@pytest.mark.asyncio
async def test_rollback_force_closes_on_outer_cancel_during_wire_call() -> None:
    """An outer cancel landing during the ROLLBACK round-trip must
    trigger force_close_transport()."""
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
    """Negative pin: the happy path must NOT trigger force-close."""
    conn = _bare_conn()
    conn._async_conn.execute = MagicMock(return_value=AsyncMock()())
    conn.force_close_transport = MagicMock()

    await conn.commit()

    conn.force_close_transport.assert_not_called()


@pytest.mark.asyncio
async def test_commit_does_not_force_close_on_cancel_before_lock_acquire() -> None:
    """Negative pin: a cancel landing BEFORE the wire call begins
    (lock acquire phase, fast-path checks) must NOT force-close --
    no wire state was partial. ``request_in_flight`` stays False."""
    conn = _bare_conn()
    conn.force_close_transport = MagicMock()

    # Hold the op_lock so commit() blocks on lock acquire indefinitely.
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
