"""async commit/rollback re-read in_transaction under op_lock so a sibling that
just committed under the same lock cannot leave a stale True that routes into a
redundant round-trip."""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


@pytest.fixture
async def conn() -> AsyncIterator[AsyncConnection]:
    c = AsyncConnection("localhost:9001", database="x")
    loop = asyncio.get_running_loop()
    c._loop_ref = weakref.ref(loop)
    c._connect_lock = asyncio.Lock()
    c._op_lock = asyncio.Lock()
    fake = MagicMock()
    fake.execute = AsyncMock(return_value=(0, 0))
    fake.close = AsyncMock()
    fake.in_transaction = True
    c._async_conn = fake
    yield c


async def test_commit_rereads_in_transaction_under_lock(
    conn: AsyncConnection,
) -> None:
    fake: Any = conn._async_conn
    assert fake is not None
    assert conn._op_lock is not None
    await conn._op_lock.acquire()
    try:
        commit_task = asyncio.create_task(conn.commit())
        await asyncio.sleep(0)  # let commit() park on op_lock
        fake.in_transaction = False  # sibling decision: transaction is over
    finally:
        conn._op_lock.release()
    await commit_task
    assert not fake.execute.called, (
        "commit must re-read in_transaction under the lock; the parked "
        "task was holding a stale True read"
    )


async def test_rollback_rereads_in_transaction_under_lock(
    conn: AsyncConnection,
) -> None:
    fake: Any = conn._async_conn
    assert fake is not None
    assert conn._op_lock is not None
    await conn._op_lock.acquire()
    try:
        rollback_task = asyncio.create_task(conn.rollback())
        await asyncio.sleep(0)
        fake.in_transaction = False
    finally:
        conn._op_lock.release()
    await rollback_task
    assert not fake.execute.called


async def test_commit_executes_when_in_transaction_stays_true(
    conn: AsyncConnection,
) -> None:
    """in_transaction True both outside and inside the lock: commit issues COMMIT."""
    fake: Any = conn._async_conn
    assert fake is not None
    await conn.commit()
    fake.execute.assert_awaited_with("COMMIT")
