"""Pin: async ``commit`` / ``rollback`` re-read ``in_transaction``
*under* ``op_lock`` so a sibling task that just COMMITted/ROLLBACKed
under the same lock cannot leave us with a stale ``True`` read that
routes into a redundant round-trip.

Reading ``in_transaction`` outside the lock left a window where:

  T1: enters ``commit``, sees ``in_transaction = True``
  T2: under the lock, COMMITs (sets in_transaction=False)
  T1: acquires the lock, executes COMMIT — but server says "no
       transaction is active", which the wrap silences as a no-op

The execution is functionally a no-op (the wrap silences "no tx"),
but the wasted RTT is observable and the loose structure is what
ISSUE-740 flagged. Reading inside the lock closes the window.
"""

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
    """An AsyncConnection in the post-``_ensure_locks`` state with a
    mocked underlying client."""
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
    """While ``commit`` is parked on ``op_lock`` (held externally), a
    sibling clearing ``in_transaction`` to False should be observed
    when ``commit`` resumes — no redundant COMMIT round-trip.
    """
    fake: Any = conn._async_conn
    assert fake is not None
    assert conn._op_lock is not None
    await conn._op_lock.acquire()
    try:
        commit_task = asyncio.create_task(conn.commit())
        await asyncio.sleep(0)  # let commit() park on op_lock
        # Sibling decision: transaction is over.
        fake.in_transaction = False
    finally:
        conn._op_lock.release()
    await commit_task
    # No COMMIT call was issued because the post-acquire re-read saw
    # in_transaction=False.
    assert not fake.execute.called, (
        "commit must re-read in_transaction under the lock; the parked "
        "task was holding a stale True read"
    )


async def test_rollback_rereads_in_transaction_under_lock(
    conn: AsyncConnection,
) -> None:
    """Same as commit pin but for rollback."""
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
    """Regression pin: when ``in_transaction`` is True both outside
    and inside the lock, commit issues the COMMIT round-trip
    normally."""
    fake: Any = conn._async_conn
    assert fake is not None
    await conn.commit()
    fake.execute.assert_awaited_with("COMMIT")
