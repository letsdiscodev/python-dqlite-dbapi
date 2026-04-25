"""Pin: async ``commit`` / ``rollback`` clear ``messages`` *inside* ``op_lock``.

PEP 249 §6.1.1 says ``Connection.messages`` is cleared by every method
call on the connection. Clearing pre-lock leaves a window where a
sibling task can append a message between the clear and the operation
that the PEP 249 contract says belongs to *this* method call.

Pin the contract: while a parked ``commit`` / ``rollback`` waits on
``op_lock`` (held by another task), an append to ``self.messages`` made
by an external observer survives across the wait — but is cleared by
the parked method's ``commit`` / ``rollback`` *as soon as it acquires
the lock*, before the COMMIT/ROLLBACK SQL runs.
"""

from __future__ import annotations

import asyncio
import weakref
from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


async def _prime() -> AsyncConnection:
    """Build an AsyncConnection in the post-_ensure_locks state on the
    currently-running loop, with a mocked underlying client."""
    conn = AsyncConnection("localhost:9001", database="x")
    loop = asyncio.get_running_loop()
    conn._loop_ref = weakref.ref(loop)
    conn._connect_lock = asyncio.Lock()
    conn._op_lock = asyncio.Lock()
    fake = MagicMock()
    fake.execute = AsyncMock(return_value=(0, 0))
    fake.close = AsyncMock()
    conn._async_conn = fake
    return conn


class TestAsyncCommitMessagesUnderLock:
    async def test_messages_cleared_under_lock(self) -> None:
        conn = await _prime()
        # Hold op_lock from a control task to park the commit.
        await conn._op_lock.acquire()  # type: ignore[union-attr]
        commit_task = asyncio.create_task(conn.commit())
        # Let commit() park; it should NOT have cleared messages yet
        # (clear is now inside the lock).
        await asyncio.sleep(0)
        conn.messages.append((RuntimeError, "synthetic"))
        assert conn.messages == [(RuntimeError, "synthetic")]
        # Release the lock; commit() resumes, clears messages, runs COMMIT.
        conn._op_lock.release()  # type: ignore[union-attr]
        await commit_task
        assert conn.messages == []

    async def test_messages_cleared_on_no_op_path(self) -> None:
        """``_async_conn is None`` is the never-connected fast path; the
        clear runs there too so callers see consistent semantics."""
        conn = AsyncConnection("localhost:9001", database="x")
        # No async conn assigned — never connected.
        conn.messages.append((RuntimeError, "leftover"))
        await conn.commit()
        assert conn.messages == []


class TestAsyncRollbackMessagesUnderLock:
    async def test_messages_cleared_under_lock(self) -> None:
        conn = await _prime()
        await conn._op_lock.acquire()  # type: ignore[union-attr]
        rollback_task = asyncio.create_task(conn.rollback())
        await asyncio.sleep(0)
        conn.messages.append((RuntimeError, "synthetic"))
        assert conn.messages == [(RuntimeError, "synthetic")]
        conn._op_lock.release()  # type: ignore[union-attr]
        await rollback_task
        assert conn.messages == []

    async def test_messages_cleared_on_no_op_path(self) -> None:
        conn = AsyncConnection("localhost:9001", database="x")
        conn.messages.append((RuntimeError, "leftover"))
        await conn.rollback()
        assert conn.messages == []
