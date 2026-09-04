"""async commit/rollback clear messages inside op_lock (PEP 249 §6.1.1): clearing
pre-lock would let a sibling's append fall under this method's call."""

from __future__ import annotations

import asyncio
import weakref
from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.aio.connection import AsyncConnection

_RUNTIMEERROR_SYNTHETIC: tuple[type[Exception], Exception] = (
    RuntimeError,
    RuntimeError("synthetic"),
)
_RUNTIMEERROR_LEFTOVER: tuple[type[Exception], Exception] = (RuntimeError, RuntimeError("leftover"))


async def _prime() -> AsyncConnection:
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
        await conn._op_lock.acquire()  # type: ignore[union-attr]  # park the commit
        commit_task = asyncio.create_task(conn.commit())
        # commit() parks here; it must NOT have cleared messages yet.
        await asyncio.sleep(0)
        conn.messages.append(_RUNTIMEERROR_SYNTHETIC)
        assert conn.messages == [_RUNTIMEERROR_SYNTHETIC]
        conn._op_lock.release()  # type: ignore[union-attr]
        await commit_task
        assert conn.messages == []

    async def test_messages_cleared_on_no_op_path(self) -> None:
        """Clear also runs on the never-connected (_async_conn is None) fast path."""
        conn = AsyncConnection("localhost:9001", database="x")
        conn.messages.append(_RUNTIMEERROR_LEFTOVER)
        await conn.commit()
        assert conn.messages == []


class TestAsyncRollbackMessagesUnderLock:
    async def test_messages_cleared_under_lock(self) -> None:
        conn = await _prime()
        await conn._op_lock.acquire()  # type: ignore[union-attr]
        rollback_task = asyncio.create_task(conn.rollback())
        await asyncio.sleep(0)
        conn.messages.append(_RUNTIMEERROR_SYNTHETIC)
        assert conn.messages == [_RUNTIMEERROR_SYNTHETIC]
        conn._op_lock.release()  # type: ignore[union-attr]
        await rollback_task
        assert conn.messages == []

    async def test_messages_cleared_on_no_op_path(self) -> None:
        conn = AsyncConnection("localhost:9001", database="x")
        conn.messages.append(_RUNTIMEERROR_LEFTOVER)
        await conn.rollback()
        assert conn.messages == []
