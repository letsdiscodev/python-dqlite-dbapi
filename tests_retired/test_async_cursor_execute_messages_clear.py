"""Async ``Cursor.execute`` clears ``messages`` per PEP 249 §6.1.2 on every entry. The single
pre-lock clear runs unconditionally, including the closed-path raise."""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError


@pytest.fixture
async def cursor() -> AsyncIterator[AsyncCursor]:
    """AsyncCursor whose parent is post-``_ensure_locks`` with a mocked underlying client."""
    conn = AsyncConnection("localhost:9001", database="x")
    loop = asyncio.get_running_loop()
    conn._loop_ref = weakref.ref(loop)
    conn._connect_lock = asyncio.Lock()
    conn._op_lock = asyncio.Lock()
    fake = MagicMock()
    fake.execute = AsyncMock(return_value=([], 0))
    fake.close = AsyncMock()
    fake.in_transaction = False
    conn._async_conn = fake
    cur = AsyncCursor(conn)
    try:
        yield cur
    finally:
        # close() must still run on an already-closed cursor to release the parent back-reference.
        with contextlib.suppress(Exception):
            cur.close()


async def test_execute_clears_messages_on_closed_path(cursor: AsyncCursor) -> None:
    """``messages`` is cleared even on the closed-cursor raise path (the pre-lock clear); moving it
    into the lock-protected body would regress this PEP 249 §6.1.2 branch."""
    cursor._closed = True
    cursor.messages.append((RuntimeError, RuntimeError("leftover")))
    with pytest.raises(InterfaceError):
        await cursor.execute("SELECT 1")
    assert cursor.messages == []


async def test_executemany_clears_messages_on_closed_path(
    cursor: AsyncCursor,
) -> None:
    """Same contract for ``executemany``."""
    cursor._closed = True
    cursor.messages.append((RuntimeError, RuntimeError("leftover")))
    with pytest.raises(InterfaceError):
        await cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert cursor.messages == []
