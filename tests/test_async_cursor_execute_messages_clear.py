"""Pin: async ``Cursor.execute`` clears ``messages`` per PEP 249
§6.1.2 on every entry.

The cursor side has only ONE messages-clear (pre-lock) — unlike the
connection's commit/rollback which have an in-lock clear too. This is
deliberate: the pre-lock clear runs unconditionally on every call,
including the closed-path raise, satisfying PEP 249.

This test mirrors the connection-side
``test_async_commit_rollback_messages_under_lock.py`` so the cursor
side is symmetrically covered against future drift.
"""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError


@pytest.fixture
async def cursor() -> AsyncIterator[AsyncCursor]:
    """An AsyncCursor whose parent AsyncConnection is in the
    post-``_ensure_locks`` state with a mocked underlying client."""
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
    yield AsyncCursor(conn)


async def test_execute_clears_messages_on_closed_path(cursor: AsyncCursor) -> None:
    """Even on the closed-cursor raise path, ``messages`` is cleared
    first so callers see consistent semantics regardless of which
    branch fires.

    This is the load-bearing PEP 249 §6.1.2 contract: clearing must
    happen on every entry path, including the early-raise branches —
    not only on successful completion. The pre-lock clear at the top
    of ``execute`` is what guarantees this; a future refactor that
    moved the clear into the lock-protected body would silently
    regress the closed-path branch.
    """
    cursor._closed = True
    cursor.messages.append((RuntimeError, "leftover"))
    with pytest.raises(InterfaceError):
        await cursor.execute("SELECT 1")
    assert cursor.messages == []


async def test_executemany_clears_messages_on_closed_path(
    cursor: AsyncCursor,
) -> None:
    """Same contract for ``executemany``."""
    cursor._closed = True
    cursor.messages.append((RuntimeError, "leftover"))
    with pytest.raises(InterfaceError):
        await cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert cursor.messages == []
