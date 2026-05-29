"""commit/rollback bound their op_lock acquire by self._timeout and remap
TimeoutError to OperationalError, so a sibling parked on a slow read cannot hang
the call past the budget (e.g. under engine.dispose() / SIGTERM)."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import OperationalError


def _prime_alive_inner() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn._timeout = 0.05
    conn.messages = []
    inner = MagicMock()
    inner._protocol = object()
    inner.in_transaction = True  # route into the COMMIT path
    conn._async_conn = inner
    return conn


async def test_commit_op_lock_acquire_bounded_by_timeout() -> None:
    conn = _prime_alive_inner()
    held_lock = asyncio.Lock()
    await held_lock.acquire()  # held by us; commit() must time out

    with (
        patch.object(conn, "_ensure_locks", return_value=(None, held_lock)),
        pytest.raises(OperationalError, match="commit op_lock"),
    ):
        await conn.commit()

    held_lock.release()


async def test_rollback_op_lock_acquire_bounded_by_timeout() -> None:
    conn = _prime_alive_inner()
    held_lock = asyncio.Lock()
    await held_lock.acquire()

    with (
        patch.object(conn, "_ensure_locks", return_value=(None, held_lock)),
        pytest.raises(OperationalError, match="rollback op_lock"),
    ):
        await conn.rollback()

    held_lock.release()
