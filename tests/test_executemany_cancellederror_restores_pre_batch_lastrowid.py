"""``executemany``'s ``except BaseException`` arm must catch cancel-class raises
(CancelledError etc.), not just Exception: narrowing it to ``except Exception``
would let CancelledError skip the reset, leaving the dirtied mid-batch state visible."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor

pytestmark = pytest.mark.asyncio


async def test_sync_executemany_cancellederror_mid_batch_restores_pre_batch_lastrowid() -> None:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = 5  # pre-batch value
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._completed_iterations = 0
    cur._connection = MagicMock()
    cur._connection._max_total_rows = None

    iteration_state = {"calls": 0}

    async def fake_execute_async(self_inner: Cursor, operation: str, params: object) -> None:
        iteration_state["calls"] += 1
        if iteration_state["calls"] >= 2:
            # Dirty the torn mid-batch state the arm must reset.
            self_inner._rowcount = 99
            self_inner._rows = [("x",)]
            self_inner._description = (("c", None, None, None, None, None, None),)
            raise asyncio.CancelledError()
        self_inner._lastrowid = 100 + iteration_state["calls"]

    with (
        patch.object(Cursor, "_execute_async", fake_execute_async),
        pytest.raises(asyncio.CancelledError),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    # Arm fires for CancelledError: torn state reset, in-batch lastrowid preserved.
    assert cur._lastrowid == 101
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None


async def test_async_executemany_cancellederror_mid_batch_restores_pre_batch_lastrowid() -> None:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = 5
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._completed_iterations = 0
    cur._executing_task = None

    conn = MagicMock()
    conn._max_total_rows = None
    finalize_lock = asyncio.Lock()
    op_lock = asyncio.Lock()
    conn._ensure_locks = MagicMock(return_value=(finalize_lock, op_lock))
    cur._connection = conn

    iteration_state = {"calls": 0}

    async def fake_execute_unlocked(
        self_inner: AsyncCursor, operation: str, params: object
    ) -> None:
        iteration_state["calls"] += 1
        if iteration_state["calls"] >= 2:
            # Dirty the torn mid-batch state the arm must reset.
            self_inner._rowcount = 99
            self_inner._rows = [("x",)]
            self_inner._description = (("c", None, None, None, None, None, None),)
            raise asyncio.CancelledError()
        self_inner._lastrowid = 100 + iteration_state["calls"]

    with (
        patch.object(AsyncCursor, "_execute_unlocked", fake_execute_unlocked),
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(asyncio.CancelledError),
    ):
        await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    assert cur._lastrowid == 101
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
