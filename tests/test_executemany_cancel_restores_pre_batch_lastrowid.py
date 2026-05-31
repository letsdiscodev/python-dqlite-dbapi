"""``executemany`` restores ``_lastrowid`` to the pre-batch value on the BaseException
re-raise (matching the success path, the docstring, and stdlib). ``_completed_iterations``
is the separate partial-progress anchor: restored only on a zero-progress raise, preserved
on a mid-batch raise."""

from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor

pytestmark = pytest.mark.asyncio


async def test_sync_executemany_cancel_restores_pre_batch_lastrowid() -> None:
    """Iteration 0 succeeds (writes _lastrowid), iteration 1 raises: lastrowid is
    restored to the pre-batch value while _completed_iterations stays at 1."""
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

    # Writes a new rowid on iteration 0, raises on iteration 1.
    iteration_state = {"calls": 0}

    async def fake_execute_async(self_inner: Cursor, operation: str, params: object) -> None:
        iteration_state["calls"] += 1
        if iteration_state["calls"] >= 2:
            raise RuntimeError("simulated mid-batch failure")
        self_inner._lastrowid = 100 + iteration_state["calls"]  # e.g. 101 / 102 / ...

    with (
        patch.object(Cursor, "_execute_async", fake_execute_async),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    assert cur._lastrowid == 5, (
        f"lastrowid must restore to pre-batch on a mid-batch raise; got {cur._lastrowid!r}"
    )
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    assert cur._completed_iterations == 1


async def test_async_executemany_cancel_restores_pre_batch_lastrowid() -> None:
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
    import asyncio as _asyncio

    finalize_lock = _asyncio.Lock()
    op_lock = _asyncio.Lock()
    conn._ensure_locks = MagicMock(return_value=(finalize_lock, op_lock))
    cur._connection = conn

    iteration_state = {"calls": 0}

    async def fake_execute_unlocked(
        self_inner: AsyncCursor, operation: str, params: object
    ) -> None:
        iteration_state["calls"] += 1
        if iteration_state["calls"] >= 2:
            raise RuntimeError("simulated mid-batch failure")
        self_inner._lastrowid = 100 + iteration_state["calls"]

    with (
        patch.object(AsyncCursor, "_execute_unlocked", fake_execute_unlocked),
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    assert cur._lastrowid == 5, (
        "AsyncCursor.executemany must restore lastrowid to pre-batch (5) "
        "on a mid-batch raise; got "
        f"{cur._lastrowid!r}"
    )
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    assert cur._completed_iterations == 1


async def test_sync_executemany_success_preserves_lastrowid() -> None:
    """Success path preserves the pre-batch lastrowid (stdlib parity — executemany
    does not update lastrowid), not an in-batch iteration's rowid."""
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = 5
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._completed_iterations = 0
    cur._connection = MagicMock()
    cur._connection._max_total_rows = None

    iteration_state = {"calls": 0}

    async def fake_execute_async(self_inner: Cursor, operation: str, params: object) -> None:
        iteration_state["calls"] += 1
        self_inner._lastrowid = 100 + iteration_state["calls"]

    with patch.object(Cursor, "_execute_async", fake_execute_async):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,)])

    assert cur._lastrowid == 5
    assert cur._completed_iterations == 2
