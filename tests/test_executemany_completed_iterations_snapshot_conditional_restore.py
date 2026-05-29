"""``executemany`` conditionally restores ``_completed_iterations`` on the BaseException
arm: input-validation raises restore the pre-batch count, mid-batch raises preserve
the in-batch partial-progress count, success leaves it at N."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor

pytestmark = pytest.mark.asyncio


def _make_sync_cursor_with_completed(n: int) -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._completed_iterations = n
    cur._connection = MagicMock()
    cur._connection._max_total_rows = None
    return cur


def _make_async_cursor_with_completed(n: int) -> AsyncCursor:
    import asyncio

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._completed_iterations = n
    cur._executing_task = None
    conn = MagicMock()
    conn._max_total_rows = None
    conn._ensure_locks = MagicMock(return_value=(asyncio.Lock(), asyncio.Lock()))
    cur._connection = conn
    return cur


async def test_sync_input_validation_raise_restores_pre_batch_completed_iterations() -> None:
    """Case (a): empty-SQL validation raise restores the counter to pre-batch."""
    from dqlitedbapi.exceptions import ProgrammingError

    cur = _make_sync_cursor_with_completed(2)
    with pytest.raises(ProgrammingError):
        await cur._executemany_async("", [(1,)])
    assert cur._completed_iterations == 2


async def test_sync_mid_batch_raise_preserves_in_batch_completed_iterations() -> None:
    """Case (b): iter 0 succeeds, iter 1 raises — counter preserves in-batch 1, not 2."""
    cur = _make_sync_cursor_with_completed(2)

    calls = {"n": 0}

    async def fake_execute_async(self_inner: Cursor, operation: str, params: object) -> None:
        calls["n"] += 1
        if calls["n"] >= 2:
            raise RuntimeError("simulated mid-batch failure")

    with (
        patch.object(Cursor, "_execute_async", fake_execute_async),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,)])

    assert cur._completed_iterations == 1


async def test_sync_success_path_counter_equals_iterations() -> None:
    """Case (c): success with N iterations leaves the counter at N."""
    cur = _make_sync_cursor_with_completed(2)

    async def fake_execute_async(self_inner: Cursor, operation: str, params: object) -> None:
        pass

    with patch.object(Cursor, "_execute_async", fake_execute_async):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    # Loop incremented from the reset baseline of 0 to 3.
    assert cur._completed_iterations == 3


async def test_async_input_validation_raise_before_reset_preserves_pre_batch() -> None:
    """Aio case (a): validation raises before _reset_execute_state runs, so the
    counter is preserved naturally without the snapshot/restore arm."""
    from dqlitedbapi.exceptions import ProgrammingError

    cur = _make_async_cursor_with_completed(2)
    with pytest.raises(ProgrammingError):
        await cur.executemany("INSERT INTO t VALUES (?)", None)  # type: ignore[arg-type]
    assert cur._completed_iterations == 2


async def test_async_mid_batch_raise_preserves_in_batch_completed_iterations() -> None:
    """Aio sibling for case (b)."""
    cur = _make_async_cursor_with_completed(2)
    calls = {"n": 0}

    async def fake_execute_unlocked(
        self_inner: AsyncCursor, operation: str, params: object
    ) -> None:
        calls["n"] += 1
        if calls["n"] >= 2:
            raise RuntimeError("simulated mid-batch failure")

    with (
        patch.object(AsyncCursor, "_execute_unlocked", fake_execute_unlocked),
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

    assert cur._completed_iterations == 1
