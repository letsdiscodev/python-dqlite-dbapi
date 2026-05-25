"""Pin: ``executemany``'s BaseException arm aligns ``_lastrowid``
restoration with ``_completed_iterations`` — mid-batch raises
(in-batch progress > 0) PRESERVE the in-batch lastrowid so callers
using the (count, anchor) pair for idempotent compensation get a
consistent surface. Zero-progress raises (input validation, iter 0
raised before its += 1) restore the pre-batch snapshot.

Before this pin, ``_lastrowid`` was unconditionally restored to the
pre-batch snapshot — the rowid of the last successful in-batch
iteration was rolled back, but ``_completed_iterations`` was
preserved. The two observability fields disagreed: count > 0 said
"row N committed" while lastrowid pointed at the prior batch's
last row.

Both the sync ``Cursor`` and the aio ``AsyncCursor`` siblings carry
the snapshot/restore pair; both are pinned here.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor

pytestmark = pytest.mark.asyncio


def _make_sync_cursor_with_state(completed: int, lastrowid: int | None) -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = lastrowid
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._completed_iterations = completed
    cur._connection = MagicMock()
    cur._connection._max_total_rows = None
    return cur


def _make_async_cursor_with_state(completed: int, lastrowid: int | None) -> AsyncCursor:
    import asyncio

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = lastrowid
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._completed_iterations = completed
    cur._executing_task = None
    conn = MagicMock()
    conn._max_total_rows = None
    conn._ensure_locks = MagicMock(return_value=(asyncio.Lock(), asyncio.Lock()))
    cur._connection = conn
    return cur


async def test_sync_mid_batch_raise_preserves_in_batch_lastrowid() -> None:
    """Iteration 0 succeeds (writes lastrowid=42), iteration 1 raises:
    lastrowid must be preserved at 42 (matching _completed_iterations==1),
    not restored to the pre-batch snapshot of 7.
    """
    cur = _make_sync_cursor_with_state(completed=2, lastrowid=7)

    calls = {"n": 0}

    async def fake_execute_async(self_inner: Cursor, operation: str, params: object) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            self_inner._lastrowid = 42
        else:
            raise RuntimeError("simulated mid-batch failure")

    with (
        patch.object(Cursor, "_execute_async", fake_execute_async),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,)])

    assert cur._completed_iterations == 1, "in-batch counter preserved"
    assert cur._lastrowid == 42, (
        f"in-batch lastrowid must be preserved when _completed_iterations > 0; "
        f"got {cur._lastrowid!r}"
    )


async def test_sync_zero_progress_raise_restores_pre_batch_lastrowid() -> None:
    """Iteration 0 raises BEFORE writing lastrowid: _completed_iterations
    is still 0, so both lastrowid AND completed_iterations restore to
    pre-batch snapshots.
    """
    cur = _make_sync_cursor_with_state(completed=2, lastrowid=7)

    async def fake_execute_async(self_inner: Cursor, operation: str, params: object) -> None:
        raise RuntimeError("iter-0 raised")

    # The first call to _execute_async raises; counter never
    # increments → restoration path.
    with (
        patch.object(Cursor, "_execute_async", fake_execute_async),
        pytest.raises(RuntimeError, match="iter-0 raised"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,)])

    assert cur._completed_iterations == 2, "pre-batch counter restored"
    assert cur._lastrowid == 7, (
        f"pre-batch lastrowid restored on zero-progress raise; got {cur._lastrowid!r}"
    )


async def test_async_mid_batch_raise_preserves_in_batch_lastrowid() -> None:
    """Aio sibling: same shape as the sync mid-batch case above."""
    cur = _make_async_cursor_with_state(completed=2, lastrowid=7)

    calls = {"n": 0}

    async def fake_execute_unlocked(
        self_inner: AsyncCursor, operation: str, params: object
    ) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            self_inner._lastrowid = 42
        else:
            raise RuntimeError("simulated mid-batch failure")

    with (
        patch.object(AsyncCursor, "_execute_unlocked", fake_execute_unlocked),
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

    assert cur._completed_iterations == 1
    assert cur._lastrowid == 42, (
        f"aio: in-batch lastrowid must be preserved when "
        f"_completed_iterations > 0; got {cur._lastrowid!r}"
    )


async def test_async_zero_progress_raise_restores_pre_batch_lastrowid() -> None:
    """Aio sibling: iteration 0 raises before lastrowid write."""
    cur = _make_async_cursor_with_state(completed=2, lastrowid=7)

    async def fake_execute_unlocked(
        self_inner: AsyncCursor, operation: str, params: object
    ) -> None:
        raise RuntimeError("iter-0 raised")

    with (
        patch.object(AsyncCursor, "_execute_unlocked", fake_execute_unlocked),
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(RuntimeError, match="iter-0 raised"),
    ):
        await cur.executemany("INSERT INTO t VALUES (?)", [(1,)])

    assert cur._completed_iterations == 2
    assert cur._lastrowid == 7
