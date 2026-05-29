"""Pin: ``executemany``'s BaseException arm catches cancel-class
raises (``CancelledError`` / ``KeyboardInterrupt`` / ``SystemExit``),
not just `Exception`-class mid-batch failures, and preserves the
in-batch ``_lastrowid`` consistently with ``_completed_iterations``.

The existing pin `test_executemany_cancel_restores_pre_batch_lastrowid.py`
drives the BaseException arm with a `RuntimeError`. A regression that
narrowed the `except BaseException:` arm to `except Exception:` would
silently lose the cancel-class handling because `CancelledError`
inherits from `BaseException` (not `Exception`). To make that narrowing
observable, the mock dirties the torn mid-batch state (`_rowcount`,
`_rows`, `_description`) before raising `CancelledError`; the arm resets
that state, so under the regression the un-reset values would survive
the propagation and the asserts fail. The in-batch `_lastrowid` is
preserved (count=1 + anchor=101) so the (count, anchor) pair stays
consistent.
"""

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
            # Dirty the torn mid-batch state the BaseException arm is
            # responsible for resetting. If the arm were narrowed to
            # ``except Exception`` it would NOT catch the BaseException-
            # class CancelledError, so these dirty values would survive
            # the propagation and the asserts below would fail.
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

    # BaseException arm fires for CancelledError: the torn rowcount /
    # rows / description are reset, while the in-batch lastrowid is
    # preserved (count=1 + anchor=101) because a mid-batch failure keeps
    # the (count, anchor) pair consistent.
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
            # Dirty the torn mid-batch state the BaseException arm must
            # reset; a narrowing to ``except Exception`` would let the
            # CancelledError skip the arm and leave these visible.
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
