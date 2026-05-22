"""Pin: ``Cursor.executemany`` / ``AsyncCursor.executemany``
restore ``_lastrowid`` to the pre-batch snapshot on the BaseException
re-raise path.

The docstring contract is "preserve the prior INSERT's rowid across
a failed/cancelled batch." Without the snapshot, the per-iteration
write of ``self._lastrowid`` inside ``_execute_async`` /
``_execute_unlocked`` leaks the intra-batch rowid: a caller who
observed ``cur.lastrowid == 5`` from a prior single-row INSERT, then
cancelled an ``executemany`` mid-batch, would see whichever rowid
the last in-batch iteration wrote (e.g. 6, 7) rather than 5.

The fix snapshots ``self._lastrowid`` at loop entry and restores it
in the BaseException arm.
"""

from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor

pytestmark = pytest.mark.asyncio


async def test_sync_executemany_cancel_restores_pre_batch_lastrowid() -> None:
    """Drive ``Cursor._executemany_async`` directly (bypassing the
    sync wrapper) so we can observe a mid-batch raise. The pre-batch
    ``_lastrowid`` must be restored on the BaseException re-raise
    path; the test fails before the snapshot/restore fix lands.
    """
    # Bare cursor — avoid the connection / loop machinery.
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._rowcount = -1
    cur._lastrowid = 5  # the pre-batch value the caller observed
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._completed_iterations = 0
    cur._connection = MagicMock()
    cur._connection._max_total_rows = None

    # Stub the per-iteration awaitable so it writes a new rowid on
    # iteration 0 (simulating a successful INSERT/REPLACE that
    # ``_execute_async`` would record), then raises on iteration 1.
    iteration_state = {"calls": 0}

    async def fake_execute_async(self_inner: Cursor, operation: str, params: object) -> None:
        # Mimic ``_execute_async``'s per-iteration ``_lastrowid``
        # write for the row-returning DML path.
        iteration_state["calls"] += 1
        self_inner._lastrowid = 100 + iteration_state["calls"]  # e.g. 101 / 102 / ...
        if iteration_state["calls"] >= 2:
            raise RuntimeError("simulated mid-batch failure")

    with (
        patch.object(Cursor, "_execute_async", fake_execute_async),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    assert cur._lastrowid == 5, (
        "executemany's BaseException arm must restore the pre-batch "
        "lastrowid (5), not the intra-batch row (101/102); got "
        f"{cur._lastrowid!r}"
    )
    # PEP 249-side invariants from the same arm.
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    # ``_completed_iterations`` reflects the partial progress (1 success).
    assert cur._completed_iterations == 1


async def test_async_executemany_cancel_restores_pre_batch_lastrowid() -> None:
    """Async sibling: same contract, same fix shape."""
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
    # Provide the locks _ensure_locks returns; the test does not
    # contend so a default async lock pair is fine.
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
        self_inner._lastrowid = 100 + iteration_state["calls"]
        if iteration_state["calls"] >= 2:
            raise RuntimeError("simulated mid-batch failure")

    # ``_check_closed`` is called inside the loop; stub to a no-op
    # since the bare cursor has no parent connection state.
    with (
        patch.object(AsyncCursor, "_execute_unlocked", fake_execute_unlocked),
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    assert cur._lastrowid == 5, (
        "AsyncCursor.executemany's BaseException arm must restore "
        "the pre-batch lastrowid (5), not the intra-batch row "
        f"(101/102); got {cur._lastrowid!r}"
    )
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    assert cur._completed_iterations == 1


async def test_sync_executemany_success_clears_lastrowid() -> None:
    """The success-path semantic (clear to None after the batch) is
    untouched by the fix: the snapshot is consulted only on the
    BaseException arm.
    """
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

    assert cur._lastrowid is None
    assert cur._completed_iterations == 2
