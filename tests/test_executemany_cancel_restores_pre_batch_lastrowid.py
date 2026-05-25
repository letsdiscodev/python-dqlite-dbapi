"""Pin: ``Cursor.executemany`` / ``AsyncCursor.executemany``
preserve a CONSISTENT (count, anchor) pair on the BaseException
re-raise path.

Mid-batch raises (in-batch progress > 0) PRESERVE the in-batch
``_lastrowid`` so the (``_completed_iterations``, ``_lastrowid``)
pair is internally consistent for idempotent compensation —
``_completed_iterations == 1`` says "iteration 0 succeeded" and
``_lastrowid`` is the rowid that iteration 0 wrote.

Zero in-batch progress (iteration 0 raised before ``+= 1``) restores
the pre-batch snapshots for both fields. See the success-path
companion test below for the post-batch clear-to-None semantic.
"""

from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor

pytestmark = pytest.mark.asyncio


async def test_sync_executemany_cancel_preserves_in_batch_lastrowid() -> None:
    """Drive ``Cursor._executemany_async`` directly (bypassing the
    sync wrapper) so we can observe a mid-batch raise. Iteration 0
    succeeds (writes ``_lastrowid``); iteration 1 raises. The
    in-batch lastrowid is PRESERVED so it aligns with
    ``_completed_iterations == 1``.
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
        # write for the row-returning DML path. The real code only
        # writes lastrowid after the wire call returns successfully;
        # a raise during the wire call leaves lastrowid untouched.
        iteration_state["calls"] += 1
        if iteration_state["calls"] >= 2:
            raise RuntimeError("simulated mid-batch failure")
        self_inner._lastrowid = 100 + iteration_state["calls"]  # e.g. 101 / 102 / ...

    with (
        patch.object(Cursor, "_execute_async", fake_execute_async),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    # Mid-batch raise (in-batch progress > 0): _lastrowid is preserved
    # at the in-batch value so (count=1, anchor=101) is consistent.
    assert cur._lastrowid == 101, (
        "in-batch lastrowid must be PRESERVED when _completed_iterations > 0; "
        f"got {cur._lastrowid!r}"
    )
    # PEP 249-side invariants from the same arm.
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    # ``_completed_iterations`` reflects the partial progress (1 success).
    assert cur._completed_iterations == 1


async def test_async_executemany_cancel_preserves_in_batch_lastrowid() -> None:
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
        if iteration_state["calls"] >= 2:
            raise RuntimeError("simulated mid-batch failure")
        self_inner._lastrowid = 100 + iteration_state["calls"]

    # ``_check_closed`` is called inside the loop; stub to a no-op
    # since the bare cursor has no parent connection state.
    with (
        patch.object(AsyncCursor, "_execute_unlocked", fake_execute_unlocked),
        patch.object(AsyncCursor, "_check_closed", lambda self: None),
        pytest.raises(RuntimeError, match="simulated mid-batch failure"),
    ):
        await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    # Mid-batch raise: in-batch lastrowid preserved at 101 so
    # (count=1, anchor=101) is the consistent (count, anchor) pair.
    assert cur._lastrowid == 101, (
        "AsyncCursor.executemany must PRESERVE in-batch lastrowid (101) "
        "when _completed_iterations > 0; got "
        f"{cur._lastrowid!r}"
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
