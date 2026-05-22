"""Pin: ``Cursor.executemany`` / ``AsyncCursor.executemany``
``except BaseException`` arms reset every per-result-set
field on mid-batch failure and re-raise.

PEP 249 §6.1.5 says ``rowcount=-1`` means undetermined.
The reset block uses that signal so callers
cannot mistake the LAST iteration's rowcount for the
cumulative count of successfully-applied iterations.
A regression that drops the bare ``raise`` would
silently turn ``executemany`` failures into "succeeded
with rowcount=-1"; a regression that drops one of the
field assignments leaves stale state observable.

The end-to-end behaviour is covered by integration tests
(``test_executemany_failure_resets_rowcount``,
``test_executemany_cancel_mid_batch``); these unit pins
exercise the same reset block without a live cluster
so PR-time CI catches the regression.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _seed_post_iteration_state(cur: Cursor | AsyncCursor) -> None:
    """Mimic state that a successfully-applied iteration would
    have left behind — so the reset's effect is observable.

    ``_lastrowid`` is deliberately *not* mutated here: the per-iteration
    rowid write happens via the snapshot/restore arm inside
    ``executemany``, and the BaseException arm restores
    ``_lastrowid`` to its pre-batch value. Mutating ``_lastrowid`` in
    this helper would mask the restore.
    """
    cur._rowcount = 42
    cur._rows = [(1,), (2,)]
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._row_index = 1


async def test_sync_executemany_basecaught_resets_all_fields_and_reraises() -> None:
    conn = MagicMock()
    raised = RuntimeError("simulated mid-batch failure")

    async def _execute_then_fail(*args: object, **kwargs: object) -> None:
        raise raised

    cur = Cursor(conn)
    # Pre-batch lastrowid — the value the caller observed from a prior
    # single-row INSERT. The BaseException arm restores this snapshot
    # so callers see the rowid they had before executemany.
    cur._lastrowid = 99

    async def _seeded_execute(*args: object, **kwargs: object) -> None:
        _seed_post_iteration_state(cur)
        # Intra-batch rowid write — what _execute_async does on the
        # row-returning DML path. Without the snapshot/restore arm,
        # this value would leak past the BaseException re-raise.
        cur._lastrowid = 101
        await _execute_then_fail()

    with (
        patch.object(Cursor, "_execute_async", _seeded_execute),
        pytest.raises(RuntimeError, match="simulated mid-batch"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,)])

    # Every field is reset to the "no operation performed" surface;
    # rowcount=-1 (PEP 249 "undetermined"). _lastrowid is restored to
    # the pre-batch snapshot — stdlib sqlite3.Cursor.lastrowid is
    # documented as not being cleared by failed/cancelled operations,
    # and the cursor's docstring at module top pins close() as the
    # single lifecycle event that scrubs it. The intra-batch write
    # (101) is overwritten by the restore so cross-driver code reading
    # ``cur.lastrowid`` after the failure sees the pre-batch value.
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    assert cur._lastrowid == 99  # restored to pre-batch snapshot
    assert cur._row_index == 0


async def test_sync_executemany_basecaught_preserves_completed_iterations() -> None:
    """Pin the preserve-direction of the BaseException arm: iteration 0
    succeeds (incrementing ``_completed_iterations`` to 1), iteration 1
    raises a BaseException-subclass. After the re-raise, the reset
    fields scrub to baseline AND ``_completed_iterations`` survives at
    its mid-batch value — the documented observability signal callers
    rely on for idempotent compensation after a cancel."""
    conn = MagicMock()

    class _Sentinel(BaseException):
        """Synthetic BaseException subclass to drive the arm without
        triggering pytest's BaseException-bypassing behaviour."""

    cur = Cursor(conn)
    cur._lastrowid = 99  # seed lastrowid so we can also assert it survives

    # The loop body calls ``_execute_async`` then ``acc.push(self)`` then
    # ``self._completed_iterations += 1``. Drive iteration 0 to
    # success (push will read seeded state) and iteration 1 to raise.
    call_count = {"n": 0}

    async def _maybe_fail(*args: object, **kwargs: object) -> None:
        # Seed the per-iteration state so ``acc.push`` is happy.
        cur._rowcount = 1
        cur._rows = []
        cur._description = None
        cur._row_index = 0
        if call_count["n"] == 1:
            raise _Sentinel("simulated mid-batch failure on iter 1")
        call_count["n"] += 1

    with (
        patch.object(Cursor, "_execute_async", _maybe_fail),
        pytest.raises(_Sentinel, match="iter 1"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,)])

    # Reset fields scrub to baseline (already pinned by the sibling
    # test above — re-pin together for locality):
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    assert cur._row_index == 0
    # ``_lastrowid`` preservation (mirrored from the sibling test):
    assert cur._lastrowid == 99
    # ``_completed_iterations`` preservation (the new pin):
    # iteration 0 succeeded (incremented to 1), iteration 1 raised
    # BEFORE the increment ran. The arm must NOT zero this — it is
    # the observability signal for "how many iterations committed
    # before the failure".
    assert cur._completed_iterations == 1, (
        "_completed_iterations must survive the BaseException re-raise; "
        f"got {cur._completed_iterations}"
    )


async def test_async_executemany_basecaught_resets_all_fields_and_reraises() -> None:
    conn = MagicMock()
    raised = RuntimeError("simulated mid-batch failure")

    async def _execute_then_fail(*args: object, **kwargs: object) -> None:
        raise raised

    aconn_cursor = AsyncCursor(conn)
    # Pre-batch lastrowid — the value the caller observed from a prior
    # single-row INSERT. The BaseException arm restores this snapshot.
    aconn_cursor._lastrowid = 99

    async def _seeded_execute(*args: object, **kwargs: object) -> None:
        _seed_post_iteration_state(aconn_cursor)
        # Intra-batch rowid write — what _execute_unlocked does on the
        # row-returning DML path. The snapshot/restore arm overwrites
        # this on the BaseException re-raise.
        aconn_cursor._lastrowid = 101
        await _execute_then_fail()

    import asyncio

    op_lock = asyncio.Lock()
    aconn_cursor._connection._ensure_locks = MagicMock(return_value=(MagicMock(), op_lock))
    aconn_cursor._connection._ensure_connection = MagicMock(return_value=MagicMock())

    with (
        patch.object(AsyncCursor, "_execute_unlocked", _seeded_execute),
        pytest.raises(RuntimeError, match="simulated mid-batch"),
    ):
        await aconn_cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

    assert aconn_cursor._rowcount == -1
    assert list(aconn_cursor._rows) == []
    assert aconn_cursor._description is None
    assert aconn_cursor._lastrowid == 99  # restored to pre-batch snapshot
    assert aconn_cursor._row_index == 0


async def test_async_executemany_basecaught_preserves_completed_iterations() -> None:
    """Async sibling of the sync preserve-direction pin: iteration 0
    succeeds, iteration 1 raises a BaseException-subclass, the re-raise
    leaves ``_completed_iterations`` at its mid-batch value."""
    import asyncio

    conn = MagicMock()

    class _Sentinel(BaseException):
        pass

    aconn_cursor = AsyncCursor(conn)
    aconn_cursor._lastrowid = 99

    call_count = {"n": 0}

    async def _maybe_fail(*args: object, **kwargs: object) -> None:
        aconn_cursor._rowcount = 1
        aconn_cursor._rows = []
        aconn_cursor._description = None
        aconn_cursor._row_index = 0
        if call_count["n"] == 1:
            raise _Sentinel("simulated mid-batch failure on iter 1")
        call_count["n"] += 1

    op_lock = asyncio.Lock()
    aconn_cursor._connection._ensure_locks = MagicMock(return_value=(MagicMock(), op_lock))
    aconn_cursor._connection._ensure_connection = MagicMock(return_value=MagicMock())

    with (
        patch.object(AsyncCursor, "_execute_unlocked", _maybe_fail),
        pytest.raises(_Sentinel, match="iter 1"),
    ):
        await aconn_cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

    # Reset fields scrub to baseline:
    assert aconn_cursor._rowcount == -1
    assert list(aconn_cursor._rows) == []
    assert aconn_cursor._description is None
    assert aconn_cursor._row_index == 0
    # ``_lastrowid`` preservation:
    assert aconn_cursor._lastrowid == 99
    # ``_completed_iterations`` preservation (the new pin):
    assert aconn_cursor._completed_iterations == 1, (
        "_completed_iterations must survive the BaseException re-raise; "
        f"got {aconn_cursor._completed_iterations}"
    )
