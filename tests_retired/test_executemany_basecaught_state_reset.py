"""``executemany``'s ``except BaseException`` arm resets per-result-set state and re-raises."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _seed_post_iteration_state(cur: Cursor | AsyncCursor) -> None:
    """Mimic post-iteration state so the reset's effect is observable.

    Deliberately leaves ``_lastrowid`` alone: mutating it here would mask the
    BaseException arm's restore-to-pre-batch.
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
    cur._lastrowid = 99  # pre-batch value the BaseException arm must restore

    async def _seeded_execute(*args: object, **kwargs: object) -> None:
        _seed_post_iteration_state(cur)
        # Intra-batch rowid write that must not leak past the re-raise.
        cur._lastrowid = 101
        await _execute_then_fail()

    with (
        patch.object(Cursor, "_execute_async", _seeded_execute),
        pytest.raises(RuntimeError, match="simulated mid-batch"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,)])

    # Fields reset to baseline (rowcount=-1); _lastrowid restored to the
    # pre-batch snapshot, overwriting the intra-batch write (101).
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    assert cur._lastrowid == 99  # restored to pre-batch snapshot
    assert cur._row_index == 0


async def test_sync_executemany_basecaught_preserves_completed_iterations() -> None:
    """BaseException arm scrubs reset fields but preserves the mid-batch
    ``_completed_iterations`` (the observability signal for idempotent compensation)."""
    conn = MagicMock()

    class _Sentinel(BaseException):
        """BaseException subclass that pytest won't bypass."""

    cur = Cursor(conn)
    cur._lastrowid = 99

    call_count = {"n": 0}

    async def _maybe_fail(*args: object, **kwargs: object) -> None:
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

    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    assert cur._row_index == 0
    assert cur._lastrowid == 99
    # iteration 0 succeeded (count->1); iteration 1 raised before the increment.
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
    aconn_cursor._lastrowid = 99  # pre-batch value the BaseException arm restores

    async def _seeded_execute(*args: object, **kwargs: object) -> None:
        _seed_post_iteration_state(aconn_cursor)
        # Intra-batch rowid write overwritten on the re-raise.
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
    """Async sibling: re-raise leaves ``_completed_iterations`` at its mid-batch value."""
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

    assert aconn_cursor._rowcount == -1
    assert list(aconn_cursor._rows) == []
    assert aconn_cursor._description is None
    assert aconn_cursor._row_index == 0
    assert aconn_cursor._lastrowid == 99
    assert aconn_cursor._completed_iterations == 1, (
        "_completed_iterations must survive the BaseException re-raise; "
        f"got {aconn_cursor._completed_iterations}"
    )
