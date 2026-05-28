"""Pin: ``AsyncCursor.fetchmany`` yields cooperatively when a large
``size`` is requested, so the per-row delivery loop (which applies a
custom ``row_factory`` via ``_next_row_unlocked``) does not monopolise
the user's event loop.

``fetchmany(size)`` delivers up to ``size`` rows by calling the sync
``_next_row_unlocked`` per row, which applies ``row_factory``. The prior
loop had no ``await`` — a large explicit ``size`` (a common batch-buffer
idiom) with a row_factory pinned the user's loop for the whole batch.
Default small ``size`` is benign; the concern is an explicit large size.

The fix inserts ``await asyncio.sleep(0)`` every ``_CONVERT_ROWS_YIELD_EVERY``
DELIVERED rows, gated on ``size >= _LARGE_RESULT_ROW_THRESHOLD`` so small
fetches keep zero scheduler overhead. The yield sits AFTER
``result.append(row)`` so ``len(result)`` always equals the count of
fully-delivered rows: a cancel landing on the yield restores
``_row_index`` to ``snapshot + len(result)`` exactly — no row skipped or
replayed — preserving the existing cancel-atomicity contract.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import _CONVERT_ROWS_YIELD_EVERY


def _prime_async_cursor(rows: list[tuple[Any, ...]]) -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = rows
    cur._row_index = 0
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur.messages = []
    conn = MagicMock()
    conn._check_loop_binding = MagicMock()
    cur._connection = conn
    return cur


@pytest.mark.asyncio
async def test_fetchmany_large_size_row_factory_yields_between_batches() -> None:
    """A 50k-row fetchmany under a row_factory must let a sibling ticker
    run a non-trivial number of times. Under the prior loop the sibling
    got zero ticks."""
    cur = _prime_async_cursor([(i,) for i in range(50_000)])
    cur._row_factory = lambda _c, r: tuple(r)

    sibling_ran = 0

    async def sibling() -> None:
        nonlocal sibling_ran
        while True:
            await asyncio.sleep(0)
            sibling_ran += 1

    task = asyncio.create_task(sibling())
    try:
        await asyncio.sleep(0)
        baseline = sibling_ran
        result = await cur.fetchmany(50_000)
        during = sibling_ran - baseline
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(result) == 50_000
    assert during >= 5, (
        f"sibling ran only {during} times during a 50k-row fetchmany; "
        "cooperative yield missing on the row_factory path"
    )


@pytest.mark.asyncio
async def test_fetchmany_small_size_no_yield() -> None:
    """Small explicit ``size`` (below the threshold) must not pay any
    yield overhead."""
    cur = _prime_async_cursor([(i,) for i in range(1_000)])
    cur._row_factory = lambda _c, r: tuple(r)

    sibling_ran = 0

    async def sibling() -> None:
        nonlocal sibling_ran
        while True:
            await asyncio.sleep(0)
            sibling_ran += 1

    task = asyncio.create_task(sibling())
    try:
        await asyncio.sleep(0)
        baseline = sibling_ran
        result = await cur.fetchmany(100)
        during = sibling_ran - baseline
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(result) == 100
    assert during <= 2, f"small fetchmany should not yield internally; sibling ran {during} times"


@pytest.mark.asyncio
async def test_fetchmany_large_size_result_identity() -> None:
    """The yielding loop must deliver the same transformed rows, in the
    same order, as the prior loop."""
    cur = _prime_async_cursor([(i, i * 2) for i in range(10_000)])
    cur._description = (
        ("a", None, None, None, None, None, None),
        ("b", None, None, None, None, None, None),
    )
    cur._row_factory = lambda _c, r: {"a": r[0], "b": r[1]}

    result = await cur.fetchmany(10_000)

    assert result == [{"a": i, "b": i * 2} for i in range(10_000)]


@pytest.mark.asyncio
async def test_fetchmany_cancel_mid_batch_restores_index_exactly() -> None:
    """The load-bearing pin: a cancel landing on an inserted yield leaves
    ``_row_index`` at a clean delivered-count boundary, and a follow-up
    fetchmany continues from there with NO row skipped or replayed."""
    n = 200_000
    cur = _prime_async_cursor([(i,) for i in range(n)])
    cur._row_factory = lambda _c, r: tuple(r)

    task = asyncio.create_task(cur.fetchmany(n))
    # Let the loop cross several yield boundaries, then cancel.
    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    idx = cur._row_index
    # Partial progress, landed on a yield boundary (delivered count is a
    # multiple of the yield stride).
    assert 0 < idx < n
    assert idx % _CONVERT_ROWS_YIELD_EVERY == 0

    # Continuation is exact: the next row delivered is the one at ``idx``
    # (no replay of consumed rows, no skip past un-consumed ones).
    cur._row_factory = None
    nxt = await cur.fetchmany(1)
    assert nxt == [(idx,)]


@pytest.mark.asyncio
async def test_fetchmany_row_factory_typeerror_wrapped_on_yielding_path() -> None:
    """A factory ``TypeError`` on the large (yielding) path — past the
    first yield boundary — surfaces as ``DataError``, index unchanged for
    the failing row."""
    from dqlitedbapi.exceptions import DataError

    cur = _prime_async_cursor([(i,) for i in range(20_000)])

    def factory(_c: object, r: tuple[Any, ...]) -> tuple[Any, ...]:
        if r[0] == 9_000:  # past the 4096-row yield boundary
            raise TypeError("argument 1 must be a cursor")
        return tuple(r)

    cur._row_factory = factory

    with pytest.raises(DataError, match="row_factory call failed"):
        await cur.fetchmany(20_000)
    # The failing row never advanced the index; delivered rows did.
    assert cur._row_index == 9_000
