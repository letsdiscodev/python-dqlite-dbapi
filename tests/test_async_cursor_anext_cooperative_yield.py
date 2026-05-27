"""Pin: ``AsyncCursor.__anext__`` yields cooperatively between
buffered rows so ``async for row in cursor:`` over a large result
set does not monopolise the event loop.

The cursor pre-fetches the entire result set at ``execute`` time;
``fetchone`` reads from the in-memory buffer with no wire IO. The
``await self.fetchone()`` in ``__anext__`` is therefore an "await
on an already-resolved coroutine", which does NOT yield to the loop
scheduler. ``async for row in cur:`` over N rows is N tight
iterations with effectively zero yields — the canonical async
anti-pattern.

The fix adds ``await asyncio.sleep(0)`` every N rows (threshold
chosen so tight inner loops over small fetches pay zero overhead)
so siblings on the user loop get loop time even when the cursor
is being iterated row-by-row.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor


class _StubCursor:
    """Lightweight stand-in that satisfies just enough of
    ``AsyncCursor.__anext__``'s contract: ``fetchone`` returns the
    next row from a pre-built list (or ``None`` at end).

    Iterating an instance via ``__anext__.__get__(self)`` exercises
    the real cursor method against this stub — we don't need the
    rest of the AsyncCursor slot machinery.
    """

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self._row_index = 0
        # Mirror the AsyncCursor slot so __anext__'s counter
        # increment lands on a real attribute.
        self._aiter_yield_counter = 0

    async def fetchone(self) -> tuple[Any, ...] | None:
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def _check_parent_loop_only_lazy(self) -> None:
        return None


async def _iterate_stub_via_real_anext(stub: _StubCursor) -> list[tuple[Any, ...]]:
    """Drive the stub through AsyncCursor's actual ``__anext__`` and
    ``__aiter__`` methods so the test exercises the production code
    rather than a re-implementation."""
    out: list[tuple[Any, ...]] = []
    # ``__aiter__`` calls ``_check_parent_loop_only_lazy`` then returns
    # self; replicate inline.
    stub._check_parent_loop_only_lazy()
    while True:
        try:
            row = await AsyncCursor.__anext__(stub)  # type: ignore[arg-type]
        except StopAsyncIteration:
            break
        out.append(row)
    return out


@pytest.mark.asyncio
async def test_anext_yields_between_buffered_rows_on_large_iteration() -> None:
    """Iterating 10000 pre-buffered rows must let a sibling ticker
    run at least a non-trivial number of times. Under the prior
    shape the sibling got zero ticks."""
    rows: list[tuple[Any, ...]] = [(i, i * 2) for i in range(10_000)]
    stub = _StubCursor(rows)

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
        seen = await _iterate_stub_via_real_anext(stub)
        during = sibling_ran - baseline
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(seen) == 10_000, "all rows must be delivered"
    assert during >= 5, (
        f"sibling ran only {during} times during a 10k-row async iteration; "
        "__anext__ cooperative yield missing"
    )


@pytest.mark.asyncio
async def test_anext_small_iteration_does_not_pay_excess_yield_overhead() -> None:
    """Small iterations (below the threshold) should not pay any
    per-row yield overhead."""
    rows: list[tuple[Any, ...]] = [(i,) for i in range(10)]
    stub = _StubCursor(rows)

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
        await _iterate_stub_via_real_anext(stub)
        during = sibling_ran - baseline
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # A tiny iteration should not trigger the threshold-gated yield.
    assert during <= 2, (
        f"small async iteration should not yield internally; sibling ran {during} times"
    )


@pytest.mark.asyncio
async def test_anext_stop_iteration_at_end() -> None:
    """Sanity: the iteration stops when fetchone returns None."""
    stub = _StubCursor([(1,), (2,)])
    seen = await _iterate_stub_via_real_anext(stub)
    assert seen == [(1,), (2,)]
