"""``__anext__`` yields cooperatively (``asyncio.sleep(0)`` every N rows) so iterating a large
buffered result set does not monopolise the loop. Rows are pre-fetched, so awaiting the buffered
``fetchone`` would otherwise never yield to the scheduler."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor


class _StubCursor:
    """Stand-in whose ``fetchone`` returns the next row from a pre-built list (``None`` at end)."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self._row_index = 0
        # Mirror the AsyncCursor slot so __anext__'s counter increment lands on a real attribute.
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
    """Drive the stub through AsyncCursor's real ``__anext__`` (production code, not a re-impl)."""
    out: list[tuple[Any, ...]] = []
    # Replicate __aiter__: check loop binding, then return self.
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
    """Iterating 10000 buffered rows must let a sibling ticker run a non-trivial count."""
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
    """Small iterations (below the threshold) should not pay any per-row yield overhead."""
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

    assert during <= 2, (
        f"small async iteration should not yield internally; sibling ran {during} times"
    )


@pytest.mark.asyncio
async def test_anext_stop_iteration_at_end() -> None:
    """Iteration stops when fetchone returns None."""
    stub = _StubCursor([(1,), (2,)])
    seen = await _iterate_stub_via_real_anext(stub)
    assert seen == [(1,), (2,)]
