"""Pin: ``_convert_rows_async`` folds the converter probe into the
per-chunk loop instead of an O(rows × cols) up-front walk, so the
cooperative ``await asyncio.sleep(0)`` fires regardless of fetch width.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

import pytest

from dqlitedbapi import cursor as cursor_mod


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "n_rows, n_cols",
    [
        (10, 1),  # tiny — short-circuits to sync helper
        (4096, 4),  # at threshold
        (8192, 16),  # large
    ],
)
async def test_convert_rows_async_returns_tuples_for_converter_free_fetch(
    n_rows: int, n_cols: int
) -> None:
    """A converter-free workload yields tuples that round-trip the input rows."""
    from dqlitewire.constants import ValueType

    rows = [tuple(range(n_cols)) for _ in range(n_rows)]
    column_types = [int(ValueType.INTEGER)] * n_cols
    row_types = [column_types[:] for _ in range(n_rows)]

    result = await cursor_mod._convert_rows_async(rows, row_types, column_types)
    assert len(result) == n_rows
    assert result[0] == tuple(range(n_cols))
    assert result[-1] == tuple(range(n_cols))
    assert all(isinstance(r, tuple) for r in result)


@pytest.mark.asyncio
async def test_convert_rows_async_inter_yield_gap_bounded_for_large_converter_free_fetch() -> None:
    """A 100k-row × 32-col converter-free fetch must not pin the loop between yields."""
    from dqlitewire.constants import ValueType

    n_rows = 100_000
    n_cols = 32
    rows = [tuple(range(n_cols)) for _ in range(n_rows)]
    column_types = [int(ValueType.INTEGER)] * n_cols
    row_types = [column_types[:] for _ in range(n_rows)]

    inter_yield_gaps: list[float] = []
    stop = False
    last_tick = time.perf_counter()

    async def _tick_recorder() -> None:
        nonlocal last_tick
        while not stop:
            await asyncio.sleep(0)
            now = time.perf_counter()
            inter_yield_gaps.append(now - last_tick)
            last_tick = now

    tick_task = asyncio.create_task(_tick_recorder())
    try:
        last_tick = time.perf_counter()
        await cursor_mod._convert_rows_async(rows, row_types, column_types)
    finally:
        stop = True
        await asyncio.sleep(0)
        tick_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await tick_task

    # Drop first/last samples (startup/shutdown noise). The 200 ms ceiling
    # is generous for Pi-class runners but still catches the pre-fix stall.
    if len(inter_yield_gaps) > 2:
        worst = max(inter_yield_gaps[1:-1])
        assert worst < 0.200, (
            f"_convert_rows_async pinned the loop for {worst * 1000:.1f} ms "
            f"between yields on a 100k×32 converter-free fetch — the per-chunk "
            f"yield should cap inter-yield gaps well under 200 ms."
        )
