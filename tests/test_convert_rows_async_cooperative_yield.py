"""Pin: ``_convert_rows_async`` yields cooperatively when materialising
large result sets so the per-row work after ``await client.query_sql(...)``
does not monopolise the user's event loop.

The synchronous ``_convert_rows`` helper still backs the sync DB-API
surface (which runs on the daemon background loop — user loop
unaffected). The async cursor's ``_execute_unlocked`` now routes
through ``_convert_rows_async``, which:

1. For small result sets (below the threshold), falls through to the
   synchronous fast path — no scheduler overhead on the hot path.
2. For large result sets, yields with ``await asyncio.sleep(0)``
   every N rows so sibling coroutines on the user loop get a turn.

Under the prior unconditional list comprehension, a 100k-row fetch
burned ~50 ms+ of loop-thread CPU between the wire-layer drain (which
already yields per frame) and the cursor handing rows back to the
caller — breaking the cooperative-yield chain established by the
``_drain_continuations`` fix.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from dqlitedbapi import cursor as cursor_mod
from dqlitewire.constants import ValueType


def _make_rows(count: int) -> tuple[list[list[Any]], list[list[int]], list[int]]:
    """Build (rows, row_types, column_types) for an INTEGER-only
    fast-path payload of the given row count."""
    rows: list[list[Any]] = [[i, i * 2] for i in range(count)]
    row_types: list[list[int]] = [
        [int(ValueType.INTEGER), int(ValueType.INTEGER)] for _ in range(count)
    ]
    column_types: list[int] = [int(ValueType.INTEGER), int(ValueType.INTEGER)]
    return rows, row_types, column_types


@pytest.mark.asyncio
async def test_convert_rows_async_yields_between_batches_on_large_fetch() -> None:
    """A 50k-row materialisation must let a sibling ticker run at
    least a non-trivial number of times. Under the prior
    list-comprehension shape the sibling got zero ticks."""
    rows, row_types, column_types = _make_rows(50_000)

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
        result = await cursor_mod._convert_rows_async(rows, row_types, column_types)
        during = sibling_ran - baseline
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(result) == 50_000, "all rows must be materialised"
    assert during >= 5, (
        f"sibling ran only {during} times during a 50k-row materialise; "
        "cooperative yield missing on the async fetch path"
    )


@pytest.mark.asyncio
async def test_convert_rows_async_small_batch_uses_fast_path_no_yield() -> None:
    """Small result sets (below the threshold) must not pay any
    yield overhead — fall through to the synchronous fast path."""
    rows, row_types, column_types = _make_rows(100)

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
        result = await cursor_mod._convert_rows_async(rows, row_types, column_types)
        during = sibling_ran - baseline
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(result) == 100
    # Small fetches go through the sync fast path — at most a couple
    # of ticks from the asyncio.sleep that wraps the call.
    assert during <= 2, f"small fetch should not yield internally; sibling ran {during} times"


@pytest.mark.asyncio
async def test_convert_rows_async_empty_rows_returns_empty_list() -> None:
    """Edge case: zero rows."""
    result = await cursor_mod._convert_rows_async([], [], [int(ValueType.INTEGER)])
    assert result == []


@pytest.mark.asyncio
async def test_convert_rows_async_behaviour_matches_sync_for_converter_path() -> None:
    """ISO8601 column triggers the slow path; output must equal the
    sync helper's output cell-for-cell."""
    rows: list[list[Any]] = [[1, "2024-01-01T00:00:00", 10]]
    row_types: list[list[int]] = [
        [int(ValueType.INTEGER), int(ValueType.ISO8601), int(ValueType.INTEGER)]
    ]
    column_types: list[int] = [
        int(ValueType.INTEGER),
        int(ValueType.ISO8601),
        int(ValueType.INTEGER),
    ]

    sync_result = cursor_mod._convert_rows(rows, row_types, column_types)
    async_result = await cursor_mod._convert_rows_async(rows, row_types, column_types)
    assert async_result == sync_result
