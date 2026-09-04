"""Pin: ``AsyncCursor.fetchmany`` yields cooperatively on large ``size``
(row_factory delivery loop), gated above ``_LARGE_RESULT_ROW_THRESHOLD``.
The yield sits after ``result.append`` so a cancel restores ``_row_index``
to a clean delivered-count boundary (cancel-atomicity)."""

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
    """A 50k-row fetchmany under a row_factory must let a sibling ticker run."""
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
    """Small explicit ``size`` (below the threshold) must not pay yield overhead."""
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
    """The yielding loop delivers the same transformed rows, in order."""
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
    """A cancel on a yield leaves ``_row_index`` at a clean delivered-count
    boundary; a follow-up fetchmany continues with no row skipped or replayed."""
    n = 200_000
    cur = _prime_async_cursor([(i,) for i in range(n)])
    cur._row_factory = lambda _c, r: tuple(r)

    task = asyncio.create_task(cur.fetchmany(n))
    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    idx = cur._row_index
    # Partial progress, landed on a yield boundary.
    assert 0 < idx < n
    assert idx % _CONVERT_ROWS_YIELD_EVERY == 0

    cur._row_factory = None
    nxt = await cur.fetchmany(1)
    assert nxt == [(idx,)]


@pytest.mark.asyncio
async def test_fetchmany_row_factory_typeerror_wrapped_on_yielding_path() -> None:
    """A factory ``TypeError`` on the yielding path surfaces as ``DataError``,
    index unchanged for the failing row."""
    from dqlitedbapi.exceptions import DataError

    cur = _prime_async_cursor([(i,) for i in range(20_000)])

    def factory(_c: object, r: tuple[Any, ...]) -> tuple[Any, ...]:
        if r[0] == 9_000:  # past the 4096-row yield boundary
            raise TypeError("argument 1 must be a cursor")
        return tuple(r)

    cur._row_factory = factory

    with pytest.raises(DataError, match="row_factory call failed"):
        await cur.fetchmany(20_000)
    assert cur._row_index == 9_000
