"""Pin: ``AsyncCursor.fetchall`` yields cooperatively while applying a
custom ``row_factory`` to a large buffer, so the per-row transform does
not monopolise the user's event loop.

When ``row_factory`` is set, ``fetchall`` transforms every remaining
buffered row by calling the user's factory. The prior shape was a single
list comprehension with no ``await`` — for a multi-100k-row buffer that
pinned the user's loop for the whole transform (no sibling coroutine
could run, cancellation could not land). This is the same failure mode
already fixed for the materialisation pass (``_convert_rows_async``) and
the description NULL-rescue scan; ``fetchall``'s fetch-time row_factory
transform was a residual site the prior fixes did not reach.

The fix gates the yield on ``_LARGE_RESULT_ROW_THRESHOLD`` (small results
keep the straight comprehension — zero scheduler overhead) and fires
``await asyncio.sleep(0)`` every ``_CONVERT_ROWS_YIELD_EVERY`` rows. The
``row_factory is None`` path (a plain slice) is unaffected. The
load-bearing raise-safety contract — factory applied BEFORE
``_row_index`` advances, so a raise/cancel leaves the whole result
re-fetchable — is preserved.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor


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
async def test_fetchall_row_factory_large_buffer_yields_between_batches() -> None:
    """A 50k-row fetchall under a row_factory must let a sibling ticker
    run a non-trivial number of times. Under the prior single
    comprehension the sibling got zero ticks."""
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
        result = await cur.fetchall()
        during = sibling_ran - baseline
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(result) == 50_000, "all rows must be transformed and returned"
    assert cur._row_index == 50_000, "index advanced to buffer end after success"
    assert during >= 5, (
        f"sibling ran only {during} times during a 50k-row fetchall transform; "
        "cooperative yield missing on the row_factory path"
    )


@pytest.mark.asyncio
async def test_fetchall_row_factory_small_buffer_no_yield() -> None:
    """Small results (below the threshold) must not pay any yield
    overhead — keep the straight comprehension."""
    cur = _prime_async_cursor([(i,) for i in range(100)])
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
        result = await cur.fetchall()
        during = sibling_ran - baseline
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(result) == 100
    assert during <= 2, f"small fetchall should not yield internally; sibling ran {during} times"


@pytest.mark.asyncio
async def test_fetchall_row_factory_large_buffer_result_identity() -> None:
    """The yielding transform must be byte-identical to the prior
    comprehension output (same objects, same order)."""
    rows = [(i, i * 2) for i in range(10_000)]
    cur = _prime_async_cursor(list(rows))
    cur._description = (
        ("a", None, None, None, None, None, None),
        ("b", None, None, None, None, None, None),
    )
    cur._row_factory = lambda _c, r: {"a": r[0], "b": r[1]}

    result = await cur.fetchall()

    assert result == [{"a": i, "b": i * 2} for i in range(10_000)]


@pytest.mark.asyncio
async def test_fetchall_row_factory_raise_leaves_index_unchanged() -> None:
    """Raise-safety contract: a factory that raises mid-transform leaves
    ``_row_index`` unchanged so the whole result is re-fetchable. Must
    survive the yield insertion."""
    cur = _prime_async_cursor([(i,) for i in range(10_000)])

    def boom(_c: object, r: tuple[Any, ...]) -> tuple[Any, ...]:
        if r[0] == 5_000:
            raise ValueError("boom")
        return tuple(r)

    cur._row_factory = boom

    with pytest.raises(ValueError, match="boom"):
        await cur.fetchall()

    # Index NOT advanced — the whole buffer is still re-fetchable.
    assert cur._row_index == 0


@pytest.mark.asyncio
async def test_fetchall_row_factory_typeerror_wrapped_on_yielding_path() -> None:
    """A factory ``TypeError`` raised on the LARGE (yielding) path — on a
    row well past the first yield boundary — must still surface as
    ``DataError``, not leak the bare ``TypeError``."""
    from dqlitedbapi.exceptions import DataError

    cur = _prime_async_cursor([(i,) for i in range(20_000)])

    def factory(_c: object, r: tuple[Any, ...]) -> tuple[Any, ...]:
        if r[0] == 9_000:  # past the 4096-row yield boundary
            raise TypeError("argument 1 must be a cursor")
        return tuple(r)

    cur._row_factory = factory

    with pytest.raises(DataError, match="row_factory call failed"):
        await cur.fetchall()
    assert cur._row_index == 0


@pytest.mark.asyncio
async def test_fetchall_row_factory_cancel_mid_transform_leaves_index() -> None:
    """A cancel landing during a large transform leaves ``_row_index``
    unchanged so the whole result is re-fetchable. Drive it by cancelling
    the fetchall task after it has started yielding."""
    cur = _prime_async_cursor([(i,) for i in range(200_000)])
    cur._row_factory = lambda _c, r: tuple(r)

    task = asyncio.create_task(cur.fetchall())
    # Let the transform start and cross several yield boundaries.
    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Index untouched — re-fetchable in full.
    assert cur._row_index == 0
