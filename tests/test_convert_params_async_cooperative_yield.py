"""Pin: ``_convert_params_async`` yields cooperatively on large bind lists.

Bind-side mirror of ``test_convert_rows_async_cooperative_yield.py``.
"""

from __future__ import annotations

import asyncio
import datetime
from typing import Any

import pytest

from dqlitedbapi import cursor as cursor_mod
from dqlitedbapi.exceptions import DataError, ProgrammingError


async def _count_sibling_ticks_during(coro: Any) -> tuple[Any, int]:
    """Run ``coro`` while a ticker counts schedulings; return (result, tick_count)."""
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
        result = await coro
        during = sibling_ran - baseline
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    return result, during


@pytest.mark.asyncio
async def test_convert_params_async_yields_between_batches_on_large_list() -> None:
    """A ~50k-value bind must let a sibling ticker run several times."""
    params = list(range(50_000))
    result, during = await _count_sibling_ticks_during(cursor_mod._convert_params_async(params))
    assert result == params, "all values must be converted (ints pass through)"
    assert during >= 5, (
        f"sibling ran only {during} times during a 50k-value bind; "
        "cooperative yield missing on the async bind path"
    )


@pytest.mark.asyncio
async def test_convert_params_async_small_list_uses_fast_path_no_yield() -> None:
    """Small bind lists fall through to the sync fast path without yielding."""
    params = list(range(100))
    result, during = await _count_sibling_ticks_during(cursor_mod._convert_params_async(params))
    assert result == params
    assert during <= 2, f"small bind should not yield internally; sibling ran {during} times"


@pytest.mark.asyncio
async def test_convert_params_async_none_returns_none() -> None:
    assert await cursor_mod._convert_params_async(None) is None


@pytest.mark.asyncio
async def test_convert_params_async_empty_returns_empty_list() -> None:
    assert await cursor_mod._convert_params_async([]) == []


@pytest.mark.asyncio
async def test_convert_params_async_matches_sync_small() -> None:
    """A mixed small payload must convert identically to the sync helper."""
    params = [1, "text", None, datetime.datetime(2024, 1, 1, 12, 30, 0), 3.5]
    sync_result = cursor_mod._convert_params(params)
    async_result = await cursor_mod._convert_params_async(params)
    assert async_result == sync_result


@pytest.mark.asyncio
async def test_convert_params_async_matches_sync_large_converter_path() -> None:
    """A large datetime payload (crossing the yield threshold) must match the sync helper."""
    params: list[Any] = [datetime.datetime(2024, 1, 1) for _ in range(5_000)]
    sync_result = cursor_mod._convert_params(params)
    async_result = await cursor_mod._convert_params_async(params)
    assert async_result == sync_result


@pytest.mark.asyncio
async def test_convert_params_async_conform_raise_propagates_unwrapped() -> None:
    """A user ``__conform__`` raising a non-PEP-249 exception propagates unwrapped."""

    class _BadConform:
        def __conform__(self, _protocol: object) -> object:
            raise RuntimeError("boom from conform")

    with pytest.raises(RuntimeError, match="boom from conform"):
        await cursor_mod._convert_params_async([_BadConform()])


@pytest.mark.asyncio
async def test_convert_params_async_unsupported_type_wraps_as_data_error() -> None:
    """A None-returning ``__conform__`` (declines adaptation) wraps as DataError."""

    class WithFailingConform:
        def __conform__(self, _protocol: object) -> object:
            return None

    with pytest.raises(DataError, match="is not supported"):
        await cursor_mod._convert_params_async([WithFailingConform()])


@pytest.mark.asyncio
async def test_convert_params_async_rejects_non_sequence() -> None:
    """The async path applies the same non-sequence qmark guard as the sync helper."""
    with pytest.raises(ProgrammingError):
        await cursor_mod._convert_params_async("not a sequence")
    with pytest.raises(ProgrammingError):
        await cursor_mod._convert_params_async({"a": 1})  # type: ignore[arg-type]
