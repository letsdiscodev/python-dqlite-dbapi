"""Pin: ``_convert_params_async`` yields cooperatively when binding a
large parameter list so the per-value adaptation that runs before the
wire round-trip does not monopolise the user's event loop.

The synchronous ``_convert_params`` helper still backs the sync DB-API
surface (which runs on the daemon background loop — user loop
unaffected). The async cursor's ``_execute_unlocked`` now routes through
``_convert_params_async``, which:

1. For small / ``None`` bind lists (below the threshold), falls through
   to the synchronous helper unchanged — no scheduler overhead on the
   hot path.
2. For large bind lists, yields with ``await asyncio.sleep(0)`` every N
   values so sibling coroutines on the user loop get a turn.

A single ``execute`` may carry up to the wire ``_MAX_PARAM_COUNT``
(~32k) positional binds — the shape SQLAlchemy ``insertmanyvalues``
flattens a bulk INSERT batch into. Under the prior unconditional
``_convert_params`` loop, converting ~32k datetime / Decimal / adapter
values burned tens-to-hundreds of ms of loop-thread CPU before the
first ``await``, breaking the cooperative-yield chain the read path
already honours via ``_convert_rows_async``.

This is the bind-side mirror of
``test_convert_rows_async_cooperative_yield.py``.
"""

from __future__ import annotations

import asyncio
import datetime
from typing import Any

import pytest

from dqlitedbapi import cursor as cursor_mod
from dqlitedbapi.exceptions import DataError, ProgrammingError


async def _count_sibling_ticks_during(coro: Any) -> tuple[Any, int]:
    """Run ``coro`` while a background ticker counts how many times it
    gets scheduled, returning ``(result, tick_count)``."""
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
    """A ~50k-value bind must let a sibling ticker run a non-trivial
    number of times. Under the prior synchronous ``_convert_params``
    call the sibling got zero ticks."""
    params = list(range(50_000))
    result, during = await _count_sibling_ticks_during(cursor_mod._convert_params_async(params))
    assert result == params, "all values must be converted (ints pass through)"
    assert during >= 5, (
        f"sibling ran only {during} times during a 50k-value bind; "
        "cooperative yield missing on the async bind path"
    )


@pytest.mark.asyncio
async def test_convert_params_async_small_list_uses_fast_path_no_yield() -> None:
    """Small bind lists (below the threshold) must not pay any yield
    overhead — fall through to the synchronous fast path."""
    params = list(range(100))
    result, during = await _count_sibling_ticks_during(cursor_mod._convert_params_async(params))
    assert result == params
    assert during <= 2, f"small bind should not yield internally; sibling ran {during} times"


@pytest.mark.asyncio
async def test_convert_params_async_none_returns_none() -> None:
    """Edge case: ``None`` params (no binds) returns ``None``."""
    assert await cursor_mod._convert_params_async(None) is None


@pytest.mark.asyncio
async def test_convert_params_async_empty_returns_empty_list() -> None:
    """Edge case: empty sequence converts to an empty list."""
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
    """A large payload that crosses the yield threshold and exercises
    the per-value converter (datetime) must still convert identically
    to the sync helper, cell-for-cell."""
    params: list[Any] = [datetime.datetime(2024, 1, 1) for _ in range(5_000)]
    sync_result = cursor_mod._convert_params(params)
    async_result = await cursor_mod._convert_params_async(params)
    assert async_result == sync_result


@pytest.mark.asyncio
async def test_convert_params_async_conform_raise_propagates_unwrapped() -> None:
    """Exception parity: a user ``__conform__`` raising a non-PEP-249
    exception propagates UNWRAPPED, exactly as the sync converter does."""

    class _BadConform:
        def __conform__(self, _protocol: object) -> object:
            raise RuntimeError("boom from conform")

    with pytest.raises(RuntimeError, match="boom from conform"):
        await cursor_mod._convert_params_async([_BadConform()])


@pytest.mark.asyncio
async def test_convert_params_async_adapter_failure_wraps_as_data_error() -> None:
    """Exception parity: a documented adapter-misuse failure still wraps
    as ``DataError`` on the async path."""

    class WithFailingConform:
        def __conform__(self, _protocol: object) -> object:
            return None

    with pytest.raises(DataError, match="adapter for"):
        await cursor_mod._convert_params_async([WithFailingConform()])


@pytest.mark.asyncio
async def test_convert_params_async_rejects_non_sequence() -> None:
    """Exception parity: the async path applies the same
    ``_reject_non_sequence_params`` qmark guard as the sync helper."""
    with pytest.raises(ProgrammingError):
        await cursor_mod._convert_params_async("not a sequence")
    with pytest.raises(ProgrammingError):
        await cursor_mod._convert_params_async({"a": 1})  # type: ignore[arg-type]
