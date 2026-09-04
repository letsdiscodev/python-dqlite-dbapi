"""Pin: the async NULL-rescue type-code scan yields cooperatively on
large all-NULL columns (gated by ``_LARGE_RESULT_ROW_THRESHOLD``) so the
O(cols x rows) walk no longer blocks the loop before the first await."""

from __future__ import annotations

import asyncio
import contextlib
import time

import pytest

from dqlitedbapi.aio.cursor import _resolve_null_rescue_type_codes
from dqlitedbapi.types import UNKNOWN as _UNKNOWN_TYPE
from dqlitewire import ValueType

pytestmark = pytest.mark.asyncio


def _sync_reference(
    column_types: list[ValueType],
    row_types: list[list[ValueType]],
) -> list[int | object]:
    """Byte-identical reference: the prior inline scan shape."""
    type_codes: list[int | object] = []
    for col_idx, c in enumerate(column_types):
        if c != ValueType.NULL:
            type_codes.append(int(c))
            continue
        resolved: int | object = _UNKNOWN_TYPE
        for j in range(1, len(row_types)):
            if col_idx < len(row_types[j]):
                candidate = row_types[j][col_idx]
                if candidate != ValueType.NULL:
                    resolved = int(candidate)
                    break
        type_codes.append(resolved)
    return type_codes


async def test_typed_row0_columns_resolve_directly() -> None:
    column_types = [ValueType.INTEGER, ValueType.TEXT]
    row_types = [[ValueType.INTEGER, ValueType.TEXT]]
    result = await _resolve_null_rescue_type_codes(column_types, row_types)
    assert result == [int(ValueType.INTEGER), int(ValueType.TEXT)]
    assert result == _sync_reference(column_types, row_types)


async def test_null_first_row_rescued_from_later_row() -> None:
    column_types = [ValueType.NULL, ValueType.TEXT]
    row_types = [
        [ValueType.NULL, ValueType.TEXT],
        [ValueType.NULL, ValueType.TEXT],
        [ValueType.INTEGER, ValueType.TEXT],
    ]
    result = await _resolve_null_rescue_type_codes(column_types, row_types)
    assert result[0] == int(ValueType.INTEGER)
    assert result == _sync_reference(column_types, row_types)


async def test_all_null_column_falls_back_to_unknown() -> None:
    column_types = [ValueType.NULL, ValueType.INTEGER]
    row_types = [[ValueType.NULL, ValueType.INTEGER] for _ in range(50)]
    result = await _resolve_null_rescue_type_codes(column_types, row_types)
    assert result[0] is _UNKNOWN_TYPE
    assert result[1] == int(ValueType.INTEGER)
    assert result == _sync_reference(column_types, row_types)


async def test_ragged_rows_do_not_short_circuit() -> None:
    # Short rows must be skipped without breaking the all-row contract.
    column_types = [ValueType.NULL, ValueType.NULL]
    row_types = [
        [ValueType.NULL],  # ragged: only 1 col
        [ValueType.NULL, ValueType.NULL],
        [ValueType.NULL, ValueType.FLOAT],
    ]
    result = await _resolve_null_rescue_type_codes(column_types, row_types)
    assert result[0] is _UNKNOWN_TYPE
    assert result[1] == int(ValueType.FLOAT)
    assert result == _sync_reference(column_types, row_types)


async def test_large_all_null_column_yields_cooperatively() -> None:
    """A wide all-NULL fixture must let a sibling coroutine tick during
    the scan; pre-fix the synchronous walk blocked it end-to-end."""
    n_rows = 200_000
    column_types = [ValueType.NULL, ValueType.NULL, ValueType.NULL]
    row_types = [[ValueType.NULL, ValueType.NULL, ValueType.NULL] for _ in range(n_rows)]

    inter_tick_gaps: list[float] = []
    stop = False
    last = time.perf_counter()

    async def _ticker() -> None:
        nonlocal last
        while not stop:
            await asyncio.sleep(0)
            now = time.perf_counter()
            inter_tick_gaps.append(now - last)
            last = now

    ticker = asyncio.create_task(_ticker())
    try:
        last = time.perf_counter()
        result = await _resolve_null_rescue_type_codes(column_types, row_types)
    finally:
        stop = True
        await asyncio.sleep(0)
        ticker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ticker

    assert all(rc is _UNKNOWN_TYPE for rc in result)
    # Drop startup/shutdown samples; no remaining inter-tick gap > 200 ms.
    if len(inter_tick_gaps) > 2:
        worst = max(inter_tick_gaps[1:-1])
        assert worst < 0.200, (
            f"description NULL-rescue scan pinned the loop for "
            f"{worst * 1000:.1f} ms; the per-step yield should cap "
            f"inter-tick gaps well under 200 ms."
        )


async def test_small_result_takes_synchronous_fast_path() -> None:
    """Below ``_LARGE_RESULT_ROW_THRESHOLD`` the resolver must not await
    ``asyncio.sleep(0)`` (no scheduler overhead on small fetches)."""
    from unittest.mock import patch

    column_types = [ValueType.NULL]
    row_types = [[ValueType.NULL] for _ in range(100)]  # < threshold

    sleep_calls: list[float] = []
    real_sleep = asyncio.sleep

    async def _tracking_sleep(delay: float, *a: object, **k: object) -> None:
        sleep_calls.append(delay)
        await real_sleep(delay, *a, **k)

    with patch.object(asyncio, "sleep", _tracking_sleep):
        result = await _resolve_null_rescue_type_codes(column_types, row_types)

    assert result[0] is _UNKNOWN_TYPE
    assert sleep_calls == [], f"small result should not yield; saw sleep calls: {sleep_calls!r}"
