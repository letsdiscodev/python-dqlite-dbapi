"""Pin: the async cursor's NULL-rescue type-code resolution for
``cursor.description`` yields cooperatively on large result sets
instead of walking every row of an all-NULL column synchronously
on the user loop.

PEP 249 §6.1.2 requires a real Type Object per column. When a
column's row-0 tag is ``ValueType.NULL`` the resolver scans
subsequent rows for the first non-NULL tag, falling back to the
``UNKNOWN`` sentinel only when EVERY row at that column is NULL.
A column that is NULL across the whole page (a LEFT JOIN
right-side with no match, an always-NULL projection) is scanned
end-to-end — O(n_null_cols × n_rows) of pure-Python iteration
that ran BEFORE the first ``await`` in the result path, outside
the cooperative-yield chain (wire read / drain / convert-rows).

The fix extracts the scan into an ``async def`` helper gated by
``_LARGE_RESULT_ROW_THRESHOLD`` (small fetches keep the
synchronous path, zero scheduler overhead) that yields
``await asyncio.sleep(0)`` every ``_CONVERT_ROWS_YIELD_EVERY``
scanned inner-row steps. The resolved type-code list is
byte-identical to the prior inline logic; only the all-NULL
"every row is NULL" fallback walks the full count, and only that
path benefits from the yields. The sync cursor surface is
unchanged.
"""

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
    # Column 0 is NULL in row 0 but INTEGER in row 2.
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
    # Column 0 is NULL in EVERY row → UNKNOWN sentinel.
    column_types = [ValueType.NULL, ValueType.INTEGER]
    row_types = [[ValueType.NULL, ValueType.INTEGER] for _ in range(50)]
    result = await _resolve_null_rescue_type_codes(column_types, row_types)
    assert result[0] is _UNKNOWN_TYPE
    assert result[1] == int(ValueType.INTEGER)
    assert result == _sync_reference(column_types, row_types)


async def test_ragged_rows_do_not_short_circuit() -> None:
    # Some rows are shorter than the column count; the scan must
    # skip them without breaking the all-row contract.
    column_types = [ValueType.NULL, ValueType.NULL]
    row_types = [
        [ValueType.NULL],  # ragged: only 1 col
        [ValueType.NULL, ValueType.NULL],
        [ValueType.NULL, ValueType.FLOAT],
    ]
    result = await _resolve_null_rescue_type_codes(column_types, row_types)
    assert result[0] is _UNKNOWN_TYPE  # col 0 NULL everywhere
    assert result[1] == int(ValueType.FLOAT)  # rescued from row 2
    assert result == _sync_reference(column_types, row_types)


async def test_large_all_null_column_yields_cooperatively() -> None:
    """A wide all-NULL fixture (200k rows, several all-NULL cols)
    must let a sibling coroutine make progress during the scan —
    the max inter-tick gap stays small. Pre-fix the scan ran
    synchronously with no yield, so the sibling never ticked
    until the entire O(cols × rows) walk finished.
    """
    n_rows = 200_000
    # 3 columns, all NULL in row 0 AND every subsequent row → each
    # triggers a full-row scan.
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
    # Drop first/last samples (startup/shutdown); assert no gap
    # exceeds 200 ms. Pre-fix the single synchronous scan of
    # 600k cells blocked the ticker for the whole walk.
    if len(inter_tick_gaps) > 2:
        worst = max(inter_tick_gaps[1:-1])
        assert worst < 0.200, (
            f"description NULL-rescue scan pinned the loop for "
            f"{worst * 1000:.1f} ms; the per-step yield should cap "
            f"inter-tick gaps well under 200 ms."
        )


async def test_small_result_takes_synchronous_fast_path() -> None:
    """Below ``_LARGE_RESULT_ROW_THRESHOLD`` the resolver must NOT
    await ``asyncio.sleep(0)`` — small fetches pay no scheduler
    overhead. Instrument ``asyncio.sleep`` to confirm zero calls.
    """
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
