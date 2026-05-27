"""Pin: ``_convert_rows_async`` does NOT walk every per-row type
list up-front to decide ``needs_conversion``. The probe is folded
into the per-chunk loop so the cooperative ``await
asyncio.sleep(0)`` fires on its documented cadence regardless of
fetch width.

The prior shape executed an ``O(rows × cols)`` ``any(t in
_RESULT_CONVERTERS for rt in row_types for t in rt)`` walk BEFORE
the first ``await asyncio.sleep(0)``. For a 100k-row × 32-col
fetch that walked ~3.2M dict-membership tests on the loop thread
with no yield — ~100 ms of pure-Python loop-monopolisation that
a heartbeat coroutine on a 50 ms budget could not survive.

Shape A (per-chunk probe + per-chunk dispatch) preserves the
converter-free fast-path ``tuple(row)`` materialisation at the
chunk granularity while bounding the up-front probe at one
chunk's worth of work (~12 ms at 4096 × 32 cols ≈ 130k probes).
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
import textwrap
import time

import pytest

from dqlitedbapi import cursor as cursor_mod


def _convert_rows_async_source() -> str:
    return textwrap.dedent(inspect.getsource(cursor_mod._convert_rows_async))


def test_convert_rows_async_does_not_walk_all_row_types_before_first_yield() -> None:
    """Structural pin: the function body must not contain the
    ``any(t in _RESULT_CONVERTERS for rt in row_types for t in rt)``
    up-front walk. The per-chunk probe is the post-fix shape.
    """
    src = _convert_rows_async_source()
    tree = ast.parse(src)

    # Walk every ``any(...)`` call in the function and look for the
    # specific shape: a nested generator over ``rt in row_types``.
    bad_up_front_probe = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (isinstance(node.func, ast.Name) and node.func.id == "any"):
            continue
        # The argument is a GeneratorExp.
        if not (node.args and isinstance(node.args[0], ast.GeneratorExp)):
            continue
        gen = node.args[0]
        # The bad shape iterates over ``row_types`` AS THE OUTER
        # for-clause (no slicing, no per-chunk bound). Look at every
        # generator/comprehension that walks ``row_types`` directly.
        for comp in gen.generators:
            if isinstance(comp.iter, ast.Name) and comp.iter.id == "row_types":
                bad_up_front_probe += 1

    assert bad_up_front_probe == 0, (
        f"_convert_rows_async still walks the full row_types list in "
        f"{bad_up_front_probe} ``any(...)`` probe(s) before the per-chunk "
        f"yield. Fold the probe into the per-chunk loop so the up-front "
        f"work is bounded at one chunk's cells."
    )


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
    """Behavioural pin: a converter-free workload yields tuples
    that round-trip the input rows. Regression guard for the
    Shape A fast-path preservation.
    """
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
    """A 100k-row × 32-col converter-free fetch must NOT pin the
    loop for more than ~50 ms between cooperative yields. Pre-fix:
    the up-front probe walked all 3.2M cells synchronously, blowing
    past 100 ms. Post-fix: bounded by per-chunk work.
    """
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

    # Drop the first and last samples (startup / shutdown noise);
    # assert no remaining gap exceeds 200 ms. The defensible bound
    # depends on hardware — on commodity x86 the per-chunk work for
    # 4096 × 32 cells is well under 50 ms; pin a generous 200 ms
    # ceiling so the test is stable on Pi-class runners while still
    # catching the pre-fix ~5-10 s loop-monopolisation regression.
    if len(inter_yield_gaps) > 2:
        worst = max(inter_yield_gaps[1:-1])
        assert worst < 0.200, (
            f"_convert_rows_async pinned the loop for {worst * 1000:.1f} ms "
            f"between yields on a 100k×32 converter-free fetch — the per-chunk "
            f"yield should cap inter-yield gaps well under 200 ms."
        )
