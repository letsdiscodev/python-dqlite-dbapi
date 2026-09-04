"""``_ExecuteManyAccumulator`` enforces ``max_total_rows`` cumulatively.

The per-iteration wire governor bounds each round-trip but not the accumulator's
running total, so a huge ``INSERT ... RETURNING`` could balloon memory without this cap.
"""

from __future__ import annotations

from typing import Any

import pytest

from dqlitedbapi.cursor import _ExecuteManyAccumulator
from dqlitedbapi.exceptions import DataError


class _FakeCursor:
    """Stub matching the ``_ExecuteManyCursor`` protocol."""

    def __init__(self, rows: list[tuple[Any, ...]], description: Any) -> None:
        self._rowcount = len(rows)
        self._description = description
        self._rows = rows
        self._row_index = 0


def _description(n: int = 1) -> tuple[tuple[str, None, None, None, None, None, None], ...]:
    return tuple(("c", None, None, None, None, None, None) for _ in range(n))


class TestAccumulatorCap:
    def test_cap_raises_dataerror_on_breach(self) -> None:
        acc = _ExecuteManyAccumulator(max_rows=5)
        desc = _description()
        acc.push(_FakeCursor([(i,) for i in range(3)], desc))  # type: ignore[arg-type]
        # Cumulative 6 trips the cap of 5.
        with pytest.raises(DataError, match="max_total_rows"):
            acc.push(_FakeCursor([(i,) for i in range(3)], desc))  # type: ignore[arg-type]

    def test_cap_exact_does_not_raise(self) -> None:
        acc = _ExecuteManyAccumulator(max_rows=5)
        desc = _description()
        acc.push(_FakeCursor([(i,) for i in range(3)], desc))  # type: ignore[arg-type]
        acc.push(_FakeCursor([(i,) for i in range(2)], desc))  # type: ignore[arg-type]
        assert len(acc.rows) == 5

    def test_none_cap_disables_check(self) -> None:
        acc = _ExecuteManyAccumulator(max_rows=None)
        desc = _description()
        for _ in range(100):
            acc.push(_FakeCursor([(i,) for i in range(100)], desc))  # type: ignore[arg-type]
        assert len(acc.rows) == 10_000

    def test_cap_ignores_pure_dml_rows(self) -> None:
        # Plain DML (``_description is None``) has no rows, so the cap never trips
        # on ``total_affected`` alone.
        acc = _ExecuteManyAccumulator(max_rows=5)
        for _ in range(100):
            acc.push(_FakeCursor([], None))  # type: ignore[arg-type]
        assert acc.total_affected == 0
        assert len(acc.rows) == 0
