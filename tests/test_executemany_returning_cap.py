"""``_ExecuteManyAccumulator`` enforces ``max_total_rows`` cumulatively.

Each ``executemany`` iteration is a distinct round-trip, so the wire-
layer ``max_total_rows`` governor caps each iteration independently —
the accumulator's running total is not otherwise bounded. A 10M-
parameter ``INSERT ... RETURNING id`` would accumulate ~560 MB of
Python tuples without the cap.

Thread the connection's ``_max_total_rows`` into the accumulator and
raise ``DataError`` when the cumulative total exceeds it.
"""

from __future__ import annotations

from typing import Any

import pytest

from dqlitedbapi.cursor import _ExecuteManyAccumulator
from dqlitedbapi.exceptions import DataError


class _FakeCursor:
    """Shape-compatible stub matching ``_ExecuteManyCursor`` protocol."""

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
        # First push: 3 rows, still under cap.
        acc.push(_FakeCursor([(i,) for i in range(3)], desc))
        # Second push: 3 more rows → cumulative 6, trips cap.
        with pytest.raises(DataError, match="max_total_rows"):
            acc.push(_FakeCursor([(i,) for i in range(3)], desc))

    def test_cap_exact_does_not_raise(self) -> None:
        acc = _ExecuteManyAccumulator(max_rows=5)
        desc = _description()
        acc.push(_FakeCursor([(i,) for i in range(3)], desc))
        acc.push(_FakeCursor([(i,) for i in range(2)], desc))
        assert len(acc.rows) == 5

    def test_none_cap_disables_check(self) -> None:
        acc = _ExecuteManyAccumulator(max_rows=None)
        desc = _description()
        for _ in range(100):
            acc.push(_FakeCursor([(i,) for i in range(100)], desc))
        assert len(acc.rows) == 10_000

    def test_cap_ignores_pure_dml_rows(self) -> None:
        # A DML push with ``_description is None`` (plain INSERT/UPDATE
        # without RETURNING) has no rows — the accumulator should not
        # trip the cap on ``total_affected`` alone.
        acc = _ExecuteManyAccumulator(max_rows=5)
        for _ in range(100):
            acc.push(_FakeCursor([], None))
        assert acc.total_affected == 0  # rowcount is 0 on empty push
        assert len(acc.rows) == 0
