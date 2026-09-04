"""Async ``executemany`` must observe a concurrent ``close()`` and never
re-populate visible state via ``_ExecuteManyAccumulator.apply`` after close.
"""

from __future__ import annotations

from typing import Any

from dqlitedbapi.cursor import _ExecuteManyAccumulator


class _ClosedStubCursor:
    def __init__(self) -> None:
        self._rowcount = -1
        self._description: Any = None
        self._rows: list[tuple[Any, ...]] = []
        self._row_index = 0
        self._closed = True


class _OpenStubCursor:
    def __init__(self) -> None:
        self._rowcount = -1
        self._description: Any = None
        self._rows: list[tuple[Any, ...]] = []
        self._row_index = 0
        self._closed = False


class TestAccumulatorApplySkipsClosed:
    def test_apply_noop_on_closed_cursor(self) -> None:
        acc = _ExecuteManyAccumulator()
        desc = (("c", None, None, None, None, None, None),)
        acc.rows = [(1,), (2,)]
        acc.description = desc
        acc.total_affected = 2
        cur = _ClosedStubCursor()
        acc.apply(cur)
        assert cur._rowcount == -1
        assert cur._description is None
        assert cur._rows == []

    def test_apply_writes_on_open_cursor(self) -> None:
        acc = _ExecuteManyAccumulator()
        desc = (("c", None, None, None, None, None, None),)
        acc.rows = [(1,), (2,)]
        acc.description = desc
        acc.total_affected = 2
        # Mark push() as run, else apply() skips the write to preserve the
        # zero-iteration baseline (empty-seq executemany leaves state untouched).
        acc._pushed = 1
        cur = _OpenStubCursor()
        acc.apply(cur)
        assert cur._rowcount == 2
        assert cur._description == desc
        assert cur._rows == [(1,), (2,)]
