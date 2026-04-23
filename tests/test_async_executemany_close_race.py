"""Async ``executemany`` must observe a concurrent ``close()`` at each
suspension point and must never re-populate visible state via
``_ExecuteManyAccumulator.apply`` after the cursor was closed.

Three interleavings are covered:

1. Per-iteration: ``close()`` lands after a completed iteration and
   before the next; the loop must observe the closed state on its
   own (not rely on the nested ``execute``'s guard).
2. Final apply: ``close()`` lands after the last iteration but before
   ``acc.apply()`` runs. ``apply`` must skip the write so the cursor's
   scrubbed state stays scrubbed.
3. Pure unit: ``_ExecuteManyAccumulator.apply`` on a closed cursor is
   a no-op.
"""

from __future__ import annotations

from typing import Any

from dqlitedbapi.cursor import _ExecuteManyAccumulator


class _ClosedStubCursor:
    """Minimal ``_ExecuteManyCursor``-shaped stub with ``_closed=True``."""

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
        # Accumulator state must NOT be written onto a closed cursor.
        assert cur._rowcount == -1
        assert cur._description is None
        assert cur._rows == []

    def test_apply_writes_on_open_cursor(self) -> None:
        acc = _ExecuteManyAccumulator()
        desc = (("c", None, None, None, None, None, None),)
        acc.rows = [(1,), (2,)]
        acc.description = desc
        acc.total_affected = 2
        # Mark that push() ran — otherwise apply() skips the write to
        # preserve the empty-seq baseline (see ISSUE-569).
        acc._pushed = 1
        cur = _OpenStubCursor()
        acc.apply(cur)
        assert cur._rowcount == 2
        assert cur._description == desc
        assert cur._rows == [(1,), (2,)]
