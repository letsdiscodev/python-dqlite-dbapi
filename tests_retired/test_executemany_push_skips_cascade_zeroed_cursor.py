"""``_ExecuteManyAccumulator.push`` early-returns when the cursor was cascade-zeroed.

Guards against a concurrent cascade landing between an iteration's wire-return and
``acc.push(self)``, which would otherwise capture stale-zero state.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.cursor import _ExecuteManyAccumulator


def test_push_skips_when_cursor_closed_under_snapshot() -> None:
    """A cursor whose ``_closed`` flipped True after the snapshot is NOT pushed."""
    acc = _ExecuteManyAccumulator(max_rows=None)

    cursor = MagicMock()
    cursor._description = (("col", 1, None, None, None, None, None),)
    cursor._rows = [("row1",)]
    cursor._rowcount = 1
    cursor._lastrowid = 5
    cursor._closed = True

    acc.push(cursor)

    assert acc._pushed == 0
    assert acc.rows == []
    assert acc.description is None
    assert acc.total_affected == 0


def test_push_records_when_cursor_open() -> None:
    acc = _ExecuteManyAccumulator(max_rows=None)

    cursor = MagicMock()
    cursor._description = (("col", 1, None, None, None, None, None),)
    cursor._rows = [("row1",), ("row2",)]
    cursor._rowcount = 2
    cursor._lastrowid = 5
    cursor._closed = False

    acc.push(cursor)

    assert acc._pushed == 1
    assert acc.rows == [("row1",), ("row2",)]
    assert acc.description == (("col", 1, None, None, None, None, None),)
    assert acc.total_affected == 2


def test_push_plain_dml_records_rowcount() -> None:
    acc = _ExecuteManyAccumulator(max_rows=None)

    cursor = MagicMock()
    cursor._description = None
    cursor._rows = []
    cursor._rowcount = 3
    cursor._lastrowid = None
    cursor._closed = False

    acc.push(cursor)

    assert acc._pushed == 1
    assert acc.total_affected == 3
