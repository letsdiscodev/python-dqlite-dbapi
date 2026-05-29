"""Sync ``Cursor.fetchmany`` restores ``_row_index`` on a BaseException mid-loop so the
interrupted row is not silently consumed (regressing to ``except Exception`` reopens a
KI/SystemExit footgun)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor


class _CountingCursor(Cursor):
    """Cursor that raises on the Nth row-advance after the parent advanced ``_row_index``."""

    raise_after_advance: int = 0

    def _next_row_unlocked(self) -> Any:
        row = super()._next_row_unlocked()
        # Raise after the parent increments _row_index so the restore line has work.
        if self._row_index == self.raise_after_advance:
            raise BaseException("simulated cancel post-advance")
        return row


def _make_sync_counting_cursor(n: int, raise_after_advance: int) -> _CountingCursor:
    cur = _CountingCursor.__new__(_CountingCursor)
    cur._closed = False
    cur._description = (("col", 4, None, None, None, None, None),)
    cur._rowcount = n
    cur._lastrowid = None
    cur._row_factory = None
    cur._rows = [(i,) for i in range(n)]
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur.raise_after_advance = raise_after_advance
    cur._connection = MagicMock()
    cur._connection._closed = False
    cur._connection._check_thread = lambda: None
    return cur


def test_sync_fetchmany_cancel_mid_iteration_does_not_silently_consume_rows() -> None:
    """raise_after_advance=4: parent advances _row_index to 4 and returns (3,), then the
    override raises with only 3 rows delivered. Without the restore arm the next fetchall
    would skip (3,)."""
    cur = _make_sync_counting_cursor(10, raise_after_advance=4)

    with pytest.raises(BaseException, match="simulated cancel"):  # noqa: PT011, BLE001
        cur.fetchmany(10)

    # Restored to snapshot + len(result) = 0 + 3 = 3, so (3,) is still pending.
    assert cur._row_index == 3

    cur.raise_after_advance = -1  # disable raise
    rest = cur.fetchall()
    assert rest == [(3,), (4,), (5,), (6,), (7,), (8,), (9,)]
