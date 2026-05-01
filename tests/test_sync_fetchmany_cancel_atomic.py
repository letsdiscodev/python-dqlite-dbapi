"""Pin: sync ``Cursor.fetchmany``'s per-iteration ``fetchone()``
loop preserves ``_row_index`` on cancel/exception so partially-
iterated rows are not silently consumed.

Mirrors the async sibling pin
(``tests/aio/test_fetchmany_cancel_atomic.py``) — the BaseException
restore arm in sync ``Cursor.fetchmany`` was uncovered while the
async sibling had a dedicated pin. Real KI / SystemExit-mid-
fetchmany footgun if the arm regresses to ``except Exception:``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor


class _CountingCursor(Cursor):
    """Test-only cursor that raises on the Nth fetchone call."""

    raise_at: int = 0

    def fetchone(self) -> Any:
        if self._row_index + 1 == self.raise_at:
            raise BaseException("simulated cancel mid-fetchmany")
        return super().fetchone()


def _make_sync_counting_cursor(n: int, raise_at: int) -> _CountingCursor:
    cur = _CountingCursor.__new__(_CountingCursor)
    cur._closed = False
    cur._description = (("col", 4, None, None, None, None, None),)
    cur._rowcount = n
    cur._lastrowid = None
    cur._rows = [(i,) for i in range(n)]
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur.raise_at = raise_at
    cur._connection = MagicMock()
    cur._connection._closed = False
    cur._connection._check_thread = lambda: None
    return cur


def test_sync_fetchmany_cancel_mid_iteration_does_not_silently_consume_rows() -> None:
    cur = _make_sync_counting_cursor(10, raise_at=4)

    with pytest.raises(BaseException, match="simulated cancel"):  # noqa: PT011, BLE001
        cur.fetchmany(10)

    # 3 rows were delivered before the cancel; _row_index must
    # reflect "3 rows consumed", NOT advanced past the failed
    # call into the row that raised.
    assert cur._row_index == 3

    # Remaining rows must still be fetchable.
    cur.raise_at = 0
    rest = cur.fetchall()
    assert rest == [(3,), (4,), (5,), (6,), (7,), (8,), (9,)]
