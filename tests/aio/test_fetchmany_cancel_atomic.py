"""Pin: ``fetchmany``'s per-iteration ``fetchone()`` loop preserves
``_row_index`` on cancel/exception so partially-iterated rows are not
silently consumed.

Without the snapshot/restore wrapper, a CancelledError raised during
``fetchmany(N)`` advances ``_row_index`` past rows that the caller
never received (the local ``result`` list is discarded on cancel).
A subsequent ``fetchall()`` then skips those rows.

The fix snapshots ``_row_index`` before the loop; on
cancel/exception, restores ``_row_index`` to ``snapshot + len(result)``
so the un-delivered rows are visible to the next fetch.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor


class _CountingCursor(AsyncCursor):
    """Test-only cursor that raises on the Nth fetchone call."""

    raise_at: int = 0

    async def fetchone(self) -> Any:
        # Use the parent class's increment semantics; raise mid-loop
        # AFTER ``_row_index`` would have advanced.
        if self._row_index + 1 == self.raise_at:
            raise BaseException("simulated cancel mid-fetchmany")
        return await super().fetchone()


def _make_counting_cursor(n: int, raise_at: int) -> _CountingCursor:
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
    # Provide a minimal _connection mock to satisfy _check_closed.
    cur._connection = MagicMock()
    cur._connection._closed = False
    return cur


@pytest.mark.asyncio
async def test_fetchmany_cancel_mid_iteration_does_not_silently_consume_rows() -> None:
    # Cursor has rows 0..9; raise on the 4th fetchone (row 3) so 3
    # rows are delivered first.
    cur = _make_counting_cursor(10, raise_at=4)

    with pytest.raises(BaseException, match="simulated cancel"):
        await cur.fetchmany(10)

    # 3 rows were delivered before the cancel.
    # _row_index must reflect "3 rows consumed", NOT advanced past the
    # failed call.
    assert cur._row_index == 3

    # The remaining rows must still be fetchable.
    cur.raise_at = 0  # disable the trap
    rest = await cur.fetchall()
    assert rest == [(3,), (4,), (5,), (6,), (7,), (8,), (9,)]
