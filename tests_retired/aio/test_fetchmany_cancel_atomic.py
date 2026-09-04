"""Pin: ``fetchmany`` restores ``_row_index`` to ``snapshot + len(result)``
on cancel/exception so partially-iterated-but-undelivered rows are not
silently consumed by the next fetch."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor


class _CountingCursor(AsyncCursor):
    """Raises on the Nth row-advance AFTER the parent advanced ``_row_index``."""

    raise_after_advance: int = 0

    def _next_row_unlocked(self) -> Any:
        row = super()._next_row_unlocked()
        # Raise after the parent increments _row_index so the restore has work.
        if self._row_index == self.raise_after_advance:
            raise BaseException("simulated cancel post-advance")
        return row


def _make_counting_cursor(n: int, raise_after_advance: int) -> _CountingCursor:
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
    cur._connection = MagicMock()  # satisfy _check_closed
    cur._connection._closed = False
    return cur


async def test_fetchmany_cancel_mid_iteration_does_not_silently_consume_rows() -> None:
    """raise_after_advance=4: at raise time _row_index==4 but only 3 rows
    were delivered; without the restore the next fetchall skips row (3,)."""
    cur = _make_counting_cursor(10, raise_after_advance=4)

    with pytest.raises(BaseException, match="simulated cancel"):  # noqa: PT011, BLE001
        await cur.fetchmany(10)

    assert cur._row_index == 3

    cur.raise_after_advance = -1  # disable raise
    rest = await cur.fetchall()
    assert rest == [(3,), (4,), (5,), (6,), (7,), (8,), (9,)]
