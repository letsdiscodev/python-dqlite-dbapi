"""Pin: sync ``Cursor.fetchmany``'s per-iteration ``fetchone()``
loop preserves ``_row_index`` on cancel/exception so partially-
iterated rows are not silently consumed.

Mirrors the async sibling pin
(``tests/aio/test_fetchmany_cancel_atomic.py``) — the BaseException
restore arm in sync ``Cursor.fetchmany`` was uncovered while the
async sibling had a dedicated pin. Real KI / SystemExit-mid-
fetchmany footgun if the arm regresses to ``except Exception:``.

The override raises *after* ``super().fetchone()`` advances
``_row_index``, so the snapshot/restore arm has actual work to do —
without the restore, the next ``fetchall()`` would skip exactly the
row whose retrieval was interrupted. A regression that drops the
entire try/except/restore block would leave ``_row_index`` advanced
past ``snapshot + len(result)`` and the test would fail.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor


class _CountingCursor(Cursor):
    """Test-only cursor that raises on the Nth fetchone call AFTER the
    parent has advanced ``_row_index``."""

    raise_after_advance: int = 0

    def fetchone(self) -> Any:
        row = super().fetchone()
        # Raise AFTER ``super().fetchone()`` increments ``_row_index``
        # so the production restore line has real work to do.
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
    """With raise_after_advance=4, the parent advances _row_index to
    4 and returns row (3,); the override then raises. At raise time
    _row_index==4 but only 3 rows were appended to result. Without the
    restore arm, _row_index would stay at 4 and the next fetchall
    would skip row (3,).
    """
    cur = _make_sync_counting_cursor(10, raise_after_advance=4)

    with pytest.raises(BaseException, match="simulated cancel"):  # noqa: PT011, BLE001
        cur.fetchmany(10)

    # The restore arm scrubbed _row_index back to snapshot + len(result)
    # = 0 + 3 = 3, so the row that was advanced-but-not-delivered ((3,))
    # is still pending.
    assert cur._row_index == 3

    # The interrupted row plus the remaining tail must still be
    # fetchable. If the restore arm regressed, (3,) would be silently
    # skipped.
    cur.raise_after_advance = -1  # disable raise
    rest = cur.fetchall()
    assert rest == [(3,), (4,), (5,), (6,), (7,), (8,), (9,)]
