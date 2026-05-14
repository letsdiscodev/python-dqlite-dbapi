"""Pin: ``AsyncCursor.fetchall`` happy-path for a custom row_factory.

The raise-path is covered by
``test_fetchall_row_factory_raise_replay.py`` (the
"factory raises → index unchanged" invariant). The SUCCESS path —
factory applied to every row AND ``_row_index`` advanced to the end
of the buffer AFTER the transform — had no direct pin.

Without this pin, a refactor that reverses the order (advance index
first, transform second) would silently regress: the transformed
output still matches the raw rows in shape, and only a factory raise
would surface the divergence. This file asserts both halves of the
contract directly.

See ``aio/cursor.py::AsyncCursor.fetchall`` L795-806.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from dqlitedbapi.aio.cursor import AsyncCursor


def _prime_async_cursor(rows: list[tuple[Any, ...]]) -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = rows
    cur._row_index = 0
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur.messages = []
    conn = MagicMock()
    conn._check_loop_binding = MagicMock()
    cur._connection = conn
    return cur


async def test_async_fetchall_applies_row_factory_and_advances_index() -> None:
    """fetchall under a row_factory returns the transformed rows AND
    advances ``_row_index`` to ``len(self._rows)`` AFTER the transform
    completes — the load-bearing "advance only on success" invariant."""
    cur = _prime_async_cursor([(1, "a"), (2, "b")])
    cur._description = (
        ("id", None, None, None, None, None, None),
        ("name", None, None, None, None, None, None),
    )

    def factory(_c: object, r: tuple[Any, ...]) -> dict[str, Any]:
        return {"id": r[0], "name": r[1]}

    cur._row_factory = factory

    result = await cur.fetchall()

    # Transform applied to every row.
    assert result == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    # Index advanced AFTER the transform — to the full buffer length.
    assert cur._row_index == 2
    # A second fetchall after success returns empty (no rows left).
    assert await cur.fetchall() == []
