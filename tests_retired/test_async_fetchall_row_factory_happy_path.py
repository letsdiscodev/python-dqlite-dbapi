"""Pin: ``AsyncCursor.fetchall`` applies row_factory to every row, then
advances ``_row_index`` to the buffer end (advance-only-on-success)."""

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
    """Returns transformed rows, then advances ``_row_index`` to the buffer end."""
    cur = _prime_async_cursor([(1, "a"), (2, "b")])
    cur._description = (
        ("id", None, None, None, None, None, None),
        ("name", None, None, None, None, None, None),
    )

    def factory(_c: object, r: tuple[Any, ...]) -> dict[str, Any]:
        return {"id": r[0], "name": r[1]}

    cur._row_factory = factory

    result = await cur.fetchall()

    assert result == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert cur._row_index == 2
    assert await cur.fetchall() == []
