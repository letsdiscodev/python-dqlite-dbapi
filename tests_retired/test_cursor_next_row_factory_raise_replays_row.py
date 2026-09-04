"""``__next__`` / ``__anext__`` preserve the row index on a non-StopIteration
row_factory raise (conscious divergence from sqlite3): the factory runs before
advancing ``_row_index`` so ``fetchmany``'s snapshot/restore retry keeps the row."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _prime_sync_cursor(rows: list[tuple[Any, ...]]) -> Cursor:
    cur = Cursor.__new__(Cursor)
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
    conn._check_thread = MagicMock()
    cur._connection = conn
    return cur


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
    cur._executing_task = None
    cur._completed_iterations = 0
    cur._aiter_yield_counter = 0
    cur.messages = []
    conn = MagicMock()
    conn._check_loop_binding = MagicMock()
    cur._connection = conn
    return cur


def test_sync_next_factory_raise_does_not_advance_index() -> None:
    """``__next__`` factory raise leaves the index on the un-delivered row."""
    cur = _prime_sync_cursor([(1,), (42,), (100,)])

    def factory(_c: object, row: tuple[Any, ...]) -> tuple[Any, ...]:
        if row[0] == 42:
            raise ValueError("don't like 42")
        return row

    cur._row_factory = factory

    assert next(cur) == (1,)
    assert cur._row_index == 1

    with pytest.raises(ValueError, match="don't like 42"):
        next(cur)
    assert cur._row_index == 1, (
        "row_index must NOT advance on factory raise — fetchmany's "
        "snapshot/restore retry semantic depends on it"
    )

    # Same row retried.
    with pytest.raises(ValueError, match="don't like 42"):
        next(cur)
    assert cur._row_index == 1


async def test_async_anext_factory_raise_does_not_advance_index() -> None:
    """Async sibling of the sync pin."""
    cur = _prime_async_cursor([(1,), (42,), (100,)])

    def factory(_c: object, row: tuple[Any, ...]) -> tuple[Any, ...]:
        if row[0] == 42:
            raise ValueError("don't like 42")
        return row

    cur._row_factory = factory

    assert await anext(cur) == (1,)
    assert cur._row_index == 1

    with pytest.raises(ValueError, match="don't like 42"):
        await anext(cur)
    assert cur._row_index == 1, (
        "row_index must NOT advance on factory raise — fetchmany's "
        "snapshot/restore retry semantic depends on it"
    )

    # Same row retried.
    with pytest.raises(ValueError, match="don't like 42"):
        await anext(cur)
    assert cur._row_index == 1
