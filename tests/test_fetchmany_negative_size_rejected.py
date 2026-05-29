"""``Cursor.fetchmany(-1)`` raises ``ProgrammingError`` (current stdlib raises on negative)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


def _sync_cursor_with_rows(rows: list[tuple[int, ...]]) -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = rows
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._rowcount = len(rows)
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    conn = MagicMock()
    conn._closed = False
    conn._check_thread = lambda: None
    cur._connection = conn
    return cur


def _async_cursor_with_rows(rows: list[tuple[int, ...]]) -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = rows
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._rowcount = len(rows)
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur._executing_task = None
    cur.messages = []
    conn = MagicMock()
    conn._closed = False
    conn._check_loop_only = lambda: None
    cur._connection = conn
    return cur


def test_sync_fetchmany_negative_one_raises_programmingerror() -> None:
    cur = _sync_cursor_with_rows([(1,), (2,), (3,)])
    with pytest.raises(ProgrammingError, match=r"fetchmany size must be non-negative"):
        cur.fetchmany(-1)


def test_sync_fetchmany_large_negative_raises_programmingerror() -> None:
    cur = _sync_cursor_with_rows([(1,), (2,), (3,)])
    with pytest.raises(ProgrammingError, match=r"fetchmany size must be non-negative"):
        cur.fetchmany(-99)


def test_sync_fetchmany_zero_still_returns_empty_list() -> None:
    """The negative-reject must not affect the zero-size path, which returns ``[]``."""
    cur = _sync_cursor_with_rows([(1,), (2,), (3,)])
    assert cur.fetchmany(0) == []


@pytest.mark.asyncio
async def test_async_fetchmany_negative_one_raises_programmingerror() -> None:
    cur = _async_cursor_with_rows([(1,), (2,), (3,)])
    with pytest.raises(ProgrammingError, match=r"fetchmany size must be non-negative"):
        await cur.fetchmany(-1)


@pytest.mark.asyncio
async def test_async_fetchmany_large_negative_raises_programmingerror() -> None:
    cur = _async_cursor_with_rows([(1,), (2,), (3,)])
    with pytest.raises(ProgrammingError, match=r"fetchmany size must be non-negative"):
        await cur.fetchmany(-99)
