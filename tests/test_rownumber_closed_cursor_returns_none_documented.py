"""``rownumber`` returns ``None`` on a closed cursor (not ``Error``), matching
``description`` / ``rowcount``; ``close()`` scrubs ``_description`` so the
property cannot distinguish closed from never-executed."""

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _bare_sync_cursor() -> Cursor:
    conn = MagicMock()
    conn._closed = False
    conn._check_thread = lambda: None
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._connection = conn
    cur._rows = []
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    return cur


def _bare_async_cursor() -> AsyncCursor:
    conn = MagicMock()
    conn._closed = False
    conn._check_loop_only = lambda: None
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._connection = conn
    cur._executing_task = None
    cur._rows = []
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    return cur


def test_sync_rownumber_returns_none_on_closed_cursor() -> None:
    cur = _bare_sync_cursor()
    cur.close()
    assert cur._closed is True
    assert cur.rownumber is None


@pytest.mark.asyncio
async def test_async_rownumber_returns_none_on_closed_cursor() -> None:
    cur = _bare_async_cursor()
    cur.close()
    assert cur._closed is True
    assert cur.rownumber is None
