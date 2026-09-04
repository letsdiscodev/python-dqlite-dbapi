"""Post-close property reads do not raise. ``close()`` clears ``description`` but
preserves ``lastrowid``/``rowcount``/``arraysize`` (stdlib sqlite3 parity; SQLAlchemy
reads ``lastrowid`` after close)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _populated_sync_cursor() -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = [(1,), (2,)]
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._rowcount = 2
    cur._lastrowid = 42
    cur._row_index = 0
    cur._arraysize = 5
    cur.messages = []
    conn = MagicMock()
    conn._closed = False
    conn._check_thread = lambda: None
    cur._connection = conn
    return cur


def _populated_async_cursor() -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = [(1,), (2,)]
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._rowcount = 2
    cur._lastrowid = 42
    cur._row_index = 0
    cur._arraysize = 5
    cur._executing_task = None
    cur.messages = []
    conn = MagicMock()
    conn._closed = False
    conn._check_loop_only = lambda: None
    cur._connection = conn
    return cur


def test_sync_post_close_description_scrubbed_to_none() -> None:
    cur = _populated_sync_cursor()
    cur.close()
    assert cur.description is None


def test_sync_post_close_lastrowid_preserved() -> None:
    cur = _populated_sync_cursor()
    cur.close()
    assert cur.lastrowid == 42


def test_sync_post_close_arraysize_preserved() -> None:
    """``arraysize`` is caller config, not result-set state; ``close()`` keeps it."""
    cur = _populated_sync_cursor()
    cur.close()
    assert cur.arraysize == 5


def test_sync_post_close_rowcount_preserved() -> None:
    cur = _populated_sync_cursor()
    cur.close()
    assert cur.rowcount == 2


@pytest.mark.asyncio
async def test_async_post_close_description_scrubbed_to_none() -> None:
    cur = _populated_async_cursor()
    cur.close()
    assert cur.description is None


@pytest.mark.asyncio
async def test_async_post_close_lastrowid_preserved() -> None:
    cur = _populated_async_cursor()
    cur.close()
    assert cur.lastrowid == 42


@pytest.mark.asyncio
async def test_async_post_close_arraysize_preserved() -> None:
    cur = _populated_async_cursor()
    cur.close()
    assert cur.arraysize == 5


@pytest.mark.asyncio
async def test_async_post_close_rowcount_preserved() -> None:
    cur = _populated_async_cursor()
    cur.close()
    assert cur.rowcount == 2
