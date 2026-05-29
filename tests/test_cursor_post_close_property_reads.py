"""Pin: ``Cursor.lastrowid`` / ``Cursor.description`` / ``Cursor.rowcount``
/ ``Cursor.arraysize`` post-close reads behave as documented
(bypass-permitted contract).

Closed-state reads do NOT raise (bypass-permitted). ``close()`` clears
``description`` (a closed cursor cannot serve a result set) but
PRESERVES ``lastrowid`` and ``rowcount``, matching stdlib
``sqlite3.Cursor`` — both stay readable after the cursor is closed, so a
consumer that reads ``cursor.lastrowid`` after closing the cursor (as
SQLAlchemy does at result-access time) still gets the real rowid.
Without a regression guard, a future tightening to raise
``InterfaceError("cursor is closed")`` — or a re-introduction of the
scrub — would land silently.

``arraysize`` is also preserved by ``close()`` (caller-set
configuration, not result-set state).

``connection`` post-close behaviour is its own contract (the
weakref.proxy swap + ReferenceError → InterfaceError envelope) and
is pinned by existing tests on the proxy-resolve probe; the property
read itself returns the proxy.
"""

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
    # Bypass-permitted contract: post-close read does NOT raise.
    assert cur.description is None


def test_sync_post_close_lastrowid_preserved() -> None:
    cur = _populated_sync_cursor()
    cur.close()
    # Preserved across close, matching stdlib sqlite3.Cursor.lastrowid.
    assert cur.lastrowid == 42


def test_sync_post_close_arraysize_preserved() -> None:
    """``arraysize`` is caller-set configuration, NOT result-set
    state. ``close()`` deliberately does NOT scrub it (sibling
    discipline with stdlib ``sqlite3.Cursor``).
    """
    cur = _populated_sync_cursor()
    cur.close()
    assert cur.arraysize == 5


def test_sync_post_close_rowcount_preserved() -> None:
    cur = _populated_sync_cursor()
    cur.close()
    # Preserved across close, matching stdlib sqlite3.Cursor.rowcount.
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
