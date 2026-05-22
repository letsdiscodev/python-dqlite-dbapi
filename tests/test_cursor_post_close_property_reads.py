"""Pin: ``Cursor.lastrowid`` / ``Cursor.description`` / ``Cursor.arraysize``
post-close reads behave as documented (bypass-permitted contract).

The property docstrings document closed-state reads as
bypass-permitted with the rationale that ``close()`` scrubs each
slot to its no-result-set sentinel. Without a regression guard, a
future tightening to raise ``InterfaceError("cursor is closed")``
would land silently.

``arraysize`` is deliberately NOT scrubbed by ``close()`` (it's
caller-set configuration, not result-set state) — pin that
divergence too.

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


def test_sync_post_close_lastrowid_scrubbed_to_none() -> None:
    cur = _populated_sync_cursor()
    cur.close()
    assert cur.lastrowid is None


def test_sync_post_close_arraysize_preserved() -> None:
    """``arraysize`` is caller-set configuration, NOT result-set
    state. ``close()`` deliberately does NOT scrub it (sibling
    discipline with stdlib ``sqlite3.Cursor``).
    """
    cur = _populated_sync_cursor()
    cur.close()
    assert cur.arraysize == 5


def test_sync_post_close_rowcount_scrubbed() -> None:
    cur = _populated_sync_cursor()
    cur.close()
    # Per the existing closed-cursor contract, rowcount reads through
    # the bypass-permitted accessor and returns the scrubbed value.
    assert cur.rowcount == -1


@pytest.mark.asyncio
async def test_async_post_close_description_scrubbed_to_none() -> None:
    cur = _populated_async_cursor()
    cur.close()
    assert cur.description is None


@pytest.mark.asyncio
async def test_async_post_close_lastrowid_scrubbed_to_none() -> None:
    cur = _populated_async_cursor()
    cur.close()
    assert cur.lastrowid is None


@pytest.mark.asyncio
async def test_async_post_close_arraysize_preserved() -> None:
    cur = _populated_async_cursor()
    cur.close()
    assert cur.arraysize == 5


@pytest.mark.asyncio
async def test_async_post_close_rowcount_scrubbed() -> None:
    cur = _populated_async_cursor()
    cur.close()
    assert cur.rowcount == -1
