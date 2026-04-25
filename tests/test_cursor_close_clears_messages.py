"""PEP 249 §6.1.2: Cursor.messages is cleared by every standard cursor
method "prior to executing the call". close() is a standard cursor
method; pin that it clears messages.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import connect
from dqlitedbapi.aio import AsyncConnection


def test_sync_cursor_close_clears_messages() -> None:
    conn = connect("localhost:9001")
    cur = conn.cursor()
    cur.messages.append(("sentinel", Warning("noop")))
    cur.close()
    assert list(cur.messages) == []


@pytest.mark.asyncio
async def test_async_cursor_close_clears_messages() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    cur.messages.append(("sentinel", Warning("noop")))
    await cur.close()
    assert list(cur.messages) == []


def test_sync_cursor_close_clears_messages_idempotent_call() -> None:
    """Even on a second close() (the no-op early-return path), the
    messages clear must still happen. PEP 249 wording is
    unambiguous — every method call clears, regardless of the path
    the method takes."""
    conn = connect("localhost:9001")
    cur = conn.cursor()
    cur.close()
    cur.messages.append(("late", Warning("after close")))
    cur.close()
    assert list(cur.messages) == []
