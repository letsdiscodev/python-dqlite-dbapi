"""PEP 249 §6.1.2: every standard cursor method clears ``messages``; pin close()."""

from __future__ import annotations

from dqlitedbapi import connect
from dqlitedbapi.aio import AsyncConnection


def test_sync_cursor_close_clears_messages() -> None:
    conn = connect("localhost:9001")
    cur = conn.cursor()
    cur.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    cur.close()
    assert list(cur.messages) == []


async def test_async_cursor_close_clears_messages() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    cur.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    cur.close()
    assert list(cur.messages) == []


def test_sync_cursor_close_clears_messages_idempotent_call() -> None:
    """Even a second close() (the no-op early-return path) must still clear messages."""
    conn = connect("localhost:9001")
    cur = conn.cursor()
    cur.close()
    cur.messages.append(("late", Warning("after close")))  # type: ignore[arg-type]
    cur.close()
    assert list(cur.messages) == []
