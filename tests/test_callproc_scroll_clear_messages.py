"""Cursor methods on the NotSupportedError path must still clear
``Connection.messages`` / ``Cursor.messages`` per PEP 249 §6.1.1
(``callproc`` is in the explicit list; ``scroll`` is not but we
clear for sibling consistency).
"""

from __future__ import annotations

import pytest

from dqlitedbapi import Connection, NotSupportedError


class _FakeMessages(list):  # type: ignore[type-arg]
    pass


@pytest.fixture
def cursor():
    conn = Connection("127.0.0.1:9001")
    cur = conn.cursor()
    # Seed messages so we can observe the clear.
    conn.messages.append((Warning, "stale"))
    cur.messages.append((Warning, "stale"))
    yield cur, conn


def test_sync_callproc_clears_messages(cursor) -> None:
    cur, conn = cursor
    with pytest.raises(NotSupportedError):
        cur.callproc("p")
    # PEP 249 §6.1.1 / §6.1.2 — Connection.messages and Cursor.messages
    # are independent surfaces. Cursor methods clear only Cursor.messages.
    assert list(cur.messages) == []
    assert list(conn.messages) == [(Warning, "stale")]


def test_sync_scroll_clears_messages(cursor) -> None:
    cur, conn = cursor
    with pytest.raises(NotSupportedError):
        cur.scroll(1)
    # PEP 249 §6.1.1 / §6.1.2 — Connection.messages and Cursor.messages
    # are independent surfaces. Cursor methods clear only Cursor.messages.
    assert list(cur.messages) == []
    assert list(conn.messages) == [(Warning, "stale")]


@pytest.mark.asyncio
async def test_async_callproc_clears_messages() -> None:
    from dqlitedbapi.aio.connection import AsyncConnection

    # Construct directly (not via eager ``aconnect``) so the test does
    # not require a live server for a path that only exercises local
    # cursor state.
    conn = AsyncConnection("127.0.0.1:9001")
    cur = conn.cursor()
    conn.messages.append((Warning, "stale"))
    cur.messages.append((Warning, "stale"))
    with pytest.raises(NotSupportedError):
        cur.callproc("p")
    # PEP 249 §6.1.1 / §6.1.2 — Connection.messages and Cursor.messages
    # are independent surfaces. Cursor methods clear only Cursor.messages.
    assert list(cur.messages) == []
    assert list(conn.messages) == [(Warning, "stale")]


@pytest.mark.asyncio
async def test_async_scroll_clears_messages() -> None:
    from dqlitedbapi.aio.connection import AsyncConnection

    conn = AsyncConnection("127.0.0.1:9001")
    cur = conn.cursor()
    conn.messages.append((Warning, "stale"))
    cur.messages.append((Warning, "stale"))
    with pytest.raises(NotSupportedError):
        cur.scroll(1)
    # PEP 249 §6.1.1 / §6.1.2 — Connection.messages and Cursor.messages
    # are independent surfaces. Cursor methods clear only Cursor.messages.
    assert list(cur.messages) == []
    assert list(conn.messages) == [(Warning, "stale")]
