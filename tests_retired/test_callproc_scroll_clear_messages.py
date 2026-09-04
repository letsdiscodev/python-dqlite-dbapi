"""Cursor methods on the NotSupportedError path still clear
Cursor.messages (callproc per PEP 249 §6.1.1; scroll for consistency)."""

from __future__ import annotations

import contextlib

import pytest

from dqlitedbapi import Connection, NotSupportedError

_WARNING_STALE: tuple[type[Exception], Exception] = (Warning, Warning("stale"))


class _FakeMessages(list):  # type: ignore[type-arg]
    pass


@pytest.fixture
def cursor():
    conn = Connection("127.0.0.1:9001")
    cur = conn.cursor()
    conn.messages.append(_WARNING_STALE)
    cur.messages.append(_WARNING_STALE)
    try:
        yield cur, conn
    finally:
        with contextlib.suppress(Exception):
            cur.close()
        with contextlib.suppress(Exception):
            conn.close()


def test_sync_callproc_clears_messages(cursor) -> None:
    cur, conn = cursor
    with pytest.raises(NotSupportedError):
        cur.callproc("p")
    # Cursor methods clear only Cursor.messages, not Connection.messages.
    assert list(cur.messages) == []
    assert list(conn.messages) == [_WARNING_STALE]


def test_sync_scroll_clears_messages(cursor) -> None:
    cur, conn = cursor
    with pytest.raises(NotSupportedError):
        cur.scroll(1)
    # Cursor methods clear only Cursor.messages, not Connection.messages.
    assert list(cur.messages) == []
    assert list(conn.messages) == [_WARNING_STALE]


async def test_async_callproc_clears_messages() -> None:
    from dqlitedbapi.aio.connection import AsyncConnection

    # Construct directly (not via aconnect) so no live server is needed.
    conn = AsyncConnection("127.0.0.1:9001")
    cur = conn.cursor()
    conn.messages.append(_WARNING_STALE)
    cur.messages.append(_WARNING_STALE)
    with pytest.raises(NotSupportedError):
        cur.callproc("p")
    # Cursor methods clear only Cursor.messages, not Connection.messages.
    assert list(cur.messages) == []
    assert list(conn.messages) == [_WARNING_STALE]


async def test_async_scroll_clears_messages() -> None:
    from dqlitedbapi.aio.connection import AsyncConnection

    conn = AsyncConnection("127.0.0.1:9001")
    cur = conn.cursor()
    conn.messages.append(_WARNING_STALE)
    cur.messages.append(_WARNING_STALE)
    with pytest.raises(NotSupportedError):
        cur.scroll(1)
    # Cursor methods clear only Cursor.messages, not Connection.messages.
    assert list(cur.messages) == []
    assert list(conn.messages) == [_WARNING_STALE]
