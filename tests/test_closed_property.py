"""Pin the ``closed`` property exposed on Connection / Cursor /
AsyncConnection / AsyncCursor.

PEP 249 does not require this property and stdlib ``sqlite3``
does not expose it; psycopg / asyncpg do, and callers porting
from those drivers expect ``if not conn.closed: conn.close()``
to work without ``AttributeError``. The property is read-only
and surfaces the already-maintained internal ``_closed`` flag.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


class TestSyncClosedProperty:
    def test_connection_closed_property_starts_false(self) -> None:
        conn = Connection.__new__(Connection)
        conn._closed = False
        assert conn.closed is False

    def test_connection_closed_property_reflects_close_state(self) -> None:
        conn = Connection.__new__(Connection)
        conn._closed = False
        assert conn.closed is False
        conn._closed = True
        assert conn.closed is True

    def test_cursor_closed_property_starts_false(self) -> None:
        cur = Cursor.__new__(Cursor)
        cur._closed = False
        assert cur.closed is False

    def test_cursor_closed_property_reflects_close_state(self) -> None:
        cur = Cursor.__new__(Cursor)
        cur._closed = False
        assert cur.closed is False
        cur._closed = True
        assert cur.closed is True

    def test_connection_closed_is_read_only(self) -> None:
        conn = Connection.__new__(Connection)
        conn._closed = False
        with pytest.raises(AttributeError):
            conn.closed = True  # type: ignore[misc]

    def test_cursor_closed_is_read_only(self) -> None:
        cur = Cursor.__new__(Cursor)
        cur._closed = False
        with pytest.raises(AttributeError):
            cur.closed = True  # type: ignore[misc]


class TestAsyncClosedProperty:
    def test_async_connection_closed_property_starts_false(self) -> None:
        conn = AsyncConnection.__new__(AsyncConnection)
        conn._closed = False
        assert conn.closed is False

    def test_async_connection_closed_property_reflects_close_state(self) -> None:
        conn = AsyncConnection.__new__(AsyncConnection)
        conn._closed = False
        assert conn.closed is False
        conn._closed = True
        assert conn.closed is True

    def test_async_cursor_closed_property_starts_false(self) -> None:
        cur = AsyncCursor.__new__(AsyncCursor)
        cur._closed = False
        assert cur.closed is False

    def test_async_cursor_closed_property_reflects_close_state(self) -> None:
        cur = AsyncCursor.__new__(AsyncCursor)
        cur._closed = False
        assert cur.closed is False
        cur._closed = True
        assert cur.closed is True

    def test_async_connection_closed_is_read_only(self) -> None:
        conn = AsyncConnection.__new__(AsyncConnection)
        conn._closed = False
        with pytest.raises(AttributeError):
            conn.closed = True  # type: ignore[misc]

    def test_async_cursor_closed_is_read_only(self) -> None:
        cur = AsyncCursor.__new__(AsyncCursor)
        cur._closed = False
        with pytest.raises(AttributeError):
            cur.closed = True  # type: ignore[misc]
