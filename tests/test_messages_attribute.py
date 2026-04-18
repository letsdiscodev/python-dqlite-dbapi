"""PEP 249 optional extension: Connection.messages / Cursor.messages."""

from dqlitedbapi import Connection
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def test_connection_messages_attribute() -> None:
    conn = Connection("localhost:9001")
    assert isinstance(conn.messages, list)
    assert conn.messages == []
    # Must be mutable.
    conn.messages.append((RuntimeError, "x"))


def test_cursor_messages_attribute() -> None:
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    assert isinstance(cursor.messages, list)
    assert cursor.messages == []


def test_async_connection_messages_attribute() -> None:
    conn = AsyncConnection("localhost:9001")
    assert isinstance(conn.messages, list)
    assert conn.messages == []


def test_async_cursor_messages_attribute() -> None:
    conn = AsyncConnection("localhost:9001")
    cursor = AsyncCursor(conn)
    assert isinstance(cursor.messages, list)
    assert cursor.messages == []
