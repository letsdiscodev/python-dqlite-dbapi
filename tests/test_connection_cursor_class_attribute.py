"""``Connection.Cursor`` / ``AsyncConnection.AsyncCursor`` class attributes expose
the cursor class for cross-driver isinstance checks without importing ``dqlitedbapi``."""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def test_sync_connection_class_exposes_cursor_attribute() -> None:
    assert getattr(dqlitedbapi.Connection, "Cursor") is Cursor  # noqa: B009


def test_async_connection_class_exposes_cursor_attribute() -> None:
    assert getattr(AsyncConnection, "AsyncCursor") is AsyncCursor  # noqa: B009


def test_sync_connection_instance_attribute_routes_to_class_attribute() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    assert getattr(conn, "Cursor") is Cursor  # noqa: B009


def test_async_connection_instance_attribute_routes_to_class_attribute() -> None:
    aconn = AsyncConnection("localhost:9001")
    assert getattr(aconn, "AsyncCursor") is AsyncCursor  # noqa: B009


def test_async_connection_class_exposes_cursor_attribute_aiosqlite_shape() -> None:
    """aiosqlite-shape parity: also expose the async cursor under ``Connection.Cursor``
    so ``isinstance(cur, conn.Cursor)`` works against our ``AsyncConnection``."""
    assert getattr(AsyncConnection, "Cursor") is AsyncCursor  # noqa: B009


def test_async_connection_instance_cursor_attribute_aiosqlite_shape() -> None:
    aconn = AsyncConnection("localhost:9001")
    assert getattr(aconn, "Cursor") is AsyncCursor  # noqa: B009
