"""Tests for AsyncConnection class."""

import pytest

from dqlitedbapi.aio import connect
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError


class TestAsyncConnection:
    def test_connect_function_returns_async_connection(self) -> None:
        conn = connect("localhost:9001", database="test", timeout=5.0)
        assert isinstance(conn, AsyncConnection)

    def test_cursor_returns_async_cursor(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = conn.cursor()
        assert isinstance(cursor, AsyncCursor)

    @pytest.mark.asyncio
    async def test_close_marks_connection_closed(self) -> None:
        conn = AsyncConnection("localhost:9001")
        await conn.close()
        assert conn._closed

    @pytest.mark.asyncio
    async def test_cursor_on_closed_connection_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        await conn.close()

        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.cursor()

    @pytest.mark.asyncio
    async def test_commit_on_closed_connection_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        await conn.close()

        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn.commit()

    @pytest.mark.asyncio
    async def test_rollback_on_closed_connection_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        await conn.close()

        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn.rollback()

    def test_cursor_is_sync(self) -> None:
        """cursor() is intentionally sync for SQLAlchemy compatibility."""
        conn = AsyncConnection("localhost:9001")
        cursor = conn.cursor()
        # Verify it returns directly, not a coroutine
        assert isinstance(cursor, AsyncCursor)
