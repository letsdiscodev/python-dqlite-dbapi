"""Tests for AsyncCursor class."""

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError


class TestAsyncCursor:
    def test_description_initially_none(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.description is None

    def test_rowcount_initially_minus_one(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.rowcount == -1

    def test_arraysize_default(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.arraysize == 1

    def test_arraysize_setter(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.arraysize = 10
        assert cursor.arraysize == 10

    def test_lastrowid_initially_none(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.lastrowid is None

    @pytest.mark.asyncio
    async def test_close_marks_cursor_closed(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()
        assert cursor._closed

    @pytest.mark.asyncio
    async def test_fetchone_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()

        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchone()

    @pytest.mark.asyncio
    async def test_fetchone_no_rows_returns_none(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        # Simulate a query that returned zero rows
        cursor._description = [("id", None, None, None, None, None, None)]
        cursor._rows = []
        result = await cursor.fetchone()
        assert result is None

    @pytest.mark.asyncio
    async def test_fetchall_no_rows_returns_empty(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor._description = [("id", None, None, None, None, None, None)]
        cursor._rows = []
        result = await cursor.fetchall()
        assert result == []

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        conn = AsyncConnection("localhost:9001")
        async with AsyncCursor(conn) as cursor:
            assert not cursor._closed
        assert cursor._closed

    @pytest.mark.asyncio
    async def test_async_iterator(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor._rows = [(1, "a"), (2, "b"), (3, "c")]
        cursor._description = [("id", None, None, None, None, None, None)]

        results = [row async for row in cursor]
        assert results == [(1, "a"), (2, "b"), (3, "c")]

    def test_setinputsizes_noop(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.setinputsizes([None, None])

    def test_setoutputsize_noop(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.setoutputsize(100, 0)
