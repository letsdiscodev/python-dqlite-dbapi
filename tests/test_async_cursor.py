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
    async def test_close_is_idempotent(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()
        await cursor.close()  # must not raise
        assert cursor._closed

    def test_connection_property(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.connection is conn

    @pytest.mark.asyncio
    async def test_fetchone_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()

        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchone()

    @pytest.mark.asyncio
    async def test_fetchmany_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchmany(5)

    @pytest.mark.asyncio
    async def test_fetchall_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchall()

    @pytest.mark.asyncio
    async def test_fetchone_without_execute_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(InterfaceError, match="No result set"):
            await cursor.fetchone()

    @pytest.mark.asyncio
    async def test_fetchmany_without_execute_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(InterfaceError, match="No result set"):
            await cursor.fetchmany(5)

    @pytest.mark.asyncio
    async def test_fetchall_without_execute_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(InterfaceError, match="No result set"):
            await cursor.fetchall()

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


class TestOptionalAsyncCursorMethodsRaise:
    def test_callproc_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.callproc("some_proc")

    def test_callproc_nextset_scroll_are_sync(self) -> None:
        """These three PEP 249 optional extensions all unconditionally raise
        ``NotSupportedError``. They must stay sync so callers can catch the
        error with a bare ``try: cursor.callproc(...) except ...`` rather
        than accidentally returning a coroutine object that is never awaited.
        The adapter in ``sqlalchemy-dqlite`` exposes the same three as sync.
        """
        import inspect

        assert not inspect.iscoroutinefunction(AsyncCursor.callproc)
        assert not inspect.iscoroutinefunction(AsyncCursor.nextset)
        assert not inspect.iscoroutinefunction(AsyncCursor.scroll)

    def test_nextset_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.nextset()

    def test_scroll_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.scroll(0)
