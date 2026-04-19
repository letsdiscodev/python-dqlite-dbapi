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

    @pytest.mark.asyncio
    async def test_aenter_cleans_up_on_connect_failure(self) -> None:
        """If ``connect()`` raises inside ``__aenter__``, partial state
        (lazily-constructed locks, loop-ref) must be reset so the object
        remains reusable. Python does NOT call ``__aexit__`` when
        ``__aenter__`` itself raises, so cleanup has to run in the
        ``__aenter__`` error path.
        """
        from unittest.mock import patch

        from dqliteclient.exceptions import DqliteConnectionError

        conn = AsyncConnection("localhost:9001")
        with (
            patch(
                "dqlitedbapi.aio.connection._build_and_connect",
                side_effect=DqliteConnectionError("synthetic connect failure"),
            ),
            pytest.raises(Exception, match="synthetic connect failure"),
        ):
            async with conn:
                pass

        # Connection object is in its "never connected" resting state.
        assert conn._async_conn is None
        # Lock primitives were reset so a retry on a fresh event loop
        # is not blocked by a loop-pinning error.
        assert conn._connect_lock is None
        assert conn._op_lock is None
        assert conn._loop_ref is None
