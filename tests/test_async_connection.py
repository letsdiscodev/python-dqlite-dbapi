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

    async def test_close_marks_connection_closed(self) -> None:
        conn = AsyncConnection("localhost:9001")
        await conn.close()
        assert conn._closed

    async def test_cursor_on_closed_connection_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        await conn.close()

        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.cursor()

    async def test_commit_on_closed_connection_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        await conn.close()

        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn.commit()

    async def test_rollback_on_closed_connection_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        await conn.close()

        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn.rollback()

    def test_cursor_is_sync(self) -> None:
        """cursor() is intentionally sync for SQLAlchemy compatibility."""
        conn = AsyncConnection("localhost:9001")
        cursor = conn.cursor()
        assert isinstance(cursor, AsyncCursor)

    async def test_aenter_cleans_up_on_connect_failure(self) -> None:
        """connect() raising in __aenter__ must reset partial state; Python does
        not call __aexit__ when __aenter__ raises, so cleanup runs in that path."""
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

        assert conn._async_conn is None
        # Locks reset so a retry on a fresh loop is not blocked by loop-pinning.
        assert conn._connect_lock is None
        assert conn._op_lock is None
        assert conn._loop_ref is None
