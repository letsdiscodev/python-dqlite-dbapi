"""Cursor.execute() on a cursor whose connection was closed externally
(not via cursor.close()) raises InterfaceError.
"""

import asyncio

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError


class TestCursorAfterExternalConnectionClose:
    """A live cursor whose connection was closed externally must raise InterfaceError."""

    def test_sync_cursor_execute_after_connection_close(self) -> None:
        # When the connection's _run_sync raises InterfaceError (its response
        # to a closed connection), the cursor surface must propagate it.
        from dqlitedbapi.cursor import Cursor

        class _ClosedConn:
            _closed = True

            def _check_thread(self) -> None:
                return None

            def _run_sync(self, coro) -> None:  # noqa: ANN001
                coro.close()
                raise InterfaceError("Connection is closed")

        cursor = Cursor(_ClosedConn())  # type: ignore[arg-type]

        with pytest.raises(InterfaceError):
            cursor.execute("SELECT 1")

        with pytest.raises(InterfaceError):
            cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

        # close() stays idempotent even after the connection is gone.
        cursor.close()
        cursor.close()

    def test_async_cursor_execute_after_connection_close(self) -> None:
        async def _run() -> None:
            conn = AsyncConnection("localhost:19001")
            # Don't connect — keep this a pure state-machine test.
            cursor = AsyncCursor(conn)
            await conn.close()

            with pytest.raises(InterfaceError):
                await cursor.execute("SELECT 1")

            with pytest.raises(InterfaceError):
                await cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

        asyncio.run(_run())
