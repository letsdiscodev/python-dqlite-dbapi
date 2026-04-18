"""Cycle-3 bundle E: regression tests pinning previously-untested edges.

Covers:
- ISSUE-103: cursor.execute() on a cursor whose connection was closed
  externally (not via cursor.close()) raises InterfaceError.
"""

import asyncio

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError


class TestCursorAfterExternalConnectionClose:
    """ISSUE-103: operations on a live cursor whose connection was
    closed externally must raise InterfaceError rather than hanging
    or surfacing a cryptic lower-layer error.
    """

    def test_sync_cursor_execute_after_connection_close(self) -> None:
        import dqlitedbapi

        conn = dqlitedbapi.connect("localhost:19001")
        cursor = conn.cursor()
        conn.close()

        with pytest.raises(InterfaceError):
            cursor.execute("SELECT 1")

        with pytest.raises(InterfaceError):
            cursor.executemany("SELECT ?", [(1,), (2,)])

        # cursor.close() remains idempotent even after the connection is gone.
        cursor.close()
        cursor.close()

    def test_async_cursor_execute_after_connection_close(self) -> None:
        async def _run() -> None:
            conn = AsyncConnection("localhost:19001")
            # Don't actually connect — keep this a pure state-machine test
            # so it doesn't depend on the cluster being up.
            cursor = AsyncCursor(conn)
            await conn.close()

            with pytest.raises(InterfaceError):
                await cursor.execute("SELECT 1")

            with pytest.raises(InterfaceError):
                await cursor.executemany("SELECT ?", [(1,), (2,)])

        asyncio.run(_run())
