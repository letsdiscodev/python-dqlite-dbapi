"""Tests that cursor raises errors when the connection is not usable."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.cursor import Cursor


def _make_mock_connection_not_connected() -> MagicMock:
    """Create a mock Connection where the underlying DqliteConnection is not connected."""
    from dqliteclient.exceptions import DqliteConnectionError

    mock_async_conn = AsyncMock()
    mock_async_conn.query_raw = AsyncMock(side_effect=DqliteConnectionError("Not connected"))
    mock_async_conn.execute = AsyncMock(side_effect=DqliteConnectionError("Not connected"))

    mock_conn = MagicMock()

    async def get_async_conn() -> AsyncMock:
        return mock_async_conn

    mock_conn._get_async_connection = get_async_conn

    def run_sync(coro: object) -> object:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    mock_conn._run_sync = run_sync

    return mock_conn


class TestCursorProtocolCheck:
    def test_execute_query_raises_error_when_not_connected(self) -> None:
        """execute() should raise when the connection is not connected."""
        mock_conn = _make_mock_connection_not_connected()
        cursor = Cursor(mock_conn)

        with pytest.raises(Exception, match="Not connected"):
            cursor.execute("SELECT 1")

    def test_execute_dml_raises_error_when_not_connected(self) -> None:
        """execute() should raise for DML when the connection is not connected."""
        mock_conn = _make_mock_connection_not_connected()
        cursor = Cursor(mock_conn)

        with pytest.raises(Exception, match="Not connected"):
            cursor.execute("INSERT INTO t VALUES (1)")
