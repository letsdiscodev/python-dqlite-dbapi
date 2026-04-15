"""Tests that protocol exceptions are wrapped in PEP 249 exception types."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import OperationalError


def _make_mock_connection_with_error(error: Exception) -> MagicMock:
    """Create a mock Connection where protocol raises the given error."""
    mock_protocol = AsyncMock()
    mock_protocol.exec_sql = AsyncMock(side_effect=error)
    mock_protocol.query_sql = AsyncMock(side_effect=error)

    mock_async_conn = AsyncMock()
    mock_async_conn._protocol = mock_protocol
    mock_async_conn._db_id = 0

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


class TestExceptionWrapping:
    def test_connection_error_wrapped_as_operational_error(self) -> None:
        """ConnectionError from protocol should become OperationalError."""
        mock_conn = _make_mock_connection_with_error(ConnectionError("connection lost"))
        cursor = Cursor(mock_conn)

        with pytest.raises(OperationalError, match="connection lost"):
            cursor.execute("SELECT 1")

    def test_os_error_wrapped_as_operational_error(self) -> None:
        """OSError from protocol should become OperationalError."""
        mock_conn = _make_mock_connection_with_error(OSError("network unreachable"))
        cursor = Cursor(mock_conn)

        with pytest.raises(OperationalError, match="network unreachable"):
            cursor.execute("INSERT INTO t VALUES (1)")

    def test_runtime_error_wrapped_as_operational_error(self) -> None:
        """Generic exceptions from protocol should become OperationalError."""
        mock_conn = _make_mock_connection_with_error(RuntimeError("unexpected"))
        cursor = Cursor(mock_conn)

        with pytest.raises(OperationalError, match="unexpected"):
            cursor.execute("SELECT 1")
