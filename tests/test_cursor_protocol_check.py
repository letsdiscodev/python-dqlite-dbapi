"""Tests that cursor raises InternalError when protocol is not initialized."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InternalError


def _make_mock_connection_no_protocol() -> MagicMock:
    """Create a mock Connection where _protocol is None."""
    mock_async_conn = AsyncMock()
    mock_async_conn._protocol = None
    mock_async_conn._db_id = None

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
    def test_execute_query_raises_internal_error_when_protocol_none(self) -> None:
        """execute() should raise InternalError, not AssertionError, when protocol is None."""
        mock_conn = _make_mock_connection_no_protocol()
        cursor = Cursor(mock_conn)

        with pytest.raises(InternalError, match="Connection protocol not initialized"):
            cursor.execute("SELECT 1")

    def test_execute_dml_raises_internal_error_when_protocol_none(self) -> None:
        """execute() should raise InternalError for DML when protocol is None."""
        mock_conn = _make_mock_connection_no_protocol()
        cursor = Cursor(mock_conn)

        with pytest.raises(InternalError, match="Connection protocol not initialized"):
            cursor.execute("INSERT INTO t VALUES (1)")
