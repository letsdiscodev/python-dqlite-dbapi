"""Tests for parameter conversion in cursor execute."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.cursor import Cursor


def _make_mock_connection() -> MagicMock:
    """Create a mock Connection with a fake async connection."""
    mock_protocol = AsyncMock()
    mock_protocol.exec_sql = AsyncMock(return_value=(0, 0))
    mock_protocol.query_sql = AsyncMock(return_value=(["id"], [[1]]))

    mock_async_conn = AsyncMock()
    mock_async_conn._protocol = mock_protocol
    mock_async_conn._db_id = 0

    mock_conn = MagicMock()

    async def get_async_conn() -> AsyncMock:
        return mock_async_conn

    mock_conn._get_async_connection = get_async_conn

    # Make _run_sync execute the coroutine in a new event loop
    def run_sync(coro: object) -> object:
        return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)

    mock_conn._run_sync = run_sync

    return mock_conn, mock_protocol


class TestParameterConversion:
    def test_empty_list_not_converted_to_none(self) -> None:
        """Passing parameters=[] should not convert to None."""
        mock_conn, mock_protocol = _make_mock_connection()
        cursor = Cursor(mock_conn)

        cursor.execute("INSERT INTO t VALUES (?)", [])

        mock_protocol.exec_sql.assert_called_once()
        call_args = mock_protocol.exec_sql.call_args[0]
        assert call_args[2] == [], f"Expected [], got {call_args[2]!r}"

    def test_none_params_stays_none(self) -> None:
        """Passing parameters=None should stay None."""
        mock_conn, mock_protocol = _make_mock_connection()
        cursor = Cursor(mock_conn)

        cursor.execute("INSERT INTO t VALUES (1)")

        mock_protocol.exec_sql.assert_called_once()
        call_args = mock_protocol.exec_sql.call_args[0]
        assert call_args[2] is None, f"Expected None, got {call_args[2]!r}"

    def test_nonempty_params_converted_to_list(self) -> None:
        """Passing parameters=(1, 2) should convert to [1, 2]."""
        mock_conn, mock_protocol = _make_mock_connection()
        cursor = Cursor(mock_conn)

        cursor.execute("INSERT INTO t VALUES (?, ?)", (1, 2))

        mock_protocol.exec_sql.assert_called_once()
        call_args = mock_protocol.exec_sql.call_args[0]
        assert call_args[2] == [1, 2], f"Expected [1, 2], got {call_args[2]!r}"
