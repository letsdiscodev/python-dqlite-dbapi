"""Tests that protocol exceptions propagate through the cursor.

Now that the cursor delegates to DqliteConnection.query_raw_typed()/execute(),
exception wrapping is handled by DqliteConnection._run_protocol().
These tests verify that exceptions from the connection layer propagate
correctly through the cursor.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import OperationalError


def _make_mock_connection_with_error(error: Exception) -> MagicMock:
    """Create a mock Connection where query_raw_typed/execute raise the given error."""
    mock_async_conn = AsyncMock()
    mock_async_conn.execute = AsyncMock(side_effect=error)
    mock_async_conn.query_raw_typed = AsyncMock(side_effect=error)

    mock_conn = MagicMock()

    async def get_async_conn() -> AsyncMock:
        return mock_async_conn

    mock_conn._get_async_connection = get_async_conn

    def run_sync(coro: object) -> object:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)  # type: ignore[arg-type]
        finally:
            loop.close()

    mock_conn._run_sync = run_sync

    return mock_conn


class TestExceptionWrapping:
    def test_operational_error_propagates(self) -> None:
        """OperationalError from DqliteConnection should propagate through cursor."""
        mock_conn = _make_mock_connection_with_error(OperationalError("connection lost"))
        cursor = Cursor(mock_conn)

        with pytest.raises(OperationalError, match="connection lost"):
            cursor.execute("SELECT 1")

    def test_dml_error_propagates(self) -> None:
        """Errors from DqliteConnection.execute() should propagate through cursor."""
        mock_conn = _make_mock_connection_with_error(OperationalError("network unreachable"))
        cursor = Cursor(mock_conn)

        with pytest.raises(OperationalError, match="network unreachable"):
            cursor.execute("INSERT INTO t VALUES (1)")

    def test_generic_exception_propagates(self) -> None:
        """Generic exceptions from DqliteConnection should propagate through cursor."""
        mock_conn = _make_mock_connection_with_error(RuntimeError("unexpected"))
        cursor = Cursor(mock_conn)

        with pytest.raises(RuntimeError, match="unexpected"):
            cursor.execute("SELECT 1")


class TestDqliteConnectionErrorWrapping:
    """ISSUE-349 regression fence: the dbapi wraps the client-level
    ``DqliteConnectionError`` into an ``OperationalError(code=None)``
    with the original preserved as ``__cause__`` so downstream
    disconnect detection can walk the chain without the dbapi needing
    to invent a new attribute.
    """

    async def test_wrap_preserves_cause_with_code_none(self) -> None:
        import dqliteclient.exceptions as _client_exc
        from dqlitedbapi.cursor import _call_client

        original = _client_exc.DqliteConnectionError("peer RST")

        async def raise_connection() -> None:
            raise original

        with pytest.raises(OperationalError) as exc_info:
            await _call_client(raise_connection())

        wrapped = exc_info.value
        assert wrapped.code is None
        assert wrapped.__cause__ is original
        assert isinstance(wrapped.__cause__, _client_exc.DqliteConnectionError)
