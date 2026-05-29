"""Exceptions from the connection layer propagate correctly through the cursor."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import OperationalError


def _make_mock_connection_with_error(error: Exception) -> MagicMock:
    """Mock Connection whose query_raw_typed/execute raise the given error."""
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
        mock_conn = _make_mock_connection_with_error(OperationalError("connection lost"))
        cursor = Cursor(mock_conn)

        with pytest.raises(OperationalError, match="connection lost"):
            cursor.execute("SELECT 1")

    def test_dml_error_propagates(self) -> None:
        mock_conn = _make_mock_connection_with_error(OperationalError("network unreachable"))
        cursor = Cursor(mock_conn)

        with pytest.raises(OperationalError, match="network unreachable"):
            cursor.execute("INSERT INTO t VALUES (1)")

    def test_generic_exception_propagates(self) -> None:
        mock_conn = _make_mock_connection_with_error(RuntimeError("unexpected"))
        cursor = Cursor(mock_conn)

        with pytest.raises(RuntimeError, match="unexpected"):
            cursor.execute("SELECT 1")


class TestDqliteConnectionErrorWrapping:
    """dbapi wraps client ``DqliteConnectionError`` into ``OperationalError`` with the
    original preserved as ``__cause__`` so disconnect detection can walk the chain."""

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

    @pytest.mark.parametrize(
        "code",
        # 0 is the only value distinguishing a verbatim forward from a ``code or None``
        # truthiness mutant; 10240/10250 are leader-change codes, 5/6 are BUSY/LOCKED.
        [0, 5, 6, 10240, 10250],
    )
    async def test_wrap_forwards_non_none_code_to_operationalerror(self, code: int) -> None:
        """``_call_client`` forwards the client error's ``code`` to ``OperationalError.code``
        without coercion; SA's ``is_disconnect`` LEADER_ERROR_CODES branch depends on it."""
        import dqliteclient.exceptions as _client_exc
        from dqlitedbapi.cursor import _call_client

        original = _client_exc.DqliteConnectionError("not leader", code=code)

        async def raise_connection() -> None:
            raise original

        with pytest.raises(OperationalError) as exc_info:
            await _call_client(raise_connection())

        assert exc_info.value.code == code
        assert exc_info.value.__cause__ is original
