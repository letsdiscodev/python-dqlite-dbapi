"""Cursor translates ``dqliteclient.exceptions.*`` to the PEP 249 ``dqlitedbapi.exceptions.*``."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import dqliteclient.exceptions as client_exc
import dqlitedbapi.exceptions as dbapi_exc
from dqlitedbapi.cursor import Cursor


def _cursor_with_async_conn_raising(exc: Exception) -> Cursor:
    """Build a Cursor whose async conn raises ``exc`` on every query/execute."""
    mock_async_conn = AsyncMock()
    mock_async_conn.query_raw_typed = AsyncMock(side_effect=exc)
    mock_async_conn.execute = AsyncMock(side_effect=exc)

    mock_conn = MagicMock()

    async def get_async_conn() -> AsyncMock:
        return mock_async_conn

    mock_conn._get_async_connection = get_async_conn

    def run_sync(coro: object) -> object:
        return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

    mock_conn._run_sync = run_sync
    return Cursor(mock_conn)


class TestExceptionWrapping:
    def test_client_operational_error_becomes_dbapi_operational_error(self) -> None:
        c = _cursor_with_async_conn_raising(client_exc.OperationalError("boom", 1))
        with pytest.raises(dbapi_exc.OperationalError, match="boom"):
            c.execute("SELECT 1")

    def test_client_connection_error_becomes_operational_error(self) -> None:
        c = _cursor_with_async_conn_raising(client_exc.DqliteConnectionError("no route"))
        with pytest.raises(dbapi_exc.OperationalError, match="no route"):
            c.execute("SELECT 1")

    def test_client_protocol_error_becomes_operational_error(self) -> None:
        # Wire desync maps to OperationalError so SA's is_disconnect invalidates the pool slot.
        c = _cursor_with_async_conn_raising(client_exc.ProtocolError("bad frame"))
        with pytest.raises(dbapi_exc.OperationalError, match="bad frame"):
            c.execute("SELECT 1")

    def test_client_data_error_becomes_data_error(self) -> None:
        c = _cursor_with_async_conn_raising(client_exc.DataError("bad param"))
        # Wire-primitive bind (int) so the _convert_bind_param guard doesn't intercept first;
        # this exercises the protocol-level DataError mapping, not bind validation.
        with pytest.raises(dbapi_exc.DataError, match="bad param"):
            c.execute("INSERT INTO t VALUES (?)", [1])

    def test_chained_cause_preserved(self) -> None:
        original = client_exc.OperationalError("original", 1)
        c = _cursor_with_async_conn_raising(original)
        try:
            c.execute("SELECT 1")
        except dbapi_exc.OperationalError as e:
            assert e.__cause__ is original
        else:
            pytest.fail("expected OperationalError")
