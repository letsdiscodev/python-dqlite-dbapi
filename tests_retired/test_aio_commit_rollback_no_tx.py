"""Async commit/rollback silently no-op on "no transaction is active" (matches stdlib sqlite3)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi import exceptions as _dbapi_exc
from dqlitedbapi.aio.connection import AsyncConnection


def _prime(address: str = "localhost:19001") -> AsyncConnection:
    """Build an AsyncConnection with a mocked inner client connection."""
    conn = AsyncConnection(address, database="x")
    inner = MagicMock()
    inner.close = AsyncMock()
    inner.execute = AsyncMock()
    conn._async_conn = inner
    return conn


class TestAsyncCommitNoTxSwallow:
    async def test_commit_swallows_no_transaction_error(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            "cannot commit - no transaction is active", 1
        )
        await conn.commit()  # silent no-op

    async def test_rollback_swallows_no_transaction_error(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            "cannot rollback - no transaction is active", 1
        )
        await conn.rollback()  # silent no-op

    async def test_commit_re_raises_other_operational_errors(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            "some unrelated error", 10
        )
        # Client OperationalError is wrapped into dbapi OperationalError, preserving code/message.
        with pytest.raises(_dbapi_exc.OperationalError, match="some unrelated error"):
            await conn.commit()

    async def test_rollback_re_raises_other_operational_errors(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            "some unrelated error", 10
        )
        with pytest.raises(_dbapi_exc.OperationalError, match="some unrelated error"):
            await conn.rollback()

    def test_sync_no_tx_helper_matches(self) -> None:
        from dqlitedbapi.connection import _is_no_transaction_error

        exc = _client_exc.OperationalError("cannot commit - no transaction is active", 1)
        assert _is_no_transaction_error(exc) is True
