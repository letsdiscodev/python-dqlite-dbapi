"""Async commit/rollback "no transaction is active" swallow behaviour.

Mirrors the sync-side tests for ``_is_no_transaction_error``. The
async path has the same silent-no-op contract (matches stdlib
sqlite3); without dedicated tests, a regression that widened or
narrowed the match would surface only via integration.
"""

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
            1, "cannot commit - no transaction is active"
        )
        await conn.commit()  # silent no-op

    async def test_rollback_swallows_no_transaction_error(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            1, "cannot rollback - no transaction is active"
        )
        await conn.rollback()  # silent no-op

    async def test_commit_re_raises_other_operational_errors(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            10, "some unrelated error"
        )
        # The client-layer OperationalError is wrapped into the PEP 249
        # dbapi OperationalError (via ``_call_client``); the code and
        # message are preserved. Matching on message shape so the test
        # fails loudly if either the wrap or the str representation
        # ("[code] message") regresses.
        with pytest.raises(_dbapi_exc.OperationalError, match="some unrelated error"):
            await conn.commit()

    async def test_rollback_re_raises_other_operational_errors(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            10, "some unrelated error"
        )
        with pytest.raises(_dbapi_exc.OperationalError, match="some unrelated error"):
            await conn.rollback()

    def test_sync_no_tx_helper_matches(self) -> None:
        """Sanity check for sync/async parity on the helper that drives
        both the dbapi sync Connection.commit/rollback and the
        AsyncConnection.commit/rollback swallow paths.
        """
        from dqlitedbapi.connection import _is_no_transaction_error

        exc = _client_exc.OperationalError(1, "cannot commit - no transaction is active")
        assert _is_no_transaction_error(exc) is True
