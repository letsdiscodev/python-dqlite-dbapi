"""Async commit/rollback must route client-layer exceptions through _call_client so each
surfaces as a dbapi.Error subclass (PEP 249). Parametrised across every _call_client arm."""

from unittest.mock import AsyncMock, MagicMock

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi import exceptions as _dbapi_exc
from dqlitedbapi.aio.connection import AsyncConnection


def _prime(address: str = "localhost:19001") -> AsyncConnection:
    conn = AsyncConnection(address, database="x")
    inner = MagicMock()
    inner.close = AsyncMock()
    inner.execute = AsyncMock()
    conn._async_conn = inner
    return conn


# One row per _call_client arm; add a row alongside each new branch to keep the wrap exhaustive.
_WRAPPING_CASES = [
    pytest.param(
        _client_exc.DqliteConnectionError("socket closed"),
        _dbapi_exc.OperationalError,
        id="dqlite-connection-error",
    ),
    pytest.param(
        _client_exc.ClusterError("no leader"),
        _dbapi_exc.OperationalError,
        id="cluster-error",
    ),
    pytest.param(
        _client_exc.ProtocolError("bad frame"),
        _dbapi_exc.OperationalError,
        id="protocol-error",
    ),
    pytest.param(
        _client_exc.DataError("bad param"),
        _dbapi_exc.DataError,
        id="data-error",
    ),
    pytest.param(
        _client_exc.InterfaceError("misuse"),
        _dbapi_exc.InterfaceError,
        id="interface-error",
    ),
    pytest.param(
        _client_exc.OperationalError("UNIQUE failed", 19),
        _dbapi_exc.IntegrityError,
        id="operational-error-constraint",
    ),
    pytest.param(
        _client_exc.OperationalError("internal", 2),
        _dbapi_exc.InternalError,
        id="operational-error-internal",
    ),
]


class TestAsyncCommitWrapping:
    @pytest.mark.parametrize(("raise_exc", "expect_cls"), _WRAPPING_CASES)
    async def test_commit_wraps_client_exceptions(
        self, raise_exc: BaseException, expect_cls: type[BaseException]
    ) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = raise_exc  # type: ignore[attr-defined]

        with pytest.raises(expect_cls) as exc_info:
            await conn.commit()

        assert isinstance(exc_info.value, _dbapi_exc.Error)
        # __cause__ preserves the original so SA's is_disconnect can walk it without substrings.
        assert exc_info.value.__cause__ is raise_exc

    async def test_commit_integrity_error_carries_code(self) -> None:
        """OperationalError(19) becomes IntegrityError with .code preserved (the no-tx gate
        checks .code)."""
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            "UNIQUE constraint failed", 19
        )
        with pytest.raises(_dbapi_exc.IntegrityError) as exc_info:
            await conn.commit()
        assert exc_info.value.code == 19


class TestAsyncRollbackWrapping:
    @pytest.mark.parametrize(("raise_exc", "expect_cls"), _WRAPPING_CASES)
    async def test_rollback_wraps_client_exceptions(
        self, raise_exc: BaseException, expect_cls: type[BaseException]
    ) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = raise_exc  # type: ignore[attr-defined]

        with pytest.raises(expect_cls) as exc_info:
            await conn.rollback()

        assert isinstance(exc_info.value, _dbapi_exc.Error)
        assert exc_info.value.__cause__ is raise_exc


class TestAsyncNoTxSwallowSurvivesWrapping:
    """The no-tx swallow must keep working once commit/rollback route through _call_client:
    the gate reads .code, which the wrapped dbapi.OperationalError preserves."""

    async def test_commit_no_tx_still_silent(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            "cannot commit - no transaction is active", 1
        )
        await conn.commit()  # silent no-op

    async def test_rollback_no_tx_still_silent(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            "cannot rollback - no transaction is active", 1
        )
        await conn.rollback()  # silent no-op
