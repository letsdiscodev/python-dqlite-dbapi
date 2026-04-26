"""Async commit/rollback must wrap client-layer exceptions into PEP 249 types.

The sync ``Connection._commit_async`` / ``_rollback_async`` route
through ``_call_client`` so every ``dqliteclient.exceptions`` class
surfaces as a ``dqlitedbapi.exceptions`` subclass. The async siblings
bypass that wrapping and leak raw client exceptions — which violates
PEP 249 ("all database errors expose as Error subclasses") and breaks
symmetry with the cursor execute path that already uses ``_call_client``.

Parametrised across every arm of ``_call_client`` so a future
client-side exception addition that silently bypasses the wrap will
trip one of the cases.
"""

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


# Every arm of ``_call_client`` (python-dqlite-dbapi/src/dqlitedbapi/
# cursor.py:88-115). Adding a new client-side class in this table at
# the same time as the ``_call_client`` branch keeps the wrap
# exhaustive.
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
        _client_exc.OperationalError(19, "UNIQUE failed"),
        _dbapi_exc.IntegrityError,
        id="operational-error-constraint",
    ),
    pytest.param(
        _client_exc.OperationalError(2, "internal"),
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

        # PEP 249 "Error" taxonomy: the raised class must be a subclass
        # of the driver module's Error, not a raw client class.
        assert isinstance(exc_info.value, _dbapi_exc.Error)
        # ``raise ... from e`` preserves the original so SQLAlchemy's
        # is_disconnect can walk __cause__ without relying on message
        # substrings.
        assert exc_info.value.__cause__ is raise_exc

    async def test_commit_integrity_error_carries_code(self) -> None:
        """Constraint-code OperationalError(19, ...) from the server
        on a COMMIT becomes an IntegrityError with ``.code`` preserved
        — so the no-tx swallow gate (which checks ``.code``) still
        works for the separate OperationalError(1, ...) case.
        """
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            19, "UNIQUE constraint failed"
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
    """The existing no-tx swallow (``_is_no_transaction_error``) must
    keep working once COMMIT/ROLLBACK route through ``_call_client``.
    The gate reads ``.code`` which the wrapped
    ``dbapi.OperationalError`` preserves (per ISSUE-168).
    """

    async def test_commit_no_tx_still_silent(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            1, "cannot commit - no transaction is active"
        )
        await conn.commit()  # silent no-op

    async def test_rollback_no_tx_still_silent(self) -> None:
        conn = _prime()
        assert conn._async_conn is not None
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[attr-defined]
            1, "cannot rollback - no transaction is active"
        )
        await conn.rollback()  # silent no-op
