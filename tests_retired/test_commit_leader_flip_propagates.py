"""Pin: COMMIT raising a leader-error code propagates (no silent swallow).

Leader-flip codes primary-mask to 10, outside ``_is_no_transaction_error``'s
whitelist, so they must propagate. Widening the whitelist to silence them would
report a clean exit while the row may not have committed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import OperationalError
from dqlitewire.constants import (
    SQLITE_IOERR_LEADERSHIP_LOST,
    SQLITE_IOERR_NOT_LEADER,
)

_LEADER_CODES = [SQLITE_IOERR_NOT_LEADER, SQLITE_IOERR_LEADERSHIP_LOST]


def _make_async_with_inner() -> AsyncConnection:
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock()
    inner.close = AsyncMock()
    inner.execute = AsyncMock()
    conn._async_conn = inner
    return conn


def _make_sync_with_inner() -> Connection:
    conn = Connection("localhost:9001")
    inner = MagicMock()
    inner.close = AsyncMock()
    inner.execute = AsyncMock()
    inner._in_use = False
    inner._bound_loop = None
    conn._async_conn = inner
    return conn


@pytest.mark.parametrize("code", _LEADER_CODES)
class TestSyncCommitLeaderFlipPropagates:
    def test_commit_leader_flip_raises(self, code: int) -> None:
        conn = _make_sync_with_inner()
        try:
            conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
                "leadership lost", code
            )
            with pytest.raises(OperationalError) as ei:
                conn.commit()
            assert ei.value.code == code
        finally:
            conn._closed = True

    def test_rollback_leader_flip_raises(self, code: int) -> None:
        conn = _make_sync_with_inner()
        try:
            conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
                "not leader", code
            )
            with pytest.raises(OperationalError) as ei:
                conn.rollback()
            assert ei.value.code == code
        finally:
            conn._closed = True

    def test_exit_clean_commit_leader_flip_propagates(self, code: int) -> None:
        """``__exit__`` with no exception calls commit; leader-flip must propagate."""
        conn = _make_sync_with_inner()
        try:
            conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
                "leadership lost", code
            )
            with pytest.raises(OperationalError) as ei:
                conn.__exit__(None, None, None)
            assert ei.value.code == code
        finally:
            conn._closed = True


@pytest.mark.parametrize("code", _LEADER_CODES)
class TestAsyncCommitLeaderFlipPropagates:
    async def test_commit_leader_flip_raises(self, code: int) -> None:
        conn = _make_async_with_inner()
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            "leadership lost", code
        )
        with pytest.raises(OperationalError) as ei:
            await conn.commit()
        assert ei.value.code == code

    async def test_rollback_leader_flip_raises(self, code: int) -> None:
        conn = _make_async_with_inner()
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            "not leader", code
        )
        with pytest.raises(OperationalError) as ei:
            await conn.rollback()
        assert ei.value.code == code

    async def test_aexit_clean_commit_leader_flip_propagates(self, code: int) -> None:
        """``__aexit__`` with no exception calls commit; leader-flip must propagate."""
        conn = _make_async_with_inner()
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            "leadership lost", code
        )
        with pytest.raises(OperationalError) as ei:
            await conn.__aexit__(None, None, None)
        assert ei.value.code == code
