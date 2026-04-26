"""Pin: COMMIT raising leader-error code propagates (no silent swallow).

The dbapi's ``_is_no_transaction_error`` whitelist only swallows
``OperationalError`` whose primary code is in ``_NO_TX_CODES`` AND whose
message matches the no-tx substring. Leader-flip codes
(``SQLITE_IOERR_NOT_LEADER`` 10250, ``SQLITE_IOERR_LEADERSHIP_LOST``
10506) primary-mask to 10 — outside the whitelist — so they MUST
propagate.

A future refactor that "helpfully" widens the whitelist to also silence
leader-flip codes (e.g. treating "tx state ambiguous" like "no tx, no-op")
would silently swallow the leader-flip — the user would see clean exit
even though the row may not have committed. Catastrophic.

This test pins the contract for sync and async, ``commit`` and
``rollback``, and the context-manager exit paths that route through
``commit``.
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
                code, "leadership lost"
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
                code, "not leader"
            )
            with pytest.raises(OperationalError) as ei:
                conn.rollback()
            assert ei.value.code == code
        finally:
            conn._closed = True

    def test_exit_clean_commit_leader_flip_propagates(self, code: int) -> None:
        """``__exit__`` with no exception calls ``commit``; leader-flip
        must propagate up through the context manager, not be swallowed.
        Tracker-state cleanup is verified at the client layer in
        ``python-dqlite-client/tests/test_run_protocol_auto_rollback_codes.py``
        (for the auto-rollback set) and via ``_invalidate`` (for the
        leader-class set); the mock harness here only exercises the
        propagation contract.
        """
        conn = _make_sync_with_inner()
        try:
            conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
                code, "leadership lost"
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
            code, "leadership lost"
        )
        with pytest.raises(OperationalError) as ei:
            await conn.commit()
        assert ei.value.code == code

    async def test_rollback_leader_flip_raises(self, code: int) -> None:
        conn = _make_async_with_inner()
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            code, "not leader"
        )
        with pytest.raises(OperationalError) as ei:
            await conn.rollback()
        assert ei.value.code == code

    async def test_aexit_clean_commit_leader_flip_propagates(self, code: int) -> None:
        """``__aexit__`` with no exception calls ``commit``; leader-flip
        must propagate up through the context manager. Tracker-state
        cleanup is verified at the client layer (see the sync sibling
        comment for the cross-reference)."""
        conn = _make_async_with_inner()
        conn._async_conn.execute.side_effect = _client_exc.OperationalError(  # type: ignore[union-attr]
            code, "leadership lost"
        )
        with pytest.raises(OperationalError) as ei:
            await conn.__aexit__(None, None, None)
        assert ei.value.code == code
