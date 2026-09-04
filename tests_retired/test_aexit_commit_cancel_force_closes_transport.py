"""__aexit__'s commit-cancel and rollback-Exception arms both force_close_transport() to
invalidate the slot: SA's is_disconnect ignores cancel/KI/SystemExit and walks __cause__ only."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _make_conn() -> AsyncConnection:
    import os

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._async_conn = MagicMock()
    conn._loop_ref = None
    conn.messages = []
    conn._address = "host:1234"
    conn._cursors = MagicMock()
    conn._cursors.__iter__ = lambda self: iter([])
    conn._cursors.clear = MagicMock()
    conn._finalizer = None
    conn._creator_pid = os.getpid()
    return conn


@pytest.mark.asyncio
async def test_aexit_commit_cancel_force_closes_transport() -> None:
    conn = _make_conn()

    async def cancel_commit() -> None:
        raise asyncio.CancelledError()

    conn.commit = cancel_commit
    conn.force_close_transport = MagicMock(wraps=conn.force_close_transport)

    with pytest.raises(asyncio.CancelledError):
        await conn.__aexit__(None, None, None)

    conn.force_close_transport.assert_called_once()
    assert conn._closed is True


@pytest.mark.asyncio
async def test_aexit_commit_keyboardinterrupt_force_closes_transport() -> None:
    conn = _make_conn()

    async def ki_commit() -> None:
        raise KeyboardInterrupt()

    conn.commit = ki_commit
    conn.force_close_transport = MagicMock(wraps=conn.force_close_transport)

    with pytest.raises(KeyboardInterrupt):
        await conn.__aexit__(None, None, None)

    conn.force_close_transport.assert_called_once()


@pytest.mark.asyncio
async def test_aexit_rollback_exception_force_closes_transport() -> None:
    """Body raises -> rollback fails with an Exception -> force_close_transport fires."""
    from dqlitedbapi.exceptions import OperationalError

    conn = _make_conn()

    async def failing_rollback() -> None:
        raise OperationalError("simulated leader flip mid-ROLLBACK")

    conn.rollback = failing_rollback
    conn.force_close_transport = MagicMock(wraps=conn.force_close_transport)

    body_exc = ValueError("body failure")
    await conn.__aexit__(type(body_exc), body_exc, None)
    conn.force_close_transport.assert_called_once()
    assert conn._closed is True


@pytest.mark.asyncio
async def test_aexit_clean_exit_no_force_close_on_success() -> None:
    """Regression: clean-exit commit success does NOT force_close."""
    conn = _make_conn()

    async def ok_commit() -> None:
        return None

    conn.commit = ok_commit
    conn.force_close_transport = MagicMock(wraps=conn.force_close_transport)

    await conn.__aexit__(None, None, None)
    conn.force_close_transport.assert_not_called()


@pytest.mark.asyncio
async def test_aexit_rollback_success_no_force_close() -> None:
    """Regression: body raises, rollback succeeds → no force_close."""
    conn = _make_conn()

    async def ok_rollback() -> None:
        return None

    conn.rollback = ok_rollback
    conn.force_close_transport = MagicMock(wraps=conn.force_close_transport)

    await conn.__aexit__(ValueError, ValueError("body"), None)
    conn.force_close_transport.assert_not_called()


_ = AsyncMock  # keep import for ruff
