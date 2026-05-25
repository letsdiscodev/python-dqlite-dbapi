"""Pin: ``AsyncConnection.__aexit__``'s commit-cancel arm and
rollback-Exception arm BOTH call ``force_close_transport()`` so the
slot is invalidated for SA pool reclaim.

SA's ``is_disconnect`` does NOT classify ``CancelledError`` /
``KeyboardInterrupt`` / ``SystemExit``, and it walks ``__cause__``
only (not ``__context__``). Without the defensive force-close,
the slot would return to the pool with ambiguous server-side
commit state (commit-cancel arm) or with an OPEN server-side
transaction (rollback-Exception arm).
"""

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
    """Clean-exit commit interrupted by CancelledError must
    force-close the transport before re-raising.
    """
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
    """Same pin for KeyboardInterrupt — equally ambiguous to SA's
    is_disconnect.
    """
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
    """Body raises -> rollback fails with an Exception-class error ->
    force_close_transport fires (DEBUG log already there, but the
    side-effect was missing). PEP 343: returning None lets the body
    exception propagate — pin via __aexit__ direct call.
    """
    from dqlitedbapi.exceptions import OperationalError

    conn = _make_conn()

    async def failing_rollback() -> None:
        raise OperationalError("simulated leader flip mid-ROLLBACK")

    conn.rollback = failing_rollback
    conn.force_close_transport = MagicMock(wraps=conn.force_close_transport)

    # __aexit__ called with a body exception triple.
    body_exc = ValueError("body failure")
    await conn.__aexit__(type(body_exc), body_exc, None)
    # PEP 343: returning None / falsy lets the body exception
    # propagate (return type annotation is None).
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


# Suppress unused-AsyncMock lint nag.
_ = AsyncMock
