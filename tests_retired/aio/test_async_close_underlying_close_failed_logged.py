"""Pin: when both the body close and the shielded cleanup close raise,
the primary (body) exception propagates and the secondary cleanup
failure is DEBUG-logged with ``exc_info`` and swallowed."""

from __future__ import annotations

import asyncio
import logging
import os as _os
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _prime_connection() -> AsyncConnection:
    """Build an AsyncConnection scaffolded enough to drive ``close()``
    without a real cluster."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._async_conn = None
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []
    conn._timeout = 5.0
    conn._close_timeout = 0.5
    conn._creator_pid = _os.getpid()
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    return conn


async def test_close_finally_exception_arm_swallows_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Primary body RuntimeError propagates; secondary shielded-close
    RuntimeError is DEBUG-logged with ``exc_info`` and swallowed; the
    slot is still cleared so close stays idempotent."""
    conn = _prime_connection()
    conn._ensure_locks()

    inner = MagicMock()
    # First close (body) raises so the body skips clearing _async_conn;
    # second (shielded) raises a non-Cancel Exception -> bare-Exception arm.
    inner.close = AsyncMock(
        side_effect=[
            RuntimeError("body close failed"),
            RuntimeError("shielded close also failed"),
        ]
    )
    conn._async_conn = inner

    caplog.set_level(logging.DEBUG, logger="dqlitedbapi.aio.connection")

    with pytest.raises(RuntimeError, match="body close failed"):
        await conn.close()

    assert conn._async_conn is None
    assert inner.close.call_count == 2

    debug_rec = next(
        (r for r in caplog.records if "underlying close failed" in r.getMessage()),
        None,
    )
    assert debug_rec is not None, (
        "expected a DEBUG record from the bare-Exception suppression arm "
        "at aio/connection.py:732-737"
    )
    assert debug_rec.levelno == logging.DEBUG, (
        "underlying-close failure must log at DEBUG — a refactor that "
        "bumps to WARNING/ERROR surfaces the secondary failure too loudly "
        "for SA's pool-dispose path"
    )
    assert debug_rec.exc_info is not None, (
        "DEBUG record must carry exc_info pointing at the close-time exception "
        "so operators can triage transport-tear-down failures"
    )
    assert isinstance(debug_rec.exc_info[1], RuntimeError)
    assert "shielded close also failed" in str(debug_rec.exc_info[1]), (
        "DEBUG must capture the SECONDARY (cleanup) failure, not the primary — "
        "primary is what the caller sees in the propagating exception"
    )


async def test_close_finally_exception_arm_does_not_match_cancelled() -> None:
    """CancelledError must hit its dedicated re-raise arm, not the
    bare-Exception arm; swallowing it would break TaskGroup cancellation."""
    conn = _prime_connection()
    conn._ensure_locks()

    inner = MagicMock()
    inner.close = AsyncMock(
        side_effect=[
            RuntimeError("body close failed"),
            asyncio.CancelledError(),
        ]
    )
    conn._async_conn = inner

    with pytest.raises(asyncio.CancelledError):
        await conn.close()
