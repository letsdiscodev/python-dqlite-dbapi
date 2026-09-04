"""When the operator's outer asyncio.timeout fires before the driver's inner
budget, the commit/rollback OperationalError annotates elapsed-vs-budget so a
dangling server-side transaction can be attributed to the right scope."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import OperationalError


def _make_conn() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._timeout = 5.0
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = MagicMock()
    conn._async_conn.in_transaction = True
    conn._loop_ref = None
    conn.messages = []
    conn._address = "host:1234"
    conn._cursors = MagicMock()
    conn._cursors.__iter__ = lambda self: iter([])
    conn._cursors.clear = MagicMock()
    conn._finalizer = None
    conn._creator_pid = os.getpid()
    conn._transaction_owner = None
    conn._ensure_locks = MagicMock(return_value=(asyncio.Lock(), asyncio.Lock()))
    conn._check_loop_only = MagicMock()
    return conn


@pytest.mark.asyncio
async def test_commit_timeout_outer_scope_hint_when_elapsed_short() -> None:
    """elapsed << budget when TimeoutError fires: message carries the
    "outer scope or sibling cancel likely interrupted" hint with elapsed."""
    conn = _make_conn()

    async def slow_execute(_sql: str) -> None:
        await asyncio.sleep(60)

    assert conn._async_conn is not None
    conn._async_conn.execute = MagicMock(side_effect=lambda _sql: slow_execute(_sql))

    # Inner budget ~20s; wrap in a short outer timeout so it fires first.
    with pytest.raises((OperationalError, asyncio.TimeoutError)) as ei:
        async with asyncio.timeout(0.05):
            await conn.commit()

    # Only assert the hint when the inner OperationalError arm fired (not a raw
    # TimeoutError escape, which is the outer-fires-first case covered elsewhere).
    if isinstance(ei.value, OperationalError):
        msg = str(ei.value)
        assert "outer scope or sibling cancel likely interrupted" in msg, (
            f"expected outer-scope hint in commit OperationalError; got: {msg!r}"
        )
        assert "elapsed" in msg


@pytest.mark.asyncio
async def test_rollback_timeout_outer_scope_hint_when_elapsed_short() -> None:
    conn = _make_conn()

    async def slow_execute(_sql: str) -> None:
        await asyncio.sleep(60)

    assert conn._async_conn is not None
    conn._async_conn.execute = MagicMock(side_effect=lambda _sql: slow_execute(_sql))

    with pytest.raises((OperationalError, asyncio.TimeoutError)) as ei:
        async with asyncio.timeout(0.05):
            await conn.rollback()

    if isinstance(ei.value, OperationalError):
        msg = str(ei.value)
        assert "outer scope or sibling cancel likely interrupted" in msg, (
            f"expected outer-scope hint in rollback OperationalError; got: {msg!r}"
        )


@pytest.mark.asyncio
async def test_commit_timeout_no_hint_when_inner_deadline_actually_expired() -> None:
    """Inner deadline actually expired (elapsed >= budget * 0.95): no hint."""
    conn = _make_conn()
    conn._timeout = 0.01  # commit_budget = ~0.04s with multiplier

    async def slow_execute(_sql: str) -> None:
        await asyncio.sleep(60)

    assert conn._async_conn is not None
    conn._async_conn.execute = MagicMock(side_effect=lambda _sql: slow_execute(_sql))

    with pytest.raises(OperationalError) as ei:
        await conn.commit()

    msg = str(ei.value)
    assert "outer scope or sibling cancel likely interrupted" not in msg, (
        f"inner-deadline expiry must NOT carry the outer-scope hint; got: {msg!r}"
    )
