"""commit/rollback distinguish the op_lock-acquire phase from the round-trip
phase in their TimeoutError diagnostic, since the single timeout covers both and
a late RTT timeout would otherwise be misattributed to lock acquire."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import OperationalError


def _make_conn(
    *,
    in_transaction: bool = True,
    commit_blocks: bool = False,
) -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = "host:1234"
    conn._database = "x"
    conn._closed = False
    conn._closed_flag = [False]
    conn._timeout = 0.05
    conn._transaction_owner = None
    import os as _os
    import weakref

    conn._creator_pid = _os.getpid()
    conn._connect_lock = asyncio.Lock()
    conn._op_lock = asyncio.Lock()
    # Bind locks to the current loop so _ensure_locks reuses ours, not fresh ones.
    conn._loop_ref = weakref.ref(asyncio.get_event_loop())

    inner = MagicMock()
    inner._protocol = MagicMock()
    inner.in_transaction = in_transaction

    if commit_blocks:

        async def slow_execute(sql: str) -> None:
            await asyncio.sleep(60)
    else:

        async def slow_execute(sql: str) -> None:
            return None

    inner.execute = slow_execute
    conn._async_conn = inner
    conn.messages = []
    return conn


@pytest.mark.asyncio
async def test_commit_timeout_during_rtt_attributes_to_round_trip_phase() -> None:
    """No lock contention, COMMIT RTT blocks past budget: blame COMMIT round-trip."""
    conn = _make_conn(commit_blocks=True)
    with pytest.raises(OperationalError, match="COMMIT round-trip"):
        await conn.commit()


@pytest.mark.asyncio
async def test_rollback_timeout_during_rtt_attributes_to_round_trip_phase() -> None:
    conn = _make_conn(commit_blocks=True)
    with pytest.raises(OperationalError, match="ROLLBACK round-trip"):
        await conn.rollback()


@pytest.mark.asyncio
async def test_commit_timeout_during_lock_acquire_attributes_to_lock_phase() -> None:
    """Sibling holds the lock so commit() parks on acquire and times out there:
    blame op_lock acquire."""
    conn = _make_conn()

    holding = asyncio.Event()
    release = asyncio.Event()

    op_lock = conn._op_lock
    assert op_lock is not None

    async def hold_lock() -> None:
        async with op_lock:
            holding.set()
            await release.wait()

    sibling = asyncio.create_task(hold_lock())
    try:
        await holding.wait()
        assert conn._op_lock is not None and conn._op_lock.locked()
        with pytest.raises(OperationalError, match="op_lock acquire"):
            await conn.commit()
    finally:
        release.set()
        await sibling


# Quiet unused-import lint for AsyncMock.
_ = AsyncMock
