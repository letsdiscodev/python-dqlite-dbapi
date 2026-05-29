"""commit/rollback/close use _SYNC_PHASES_MULTIPLIER * self._timeout as the
op_lock acquire budget, matching the sync sibling, so the async surface does not
false-time-out on benign worst-case first-call-after-connect latency."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.connection import _SYNC_PHASES_MULTIPLIER
from dqlitedbapi.exceptions import OperationalError


def _prime_alive_inner() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn._timeout = 0.05
    conn.messages = []
    inner = MagicMock()
    inner._protocol = object()
    inner.in_transaction = True
    conn._async_conn = inner
    return conn


async def test_commit_message_shows_multi_phase_budget() -> None:
    """A commit timeout message names _SYNC_PHASES_MULTIPLIER * self._timeout."""
    conn = _prime_alive_inner()
    held_lock = asyncio.Lock()
    await held_lock.acquire()
    try:
        with (
            patch.object(conn, "_ensure_locks", return_value=(None, held_lock)),
            pytest.raises(OperationalError) as exc_info,
        ):
            await conn.commit()
        expected_budget = _SYNC_PHASES_MULTIPLIER * conn._timeout
        assert f"{expected_budget}s" in str(exc_info.value), (
            f"expected message to name the multi-phase budget "
            f"{expected_budget}s; got: {exc_info.value}"
        )
    finally:
        held_lock.release()


async def test_rollback_message_shows_multi_phase_budget() -> None:
    conn = _prime_alive_inner()
    held_lock = asyncio.Lock()
    await held_lock.acquire()
    try:
        with (
            patch.object(conn, "_ensure_locks", return_value=(None, held_lock)),
            pytest.raises(OperationalError) as exc_info,
        ):
            await conn.rollback()
        expected_budget = _SYNC_PHASES_MULTIPLIER * conn._timeout
        assert f"{expected_budget}s" in str(exc_info.value), (
            f"expected message to name the multi-phase budget "
            f"{expected_budget}s; got: {exc_info.value}"
        )
    finally:
        held_lock.release()


async def test_commit_completes_within_multi_phase_window() -> None:
    """An operation taking 2 * timeout (under the multi-phase budget) must
    succeed; the previous single-phase bound would have timed it out."""
    conn = _prime_alive_inner()
    delay = 2 * conn._timeout  # within budget (4x), would exceed 1x

    async def slow_execute(*_a: object, **_kw: object) -> object:
        await asyncio.sleep(delay)

        class _Sentinel: ...

        return _Sentinel()

    conn._async_conn.execute = MagicMock(  # type: ignore[union-attr]
        side_effect=slow_execute
    )
    # Patch _call_client to pass through the coroutine the execute mock returns.
    with patch(
        "dqlitedbapi.aio.connection._call_client",
        side_effect=lambda x: x,  # pass-through
    ):
        lock = asyncio.Lock()
        with patch.object(conn, "_ensure_locks", return_value=(None, lock)):
            await conn.commit()  # must not raise within the multi-phase budget
