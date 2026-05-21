"""Pin: ``AsyncConnection.commit/rollback/close`` apply
``_SYNC_PHASES_MULTIPLIER * self._timeout`` as the op_lock acquire
budget so the async surface absorbs the same multi-phase budget the
sync ``Connection._run_sync`` already honours.

The sync sibling applies ``sync_timeout = _SYNC_PHASES_MULTIPLIER *
self._timeout`` at the ``Future.result(timeout=sync_timeout)``
boundary. The async side used to apply only ``self._timeout`` —
false-timing-out on benign worst-case latency (slow first-call-after-
connect: handshake + open_database + COMMIT) that the sync side
tolerates.
"""

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
    conn._timeout = 0.05  # tight so the test finishes fast
    conn.messages = []
    inner = MagicMock()
    inner._protocol = object()
    inner.in_transaction = True
    conn._async_conn = inner
    return conn


async def test_commit_message_shows_multi_phase_budget() -> None:
    """When ``commit`` times out the error message names the
    ``_SYNC_PHASES_MULTIPLIER * self._timeout`` figure — pin that the
    multiplier is applied at the boundary."""
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
    """Positive control: an operation that takes ``2 * timeout`` (i.e.
    LESS than the ``_SYNC_PHASES_MULTIPLIER * timeout`` budget) must
    succeed on the async surface. Under the previous single-phase
    bound this would time out."""
    conn = _prime_alive_inner()
    delay = 2 * conn._timeout  # within budget (4x), would exceed 1x

    async def slow_execute(*_a: object, **_kw: object) -> object:
        await asyncio.sleep(delay)

        class _Sentinel: ...

        return _Sentinel()

    conn._async_conn.execute = MagicMock(  # type: ignore[union-attr]
        side_effect=slow_execute
    )
    # Patch ``_call_client`` indirectly: the execute mock returns a
    # coroutine-like object. We patch ``_call_client`` to await it.
    with patch(
        "dqlitedbapi.aio.connection._call_client",
        side_effect=lambda x: x,  # pass-through
    ):
        # Provide a real lock so the body proceeds.
        lock = asyncio.Lock()
        with patch.object(conn, "_ensure_locks", return_value=(None, lock)):
            # No assertion needed beyond "did not raise within the
            # multi-phase budget".
            await conn.commit()
