"""Pin: ``AsyncConnection.commit()`` / ``rollback()`` raise
``InterfaceError`` when retried against an inner client connection
that was invalidated by a prior cancel-mid-flight.

Without this guard, the retry hit ``getattr(inner, 'in_transaction',
False)`` → False (cleared by ``_invalidate``) → silent return,
hiding the partial-commit ambiguity (the cancelled COMMIT may or may
not have reached the leader). asyncpg / psycopg explicitly raise on
the same retry pattern; this driver now matches.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dqlitedbapi.exceptions as _dbapi_exc
from dqlitedbapi.aio import AsyncConnection


def _prime_invalidated() -> AsyncConnection:
    """Build an AsyncConnection wrapping an inner client whose
    ``_protocol`` is None (the sentinel for invalidated state)."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = None  # invalidated
    conn._async_conn.in_transaction = False  # also cleared by invalidate
    return conn


@pytest.mark.asyncio
async def test_commit_raises_interface_error_on_invalidated_inner() -> None:
    """A retry of commit() against an invalidated inner client conn
    must NOT silently no-op; raise InterfaceError so caller code
    cannot mistakenly treat the retry as a clean commit."""
    conn = _prime_invalidated()
    with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
        await conn.commit()


@pytest.mark.asyncio
async def test_rollback_raises_interface_error_on_invalidated_inner() -> None:
    conn = _prime_invalidated()
    with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
        await conn.rollback()


@pytest.mark.asyncio
async def test_commit_with_alive_inner_does_not_raise_invalidated_error() -> None:
    """Negative pin: when the inner has a live ``_protocol`` and
    ``in_transaction=False``, commit() returns silently (PEP 249
    documented no-tx no-op) — the invalidation guard does NOT misfire
    on the documented happy path."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = object()  # alive
    conn._async_conn.in_transaction = False
    op_lock = AsyncMock()
    op_lock.__aenter__ = AsyncMock(return_value=op_lock)
    op_lock.__aexit__ = AsyncMock(return_value=False)
    with patch.object(conn, "_ensure_locks", return_value=(None, op_lock)):
        await conn.commit()  # silent no-op


@pytest.mark.asyncio
async def test_commit_invalidate_during_lock_acquire_raises_interface_error() -> None:
    """Pin: a sibling-task ``_invalidate`` racing with ``commit()``'s
    ``async with op_lock`` acquire must NOT slip through the
    in_transaction-False short-circuit. The pre-lock ``_protocol is None``
    gate fires only on retry-after-invalidate; it cannot guard the
    invalidate-during-acquire window. The under-lock recheck must
    consult ``_async_conn._protocol`` (not just ``_closed`` /
    ``_async_conn``) so the partial-commit ambiguity discipline holds
    across the sibling-task cancel shape too.

    Repro shape: hold ``op_lock`` from a fixture-task, schedule the
    real ``commit()`` so it parks on the lock, then mutate
    ``_async_conn._protocol = None`` (simulating a sibling task's
    ``_invalidate``) while commit is parked. Release the lock. The
    commit must observe the invalidated state and raise InterfaceError.
    """
    import asyncio

    # Build an AsyncConnection with a live _protocol initially.
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = object()  # initially alive
    conn._async_conn.in_transaction = False  # invalidate clears this too
    # Real asyncio.Lock so the parking semantics match production.
    real_lock = asyncio.Lock()

    with patch.object(conn, "_ensure_locks", return_value=(None, real_lock)):
        # Hold the lock from a fixture-task; commit() will park on it.
        await real_lock.acquire()

        commit_task = asyncio.create_task(conn.commit())
        # Yield once so commit() runs through its pre-lock checks
        # (which see _protocol alive) and parks on op_lock.
        await asyncio.sleep(0)
        # Sibling task's invalidate fires now: protocol gone,
        # in_transaction cleared.
        conn._async_conn._protocol = None
        conn._async_conn.in_transaction = False
        # Release the lock so commit() can proceed under-lock.
        real_lock.release()

        with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
            await commit_task


@pytest.mark.asyncio
async def test_rollback_invalidate_during_lock_acquire_raises_interface_error() -> None:
    """Sibling pin for rollback() — same race shape as commit()."""
    import asyncio

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = object()
    conn._async_conn.in_transaction = False
    real_lock = asyncio.Lock()

    with patch.object(conn, "_ensure_locks", return_value=(None, real_lock)):
        await real_lock.acquire()
        rollback_task = asyncio.create_task(conn.rollback())
        await asyncio.sleep(0)
        conn._async_conn._protocol = None
        conn._async_conn.in_transaction = False
        real_lock.release()

        with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
            await rollback_task
