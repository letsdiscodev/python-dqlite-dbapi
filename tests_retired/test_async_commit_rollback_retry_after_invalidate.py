"""commit/rollback raise InterfaceError when retried against an inner conn
invalidated by a prior cancel-mid-flight, rather than silently no-opping and
hiding the partial-commit ambiguity (matches asyncpg / psycopg)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dqlitedbapi.exceptions as _dbapi_exc
from dqlitedbapi.aio import AsyncConnection


def _prime_invalidated() -> AsyncConnection:
    """Inner client with _protocol=None, the invalidated-state sentinel."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn._timeout = 5.0
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = None  # invalidated
    conn._async_conn.in_transaction = False  # also cleared by invalidate
    return conn


async def test_commit_raises_interface_error_on_invalidated_inner() -> None:
    conn = _prime_invalidated()
    with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
        await conn.commit()


async def test_rollback_raises_interface_error_on_invalidated_inner() -> None:
    conn = _prime_invalidated()
    with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
        await conn.rollback()


async def test_commit_with_alive_inner_does_not_raise_invalidated_error() -> None:
    """Live _protocol + in_transaction=False: commit() is a silent no-op; the
    invalidation guard must not misfire on the documented happy path."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn._timeout = 5.0
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = object()  # alive
    conn._async_conn.in_transaction = False
    op_lock = AsyncMock()
    op_lock.__aenter__ = AsyncMock(return_value=op_lock)
    op_lock.__aexit__ = AsyncMock(return_value=False)
    with patch.object(conn, "_ensure_locks", return_value=(None, op_lock)):
        await conn.commit()  # silent no-op


async def test_commit_invalidate_during_lock_acquire_raises_interface_error() -> None:
    """A sibling _invalidate racing commit()'s op_lock acquire must not slip
    through: the under-lock recheck must consult _async_conn._protocol, since the
    pre-lock gate cannot guard the invalidate-during-acquire window."""
    import asyncio

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn._timeout = 5.0
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = object()  # initially alive
    conn._async_conn.in_transaction = False  # invalidate clears this too
    real_lock = asyncio.Lock()

    with patch.object(conn, "_ensure_locks", return_value=(None, real_lock)):
        await real_lock.acquire()  # hold the lock; commit() parks on it

        commit_task = asyncio.create_task(conn.commit())
        # Yield so commit() clears its pre-lock checks (protocol alive) and parks.
        await asyncio.sleep(0)
        # Sibling invalidate fires while parked: protocol gone, in_transaction cleared.
        conn._async_conn._protocol = None
        conn._async_conn.in_transaction = False
        real_lock.release()

        with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
            await commit_task


async def test_rollback_invalidate_during_lock_acquire_raises_interface_error() -> None:
    import asyncio

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn._timeout = 5.0
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
