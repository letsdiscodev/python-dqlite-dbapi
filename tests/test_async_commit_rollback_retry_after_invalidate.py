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
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = object()  # alive
    conn._async_conn.in_transaction = False
    op_lock = AsyncMock()
    op_lock.__aenter__ = AsyncMock(return_value=op_lock)
    op_lock.__aexit__ = AsyncMock(return_value=False)
    with patch.object(conn, "_ensure_locks", return_value=(None, op_lock)):
        await conn.commit()  # silent no-op
