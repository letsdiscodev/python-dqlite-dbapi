"""Pin: ``AsyncConnection.close()`` runs the underlying close even when
a cancel lands during ``async with op_lock`` acquire.

Without the finally-best-effort wrapper, a CancelledError raised at
the lock-acquire point left ``_async_conn`` non-None pointing at a
connected ``DqliteConnection``, AND set ``_closed=True``. Subsequent
``close()`` retry early-returns on ``_closed=True``. The underlying
socket leaks until process exit.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio import AsyncConnection


@pytest.mark.asyncio
async def test_cancel_during_op_lock_acquire_still_closes_underlying() -> None:
    conn = AsyncConnection("localhost:9001")
    # Spin up locks via _ensure_locks-equivalent (use connect path's
    # private hook). Easier: hand-set the private state.
    fake_underlying = MagicMock()
    fake_underlying.close = AsyncMock()
    conn._async_conn = fake_underlying

    # Replace op_lock with one whose __aenter__ raises CancelledError
    # to simulate cancel-during-acquire.
    bad_lock = MagicMock()

    async def raise_cancel(_self: object) -> None:
        raise asyncio.CancelledError("simulated cancel during acquire")

    async def aexit(_self: object, *args: object) -> None:
        return None

    bad_lock.__aenter__ = raise_cancel
    bad_lock.__aexit__ = aexit
    conn._op_lock = bad_lock
    # Provide a connect_lock too so cleanup doesn't AttributeError.
    conn._connect_lock = MagicMock()

    with pytest.raises(asyncio.CancelledError):
        await conn.close()

    # The underlying close MUST have run in the finally.
    fake_underlying.close.assert_called_once()
    # And the conn slot must be cleared.
    assert conn._async_conn is None
    assert conn._op_lock is None
    assert conn._connect_lock is None


@pytest.mark.asyncio
async def test_close_success_path_unchanged() -> None:
    """Negative pin: the success path still calls close exactly once
    and doesn't double-close from the finally."""
    conn = AsyncConnection("localhost:9001")
    fake_underlying = MagicMock()
    fake_underlying.close = AsyncMock()
    conn._async_conn = fake_underlying
    conn._op_lock = asyncio.Lock()
    conn._connect_lock = asyncio.Lock()

    await conn.close()

    # Exactly one close — the body close, not the finally fallback.
    fake_underlying.close.assert_called_once()
    assert conn._async_conn is None
    assert conn._op_lock is None
