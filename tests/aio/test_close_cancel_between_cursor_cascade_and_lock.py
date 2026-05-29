"""Pin: ``AsyncConnection.close()`` runs the underlying close even when
a cancel lands during ``async with op_lock`` acquire — otherwise the
socket leaks (``_closed=True`` makes the retry early-return)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio import AsyncConnection


async def test_cancel_during_op_lock_acquire_still_closes_underlying() -> None:
    conn = AsyncConnection("localhost:9001")
    fake_underlying = MagicMock()
    fake_underlying.close = AsyncMock()
    conn._async_conn = fake_underlying

    # op_lock whose __aenter__ raises CancelledError: cancel-during-acquire.
    bad_lock = MagicMock()

    async def raise_cancel(_self: object) -> None:
        raise asyncio.CancelledError("simulated cancel during acquire")

    async def aexit(_self: object, *args: object) -> None:
        return None

    bad_lock.__aenter__ = raise_cancel
    bad_lock.__aexit__ = aexit
    conn._op_lock = bad_lock
    conn._connect_lock = MagicMock()

    with pytest.raises(asyncio.CancelledError):
        await conn.close()

    fake_underlying.close.assert_called_once()
    assert conn._async_conn is None
    assert conn._op_lock is None
    assert conn._connect_lock is None


async def test_close_success_path_unchanged() -> None:
    """Negative pin: success path closes exactly once, no double-close
    from the finally."""
    conn = AsyncConnection("localhost:9001")
    fake_underlying = MagicMock()
    fake_underlying.close = AsyncMock()
    conn._async_conn = fake_underlying
    conn._op_lock = asyncio.Lock()
    conn._connect_lock = asyncio.Lock()

    await conn.close()

    fake_underlying.close.assert_called_once()
    assert conn._async_conn is None
    assert conn._op_lock is None
