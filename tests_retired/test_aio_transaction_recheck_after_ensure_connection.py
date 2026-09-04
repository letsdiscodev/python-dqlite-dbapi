"""``transaction()`` re-checks ``_closed`` / ``_async_conn`` after the
``_ensure_connection`` await and before reserving ``_transaction_owner``, so a racing
``force_close_transport`` does not pin the slot and mislead siblings with a "nested"
error instead of "closed"."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.asyncio
async def test_transaction_raises_when_close_races_ensure_connection() -> None:
    """Close racing during the ``_ensure_connection`` await: ctxmgr raises InterfaceError
    and must not pin the ``_transaction_owner`` slot."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._async_conn = MagicMock()
    conn._async_conn.transaction = MagicMock()
    conn._transaction_owner = None
    conn._loop_ref = None
    conn.messages = []
    conn._address = "host:1234"

    conn._check_loop_binding = MagicMock()

    inner = MagicMock()

    async def fake_ensure_connection() -> object:
        # Foreign-thread close racing in during the await.
        conn._closed = True
        conn._async_conn = None
        return inner

    conn._ensure_connection = fake_ensure_connection  # type: ignore[assignment]

    with pytest.raises(InterfaceError, match="closed during transaction setup"):
        async with conn.transaction():
            pytest.fail("body must never run")

    assert conn._transaction_owner is None  # recheck fired before reservation


@pytest.mark.asyncio
async def test_transaction_happy_path_unchanged() -> None:
    """Regression: no concurrent close → ctxmgr enters the body."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._transaction_owner = None
    conn._loop_ref = None
    conn.messages = []
    conn._address = "host:1234"

    inner_tx = MagicMock()
    inner_tx.__aenter__ = AsyncMock(return_value=None)
    inner_tx.__aexit__ = AsyncMock(return_value=None)

    inner = MagicMock()
    inner.transaction = MagicMock(return_value=inner_tx)
    conn._async_conn = inner

    async def fake_ensure_connection() -> object:
        return inner

    conn._ensure_connection = fake_ensure_connection  # type: ignore[assignment]
    conn._check_loop_binding = MagicMock()

    body_ran = False
    async with conn.transaction():
        body_ran = True
        assert conn._transaction_owner is not None
    assert body_ran
    assert conn._transaction_owner is None
