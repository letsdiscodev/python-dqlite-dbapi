"""``AsyncConnection.transaction()``: an ``async with`` context manager over
BEGIN/COMMIT/ROLLBACK, matching asyncpg/psycopg."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


async def test_transaction_method_exists() -> None:
    """Pin: AsyncConnection has a ``transaction`` method at all."""
    conn = AsyncConnection("localhost:9001")
    assert hasattr(conn, "transaction")


async def test_transaction_raises_interface_error_when_closed() -> None:
    conn = AsyncConnection("localhost:9001")
    conn._closed = True
    with pytest.raises(InterfaceError, match="closed"):
        async with conn.transaction():
            pass


async def test_transaction_delegates_to_underlying_client_transaction() -> None:
    """transaction() just delegates to the client-layer context manager (where the
    cancellation-aware rollback lives)."""
    conn = AsyncConnection("localhost:9001")

    fake_inner = MagicMock()
    enter = MagicMock()
    exit_ = MagicMock()
    fake_inner.transaction.return_value.__aenter__ = AsyncMock(side_effect=enter)
    fake_inner.transaction.return_value.__aexit__ = AsyncMock(side_effect=exit_)

    async def fake_ensure_connection() -> object:
        return fake_inner

    with patch.object(conn, "_ensure_connection", fake_ensure_connection):
        async with conn.transaction():
            pass

    fake_inner.transaction.assert_called_once_with()
    enter.assert_called_once()
    exit_.assert_called_once()
