"""``AsyncConnection.transaction()`` is the async-DB-API canonical
context manager wrapping ``BEGIN`` / ``COMMIT`` / ``ROLLBACK``.
Mirrors ``asyncpg.Connection.transaction()`` /
``psycopg.AsyncConnection.transaction()`` — the pattern that
asyncpg / psycopg / sqlalchemy_orm code expects when writing
``async with conn.transaction(): ...``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.asyncio
async def test_transaction_method_exists() -> None:
    """Pin: AsyncConnection has a ``transaction`` method (the
    feature itself, before behaviour). Cross-driver code that uses
    ``async with conn.transaction(): ...`` previously raised
    ``AttributeError`` outside ``dbapi.Error``."""
    conn = AsyncConnection("localhost:9001")
    assert hasattr(conn, "transaction")


@pytest.mark.asyncio
async def test_transaction_raises_interface_error_when_closed() -> None:
    conn = AsyncConnection("localhost:9001")
    # Force-close without ever connecting.
    conn._closed = True
    with pytest.raises(InterfaceError, match="closed"):
        async with conn.transaction():
            pass


@pytest.mark.asyncio
async def test_transaction_delegates_to_underlying_client_transaction() -> None:
    """The dbapi-async transaction() is plumbing — the cancellation-
    aware rollback discipline lives at the client layer
    (``dqliteclient.connection.DqliteConnection.transaction``).
    Verify the delegation calls the underlying context manager."""
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
