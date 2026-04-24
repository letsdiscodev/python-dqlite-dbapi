"""``Connection.in_transaction`` / ``AsyncConnection.in_transaction``
mirror stdlib ``sqlite3.Connection.in_transaction``. Closed or
never-used connections return False; the live state tracks the
underlying client-layer ``DqliteConnection.in_transaction``.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection


def test_sync_never_used_returns_false() -> None:
    conn = Connection("127.0.0.1:9001")
    assert conn.in_transaction is False


def test_sync_closed_returns_false() -> None:
    conn = Connection("127.0.0.1:9001")
    conn.close()
    # Closed Connection may still inspect the property; return False.
    assert conn.in_transaction is False


@pytest.mark.asyncio
async def test_async_never_used_returns_false() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    assert conn.in_transaction is False


@pytest.mark.asyncio
async def test_async_delegates_to_client_layer() -> None:
    from unittest.mock import MagicMock

    conn = AsyncConnection("127.0.0.1:9001")
    mock_client = MagicMock()
    mock_client.in_transaction = True
    conn._async_conn = mock_client  # type: ignore[assignment]
    assert conn.in_transaction is True
    mock_client.in_transaction = False
    assert conn.in_transaction is False
