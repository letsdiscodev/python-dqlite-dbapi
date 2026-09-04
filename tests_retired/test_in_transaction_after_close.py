"""Pin ``in_transaction`` snapshot reads: a concurrent close() racing the
property read must not raise AttributeError."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection


def test_sync_in_transaction_after_close_returns_false() -> None:
    conn = Connection("localhost:9001")
    fake = MagicMock()
    fake.execute = AsyncMock()
    fake.close = AsyncMock()
    fake._in_use = False
    fake._bound_loop = None
    conn._async_conn = fake
    conn.close()
    assert conn.in_transaction is False


def _prime_async_with_locks(addr: str = "localhost:9001") -> AsyncConnection:
    """AsyncConnection in the post-_ensure_locks state without a handshake;
    close() asserts _op_lock is not None."""
    conn = AsyncConnection(addr, database="x")
    conn._connect_lock = asyncio.Lock()
    conn._op_lock = asyncio.Lock()
    return conn


class TestAsyncInTransactionAfterClose:
    async def test_returns_false_after_close(self) -> None:
        conn = _prime_async_with_locks()
        fake = MagicMock()
        fake.execute = AsyncMock()
        fake.close = AsyncMock()
        fake.in_transaction = True  # would raise if not snapshot
        conn._async_conn = fake
        await conn.close()
        assert conn.in_transaction is False

    async def test_concurrent_close_does_not_raise(self) -> None:
        """Concurrent close() racing the property read returns False, no AttributeError."""
        for _ in range(20):
            conn = _prime_async_with_locks()
            fake = MagicMock()
            fake.execute = AsyncMock()
            fake.close = AsyncMock()
            fake.in_transaction = False
            conn._async_conn = fake

            async def reader(c: AsyncConnection = conn) -> bool:
                return c.in_transaction

            async def closer(c: AsyncConnection = conn) -> None:
                await c.close()

            results = await asyncio.gather(reader(), closer(), return_exceptions=True)
            for r in results:
                assert not isinstance(r, BaseException), r


@pytest.mark.parametrize("flag_value", [True, False])
def test_sync_in_transaction_reflects_underlying_attribute(flag_value: bool) -> None:
    conn = Connection("localhost:9001")
    try:
        fake = MagicMock()
        fake.execute = AsyncMock()
        fake.close = AsyncMock()
        fake._in_use = False
        fake._bound_loop = None
        fake.in_transaction = flag_value
        conn._async_conn = fake
        assert conn.in_transaction is flag_value
    finally:
        conn._closed = True


class TestAsyncInTransactionReflectsUnderlying:
    @pytest.mark.parametrize("flag_value", [True, False])
    async def test_async_in_transaction_reflects_underlying_attribute(
        self, flag_value: bool
    ) -> None:
        conn = AsyncConnection("localhost:9001", database="x")
        fake = MagicMock()
        fake.execute = AsyncMock()
        fake.close = AsyncMock()
        fake.in_transaction = flag_value
        conn._async_conn = fake
        assert conn.in_transaction is flag_value
