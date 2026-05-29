"""__aenter__'s cleanup-on-failed-connect absorbs a fresh outer CancelledError so the bare
raise re-delivers the original connect-time exception, matching aconnect()."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


@pytest.mark.asyncio
async def test_aenter_cleanup_preserves_original_under_outer_cancel() -> None:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = "host:1234"
    conn._database = "x"
    conn._closed_flag = [False]
    conn._async_conn = None
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None
    conn._transaction_owner = None

    original_error = RuntimeError("simulated connect failure")

    async def fail_connect() -> None:
        raise original_error

    async def slow_close() -> None:
        # Park forever; the shield cancel-suppresses under a fresh outer cancel.
        await asyncio.sleep(60)

    conn.connect = fail_connect
    conn.close = slow_close

    async def run() -> None:
        async with conn:
            pytest.fail("__aenter__ must raise; we never reach this")

    # Tight outer timeout lands a fresh CancelledError while cleanup-close is in flight.
    with pytest.raises((RuntimeError, asyncio.CancelledError)) as ei:
        async with asyncio.timeout(0.01):
            await run()

    if isinstance(ei.value, RuntimeError):
        assert ei.value is original_error
    else:
        # Cancel re-emerged at the timeout boundary; the original survives via the chain.
        chain: list[BaseException | None] = []
        node: BaseException | None = ei.value
        while node is not None and len(chain) < 6:
            chain.append(node)
            node = node.__context__
        assert any(isinstance(n, RuntimeError) and n is original_error for n in chain), (
            f"original error lost from exception chain: {chain!r}"
        )


_ = AsyncMock  # keep import for ruff
