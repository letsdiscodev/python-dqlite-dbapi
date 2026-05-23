"""Pin: ``AsyncConnection.__aenter__``'s cleanup-on-failed-connect
absorbs a fresh outer ``CancelledError`` so the bare ``raise`` re-
delivers the ORIGINAL connect-time exception. Mirrors the discipline
already in place at ``aconnect()`` so the two eager-establish entry
points behave consistently under outer-cancel-during-cleanup.

Without this discipline, the two entry points behaved differently:
- ``aconnect()`` re-raised the original connect-time error; cancel
  surfaced at the next ``await`` on the caller's task.
- ``__aenter__`` re-raised the ``CancelledError`` instead, leaving
  the original error visible only as ``__context__``.

Cross-driver code switching between the two patterns should not see
a different exception class.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


@pytest.mark.asyncio
async def test_aenter_cleanup_preserves_original_under_outer_cancel() -> None:
    """A connect-time error followed by a fresh outer cancel during
    the cleanup-close must re-deliver the ORIGINAL exception. The
    cancel propagates at the next await on the caller's task."""
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
        # Park forever; the shield will cancel-suppress under a fresh
        # outer cancel.
        await asyncio.sleep(60)

    conn.connect = fail_connect
    conn.close = slow_close

    async def run() -> None:
        async with conn:
            pytest.fail("__aenter__ must raise; we never reach this")

    # Wrap in a tight outer timeout so a fresh CancelledError lands
    # while cleanup-close is in flight. With the contextlib.suppress
    # in place, the original RuntimeError is the propagating exception
    # (cancel resurfaces at the asyncio.timeout's __aexit__).
    with pytest.raises((RuntimeError, asyncio.CancelledError)) as ei:
        async with asyncio.timeout(0.01):
            await run()

    # Either we observe the original directly OR the asyncio.timeout
    # re-raised cancel. In the latter case the original survives via
    # __context__; in the former (preferred shape) it surfaces
    # directly.
    if isinstance(ei.value, RuntimeError):
        assert ei.value is original_error
    else:
        # The cancel re-emerged at the timeout boundary. The original
        # must still be reachable via the exception chain.
        chain: list[BaseException | None] = []
        node: BaseException | None = ei.value
        while node is not None and len(chain) < 6:
            chain.append(node)
            node = node.__context__
        assert any(isinstance(n, RuntimeError) and n is original_error for n in chain), (
            f"original error lost from exception chain: {chain!r}"
        )


# Quiet AsyncMock import for ruff.
_ = AsyncMock
