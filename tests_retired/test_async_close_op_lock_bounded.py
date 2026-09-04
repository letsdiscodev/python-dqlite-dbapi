"""``close()`` bounds its ``op_lock`` acquire by ``self._timeout``; without it a sibling
parked on a slow read holds the lock, hanging an N-slot pool's shutdown for N * timeout."""

import asyncio
import os
import time
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def _prime() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._async_conn = None
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []
    conn._timeout = 0.2
    conn._close_timeout = 0.5
    conn._creator_pid = os.getpid()
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    return conn


async def test_close_raises_interface_error_when_op_lock_held_past_timeout() -> None:
    """Sibling holds op_lock indefinitely -> close() raises InterfaceError after
    self._timeout, not a longer wall-clock."""
    conn = _prime()
    conn._ensure_locks()

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    inner.close = AsyncMock()
    conn._async_conn = inner

    sibling_started = asyncio.Event()
    release = asyncio.Event()

    async def hold_lock() -> None:
        async with conn._op_lock:  # type: ignore[union-attr]
            sibling_started.set()
            await release.wait()

    sibling = asyncio.create_task(hold_lock())
    await sibling_started.wait()

    started = time.monotonic()
    with pytest.raises(InterfaceError, match="close timed out"):
        await conn.close()
    elapsed = time.monotonic() - started

    assert elapsed < 1.0, f"close took {elapsed}s — bound was 0.2s"

    release.set()
    await sibling


async def test_close_force_closes_transport_on_op_lock_timeout() -> None:
    """When the bound trips, close() force-closes the transport so the writer is reaped
    rather than left as a half-open socket."""
    conn = _prime()
    conn._ensure_locks()

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    inner.close = AsyncMock()
    conn._async_conn = inner

    sibling_started = asyncio.Event()
    release = asyncio.Event()

    async def hold_lock() -> None:
        async with conn._op_lock:  # type: ignore[union-attr]
            sibling_started.set()
            await release.wait()

    sibling = asyncio.create_task(hold_lock())
    await sibling_started.wait()

    with pytest.raises(InterfaceError):
        await conn.close()

    writer.close.assert_called()
    assert conn._async_conn is None
    assert conn._closed is True

    release.set()
    await sibling


async def test_close_does_not_fire_bound_when_sibling_releases_quickly() -> None:
    """A sibling holding the lock for less than self._timeout must not trip the bound."""
    conn = _prime()
    conn._ensure_locks()

    inner = MagicMock()
    inner.close = AsyncMock()
    conn._async_conn = inner

    sibling_started = asyncio.Event()

    async def quick_sibling() -> None:
        async with conn._op_lock:  # type: ignore[union-attr]
            sibling_started.set()
            await asyncio.sleep(0.05)  # < self._timeout (0.2)

    sibling = asyncio.create_task(quick_sibling())
    await sibling_started.wait()

    await conn.close()
    await sibling

    inner.close.assert_awaited()
    assert conn._async_conn is None
