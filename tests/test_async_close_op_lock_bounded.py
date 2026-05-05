"""``AsyncConnection.close()`` bounds its ``op_lock`` acquire by
``self._timeout``.

Without the bound, a sibling task parked on a slow ``reader.read()``
holds ``op_lock`` for the whole per-RPC ``timeout`` window. Under
SIGTERM / ``engine.dispose()`` an N-slot SA pool with stuck siblings
hangs shutdown for up to ``N * timeout`` seconds.

The sync sibling at ``connection.py:756`` already uses a bounded
acquire with ``InterfaceError`` on miss; this pin enforces the same
contract on the async surface.
"""

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


@pytest.mark.asyncio
async def test_close_raises_interface_error_when_op_lock_held_past_timeout() -> None:
    """Sibling holds op_lock indefinitely → close() raises
    InterfaceError after self._timeout, not after a longer wall-clock."""
    conn = _prime()
    conn._ensure_locks()

    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    inner.close = AsyncMock()
    conn._async_conn = inner

    # Sibling task holds the lock and parks.
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

    # The wall-clock must be bounded by self._timeout (with slack).
    assert elapsed < 1.0, f"close took {elapsed}s — bound was 0.2s"

    # Cleanup the sibling.
    release.set()
    await sibling


@pytest.mark.asyncio
async def test_close_force_closes_transport_on_op_lock_timeout() -> None:
    """When the bound trips, close() must force-close the transport
    so SIGTERM/dispose actually reap the writer rather than leaving
    a half-open socket."""
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

    # The synchronous force-close path was taken: writer.close() was
    # invoked, ``_async_conn`` was nulled.
    writer.close.assert_called()
    assert conn._async_conn is None
    assert conn._closed is True

    release.set()
    await sibling


@pytest.mark.asyncio
async def test_close_does_not_fire_bound_when_sibling_releases_quickly() -> None:
    """A well-behaved sibling that holds the lock for less than
    self._timeout must not trip the bound — close() proceeds
    normally with no InterfaceError."""
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

    # Should complete without InterfaceError.
    await conn.close()
    await sibling

    inner.close.assert_awaited()
    assert conn._async_conn is None
