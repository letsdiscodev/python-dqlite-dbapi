"""``_ensure_locks`` raises on an already-closed connection instead of lazily recreating
primitives, which would leak across close lifetimes when a task races ``close()``."""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


async def test_ensure_locks_on_closed_connection_raises() -> None:
    conn = AsyncConnection("localhost:19001")
    # Post-close state: closed and lock refs nulled the way close() does.
    conn._closed = True
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None

    with pytest.raises(InterfaceError, match="Connection is closed"):
        conn._ensure_locks()

    assert conn._connect_lock is None
    assert conn._op_lock is None
    assert conn._loop_ref is None


async def test_ensure_locks_on_open_connection_creates_primitives() -> None:
    conn = AsyncConnection("localhost:19001")
    connect_lock, op_lock = conn._ensure_locks()
    assert isinstance(connect_lock, asyncio.Lock)
    assert isinstance(op_lock, asyncio.Lock)
