"""``AsyncConnection._ensure_locks`` raises if the connection is
already closed instead of lazily recreating fresh primitives.

A concurrent ``close()`` nulls ``_connect_lock`` / ``_op_lock`` /
``_loop_ref``. If a task racing the close reaches ``_ensure_locks``
post-null, the lazy-create branch used to allocate three fresh
primitives bound to the current loop. ``_ensure_connection`` then
raised ``InterfaceError`` anyway, but the fresh primitives survived
the raise and a second close early-returned without re-nulling them,
leaking across close lifetimes.
"""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.asyncio
async def test_ensure_locks_on_closed_connection_raises() -> None:
    conn = AsyncConnection("localhost:19001")
    # Simulate a post-close state: mark closed and null the lock refs
    # the way close() does.
    conn._closed = True
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None

    with pytest.raises(InterfaceError, match="Connection is closed"):
        conn._ensure_locks()

    # No fresh primitives were created as a side effect.
    assert conn._connect_lock is None
    assert conn._op_lock is None
    assert conn._loop_ref is None


@pytest.mark.asyncio
async def test_ensure_locks_on_open_connection_creates_primitives() -> None:
    conn = AsyncConnection("localhost:19001")
    connect_lock, op_lock = conn._ensure_locks()
    assert isinstance(connect_lock, asyncio.Lock)
    assert isinstance(op_lock, asyncio.Lock)
