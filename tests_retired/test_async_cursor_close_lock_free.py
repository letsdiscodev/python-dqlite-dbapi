"""``AsyncCursor.close()`` / ``Cursor.close()`` are lock-free and MUST NOT await ``op_lock``:
serializing close against an in-flight execute could let a context-manager exit block disposal."""

from __future__ import annotations

import asyncio

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


async def test_async_cursor_close_completes_while_op_lock_is_held() -> None:
    conn = AsyncConnection("localhost:9001")
    _, op_lock = conn._ensure_locks()
    cursor = AsyncCursor(conn)

    # Hold op_lock from a sibling task to simulate an in-flight execute on a different cursor.
    lock_acquired = asyncio.Event()
    release_lock = asyncio.Event()

    async def hold_lock() -> None:
        async with op_lock:
            lock_acquired.set()
            await release_lock.wait()

    holder = asyncio.create_task(hold_lock())
    await lock_acquired.wait()
    assert op_lock.locked()

    # close() never touches op_lock; converting it back to ``async with op_lock`` would block here.
    cursor.close()
    assert cursor._closed is True

    release_lock.set()
    await holder


def test_sync_cursor_close_does_not_touch_op_lock() -> None:
    """Sync parity: ``Cursor.close`` is a pure scrub; it never contends with the threading.Lock."""
    from dqlitedbapi.connection import Connection

    conn = Connection("localhost:9001")
    try:
        assert conn._op_lock.acquire(timeout=0)
        try:
            cur = conn.cursor()
            cur.close()  # must not deadlock on the held lock
            assert cur._closed is True
        finally:
            conn._op_lock.release()
    finally:
        conn._closed = True
