"""Pin: ``AsyncCursor.close()`` and ``Cursor.close()`` are lock-free
— they MUST NOT await the connection's ``op_lock``.

Close is a pure in-memory state-clearing primitive: it scrubs
``_rows``, ``_description``, ``_rowcount``, ``_lastrowid``,
``_row_index`` on the cursor itself. It does NOT touch the wire and
must NOT serialize against an in-flight execute on the same
connection — otherwise a context-manager exit (``with cursor() as
c:`` cleanup) parked on op_lock could block ``engine.dispose()``
while a sibling cursor is mid-statement.

The companion test ``test_execute_rechecks_closed_inside_op_lock``
pins the *other* direction: that an execute racing with close still
surfaces "Cursor is closed". This test pins close's freedom to run
without the lock.
"""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


@pytest.mark.asyncio
async def test_async_cursor_close_completes_while_op_lock_is_held() -> None:
    conn = AsyncConnection("localhost:9001")
    # Force the connection to materialise its locks.
    _, op_lock = conn._ensure_locks()
    cursor = AsyncCursor(conn)

    # Hold op_lock from a sibling task to simulate an in-flight
    # execute on a different cursor.
    lock_acquired = asyncio.Event()
    release_lock = asyncio.Event()

    async def hold_lock() -> None:
        async with op_lock:
            lock_acquired.set()
            await release_lock.wait()

    holder = asyncio.create_task(hold_lock())
    await lock_acquired.wait()
    assert op_lock.locked()

    # Pin: close() returns promptly even though op_lock is held by
    # the holder task. ``asyncio.wait_for`` enforces the freedom
    # contract — a regression that adds an ``async with op_lock`` to
    # close would block here until the holder releases (and our
    # release event isn't set yet).
    await asyncio.wait_for(cursor.close(), timeout=0.5)
    assert cursor._closed is True

    # Cleanup: release the holder.
    release_lock.set()
    await holder


def test_sync_cursor_close_does_not_touch_op_lock() -> None:
    """Sync parity check via attribute inspection: ``Cursor.close``
    is a pure scrub. The sync path runs through ``_run_sync`` for
    awaited operations; close intentionally does not, so a closed
    cursor's teardown does not contend with the threading.Lock."""
    from dqlitedbapi.connection import Connection

    conn = Connection("localhost:9001")
    try:
        # Acquire the op_lock from outside; close() must not block.
        assert conn._op_lock.acquire(timeout=0)
        try:
            cur = conn.cursor()
            cur.close()  # Must not deadlock on the held lock.
            assert cur._closed is True
        finally:
            conn._op_lock.release()
    finally:
        conn._closed = True
