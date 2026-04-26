"""``Connection.close()`` cascades to outstanding cursors.

stdlib ``sqlite3.Connection.close()`` marks every cursor spawned from
the connection as closed so subsequent fetches on those cursors raise
rather than silently answering from stale in-memory rows. Track
outstanding cursors via ``weakref.WeakSet`` and cascade on close.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from dqlitedbapi.connection import Connection


def _make_conn() -> Connection:
    conn = Connection("localhost:19001", timeout=2.0)
    return conn


def test_connection_close_cascades_to_cursor_state() -> None:
    conn = _make_conn()
    # Keep construction pure unit — no real cluster dial.
    with patch.object(Connection, "connect"):
        cur1 = conn.cursor()
        cur2 = conn.cursor()
    # Pre-set some state so we can verify the scrub.
    cur1._rows = [(1,)]
    cur1._description = [("v", 1, None, None, None, None, None)]  # type: ignore[assignment]
    cur1._rowcount = 1
    cur1._lastrowid = 42
    conn.close()
    for cur in (cur1, cur2):
        assert cur._closed is True
        assert cur._rows == []
        assert cur._description is None
        assert cur._rowcount == -1
        assert cur._lastrowid is None


def test_gc_of_cursor_does_not_keep_connection_alive() -> None:
    import gc

    conn = _make_conn()
    with patch.object(Connection, "connect"):
        cur = conn.cursor()
    cur_id = id(cur)
    del cur
    gc.collect()
    # WeakSet should have dropped the reference.
    assert not any(id(c) == cur_id for c in conn._cursors)
    conn.close()


def test_async_connection_close_cascades_to_cursor_state() -> None:
    from dqlitedbapi.aio.connection import AsyncConnection
    from dqlitedbapi.aio.cursor import AsyncCursor

    async def _run() -> None:
        conn = AsyncConnection("localhost:19001")
        # Don't actually connect — keep this a pure state-machine test.
        cur1 = conn.cursor()
        cur2 = conn.cursor()
        cur1._rows = [(1,)]
        cur1._description = [("v", 1, None, None, None, None, None)]  # type: ignore[assignment]
        cur1._rowcount = 1
        cur1._lastrowid = 42
        await conn.close()
        for cur in (cur1, cur2):
            assert isinstance(cur, AsyncCursor)
            assert cur._closed is True
            assert cur._rows == []
            assert cur._description is None

    asyncio.run(_run())
