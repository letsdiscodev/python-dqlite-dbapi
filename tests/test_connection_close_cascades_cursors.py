"""``Connection.close()`` cascades to outstanding cursors (tracked via WeakSet)
so later fetches raise instead of returning stale in-memory rows."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from dqlitedbapi.connection import Connection


def _make_conn() -> Connection:
    conn = Connection("localhost:19001", timeout=2.0)
    return conn


def test_connection_close_cascades_to_cursor_state() -> None:
    conn = _make_conn()
    with patch.object(Connection, "connect"):
        cur1 = conn.cursor()
        cur2 = conn.cursor()
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
    assert not any(id(c) == cur_id for c in conn._cursors)
    conn.close()


def test_async_connection_close_cascades_to_cursor_state() -> None:
    from dqlitedbapi.aio.connection import AsyncConnection
    from dqlitedbapi.aio.cursor import AsyncCursor

    async def _run() -> None:
        conn = AsyncConnection("localhost:19001")
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
