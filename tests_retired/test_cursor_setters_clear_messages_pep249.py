"""``Cursor.arraysize`` and ``Cursor.row_factory`` setters clear ``self.messages`` on entry
per PEP 249 §6.4 (sync + async), mirroring the connection-side setter discipline.
"""

from __future__ import annotations

import asyncio

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_sync_cursor_arraysize_setter_clears_messages() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    cur.messages.append((Exception, Exception("sentinel")))
    cur.arraysize = 5
    assert cur.messages == []


def test_sync_cursor_row_factory_setter_clears_messages() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    cur.messages.append((Exception, Exception("sentinel")))
    cur.row_factory = lambda c, r: r
    assert cur.messages == []


def test_async_cursor_arraysize_setter_clears_messages() -> None:
    async def _run() -> None:
        conn = AsyncConnection("localhost:9001")
        cur = conn.cursor()
        cur.messages.append((Exception, Exception("sentinel")))
        cur.arraysize = 5
        assert cur.messages == []

    asyncio.run(_run())


def test_async_cursor_row_factory_setter_clears_messages() -> None:
    async def _run() -> None:
        conn = AsyncConnection("localhost:9001")
        cur = conn.cursor()
        cur.messages.append((Exception, Exception("sentinel")))
        cur.row_factory = lambda c, r: r
        assert cur.messages == []

    asyncio.run(_run())
