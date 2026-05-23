"""Pin: ``Cursor.arraysize.setter`` and ``Cursor.row_factory.setter``
clear ``self.messages[:]`` on entry per PEP 249 §6.4 — mirroring the
already-shipped connection-side discipline (``autocommit.setter``,
``isolation_level.setter``, ``text_factory.setter``,
``Connection.row_factory.setter``).

Sync + async parity. Without these clears, a state-mutating cursor
operation would carry over stale messages from a prior failed
operation, violating PEP 249's "list is cleared automatically by all
standard connection methods calls (prior to executing the call)".
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
