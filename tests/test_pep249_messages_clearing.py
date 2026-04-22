"""Pin the PEP 249 clearing-discipline contract for `messages`.

PEP 249 §6.1.2:
    The list is cleared automatically by all standard cursor
    methods calls (prior to executing the call) to avoid excessive
    memory usage and can also be cleared by executing
    ``del cursor.messages[:]``.

Connection analogue in §6.1.1 carries the same contract for its
``cursor``, ``commit``, and ``rollback`` methods.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi import Connection, Cursor, InterfaceError
from dqlitedbapi import Warning as DbApiWarning
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


def _build_cursor() -> Cursor:
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()
    # Attach ``messages`` explicitly; ``spec=Connection`` doesn't pick
    # up the instance attribute set in ``__init__``.
    conn.messages = []
    # Swallow the coroutine argument without scheduling it — the test
    # only cares about the clearing side-effect, not the underlying
    # execute.
    conn._run_sync = MagicMock(side_effect=lambda coro: coro.close())

    cursor = Cursor.__new__(Cursor)
    cursor._connection = conn
    cursor._description = [("col", 3, None, None, None, None, None)]
    cursor._rowcount = 0
    cursor._arraysize = 1
    cursor._rows = [("value",)]
    cursor._row_index = 0
    cursor._closed = False
    cursor._lastrowid = None
    cursor.messages = []
    return cursor


def _seed(obj: Any) -> None:
    obj.messages.append((DbApiWarning, "stale-warning"))
    assert obj.messages, "seed failed"


def test_cursor_execute_clears_messages() -> None:
    cursor = _build_cursor()
    _seed(cursor)
    cursor.execute("SELECT 1")
    assert cursor.messages == []


def test_cursor_executemany_clears_messages() -> None:
    cursor = _build_cursor()
    _seed(cursor)
    cursor.executemany("INSERT INTO t VALUES (?)", [])
    assert cursor.messages == []


def test_cursor_fetchone_clears_messages() -> None:
    cursor = _build_cursor()
    _seed(cursor)
    cursor.fetchone()
    assert cursor.messages == []


def test_cursor_fetchmany_clears_messages() -> None:
    cursor = _build_cursor()
    _seed(cursor)
    cursor.fetchmany(1)
    assert cursor.messages == []


def test_cursor_fetchall_clears_messages() -> None:
    cursor = _build_cursor()
    _seed(cursor)
    cursor.fetchall()
    assert cursor.messages == []


def _build_async_cursor() -> AsyncCursor:
    conn = MagicMock(spec=AsyncConnection)
    conn._ensure_locks = MagicMock(return_value=(MagicMock(), MagicMock()))
    conn.messages = []
    cursor = AsyncCursor.__new__(AsyncCursor)
    cursor._connection = conn
    cursor._description = [("col", 3, None, None, None, None, None)]
    cursor._rowcount = 0
    cursor._arraysize = 1
    cursor._rows = [("value",)]
    cursor._row_index = 0
    cursor._closed = False
    cursor._lastrowid = None
    cursor.messages = []
    return cursor


def test_async_cursor_execute_clears_messages() -> None:
    """Pin the pre-clearing for AsyncCursor.execute.

    AsyncCursor.execute does real wire work — patch the awaits so the
    test stays unit-scoped.
    """
    cursor = _build_async_cursor()
    _seed(cursor)

    async def run() -> None:
        cursor._closed = True
        with pytest.raises(InterfaceError):
            await cursor.execute("SELECT 1")
        # Clear happens BEFORE _check_closed raises.
        assert cursor.messages == []

    asyncio.run(run())


def test_async_cursor_executemany_clears_messages() -> None:
    cursor = _build_async_cursor()
    _seed(cursor)

    async def run() -> None:
        cursor._closed = True
        with pytest.raises(InterfaceError):
            await cursor.executemany("INSERT INTO t VALUES (?)", [])
        assert cursor.messages == []

    asyncio.run(run())


def test_async_cursor_fetchone_clears_messages() -> None:
    cursor = _build_async_cursor()
    _seed(cursor)
    asyncio.run(cursor.fetchone())
    assert cursor.messages == []


def test_cursor_fetchone_clears_before_raises_on_closed() -> None:
    """Clear runs BEFORE the closed-cursor check so failed calls still bound memory."""
    cursor = _build_cursor()
    cursor._closed = True
    _seed(cursor)
    with pytest.raises(InterfaceError):
        cursor.fetchone()
    assert cursor.messages == []


def test_async_cursor_fetchall_clears_before_raises_on_closed() -> None:
    cursor = _build_async_cursor()
    cursor._closed = True
    _seed(cursor)

    async def run() -> None:
        with pytest.raises(InterfaceError):
            await cursor.fetchall()
        assert cursor.messages == []

    asyncio.run(run())


def test_async_cursor_fetchmany_clears_messages() -> None:
    cursor = _build_async_cursor()
    _seed(cursor)
    asyncio.run(cursor.fetchmany(1))
    assert cursor.messages == []


def test_async_cursor_fetchall_clears_messages() -> None:
    cursor = _build_async_cursor()
    _seed(cursor)
    asyncio.run(cursor.fetchall())
    assert cursor.messages == []


def test_connection_cursor_clears_messages() -> None:
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()
    conn._closed = False
    conn.messages = [(DbApiWarning, "stale")]
    Connection.cursor(conn)
    assert conn.messages == []


def test_connection_commit_clears_messages() -> None:
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()
    conn._closed = False
    conn._async_conn = None
    conn.messages = [(DbApiWarning, "stale")]
    Connection.commit(conn)
    assert conn.messages == []


def test_connection_rollback_clears_messages() -> None:
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()
    conn._closed = False
    conn._async_conn = None
    conn.messages = [(DbApiWarning, "stale")]
    Connection.rollback(conn)
    assert conn.messages == []


def test_async_connection_commit_clears_messages() -> None:
    conn = MagicMock(spec=AsyncConnection)
    conn._closed = False
    conn._async_conn = None
    conn.messages = [(DbApiWarning, "stale")]
    asyncio.run(AsyncConnection.commit(conn))
    assert conn.messages == []


def test_async_connection_rollback_clears_messages() -> None:
    conn = MagicMock(spec=AsyncConnection)
    conn._closed = False
    conn._async_conn = None
    conn.messages = [(DbApiWarning, "stale")]
    asyncio.run(AsyncConnection.rollback(conn))
    assert conn.messages == []


def test_async_connection_cursor_clears_messages() -> None:
    conn = MagicMock(spec=AsyncConnection)
    conn._closed = False
    conn.messages = [(DbApiWarning, "stale")]
    AsyncConnection.cursor(conn)
    assert conn.messages == []
