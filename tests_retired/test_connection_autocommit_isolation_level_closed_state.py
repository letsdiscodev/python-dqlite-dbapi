"""``autocommit``/``isolation_level`` getters raise ``InterfaceError`` on a closed
connection, matching stdlib sqlite3 (unlike ``in_transaction``, which stays lenient)."""

from __future__ import annotations

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import InterfaceError


def _make_closed_sync_connection() -> Connection:
    conn = Connection.__new__(Connection)
    conn._closed = True
    conn._async_conn = None
    conn.messages = []
    return conn


def _make_closed_async_connection() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = True
    conn._async_conn = None
    conn.messages = []
    return conn


def test_sync_autocommit_getter_raises_on_closed() -> None:
    conn = _make_closed_sync_connection()
    with pytest.raises(InterfaceError, match="Connection is closed"):
        _ = conn.autocommit


def test_sync_isolation_level_getter_raises_on_closed() -> None:
    conn = _make_closed_sync_connection()
    with pytest.raises(InterfaceError, match="Connection is closed"):
        _ = conn.isolation_level


async def test_async_autocommit_getter_raises_on_closed() -> None:
    conn = _make_closed_async_connection()
    with pytest.raises(InterfaceError, match="Connection is closed"):
        _ = conn.autocommit


async def test_async_isolation_level_getter_raises_on_closed() -> None:
    conn = _make_closed_async_connection()
    with pytest.raises(InterfaceError, match="Connection is closed"):
        _ = conn.isolation_level


def test_sync_autocommit_getter_succeeds_on_open() -> None:
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = None
    conn.messages = []
    assert conn.autocommit is True


def test_sync_isolation_level_getter_succeeds_on_open() -> None:
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = None
    conn.messages = []
    assert conn.isolation_level is None
