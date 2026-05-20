"""Pin: ``Connection.autocommit`` / ``Connection.isolation_level``
getters raise ``InterfaceError`` on a closed connection, matching
stdlib `sqlite3`'s ``ProgrammingError("Cannot operate on a closed
database.")`` on the equivalent getters.

Stdlib verification (CPython 3.13):

    >>> import sqlite3
    >>> con = sqlite3.connect(":memory:")
    >>> con.close()
    >>> con.autocommit
    Traceback (most recent call last):
      ...
    sqlite3.ProgrammingError: Cannot operate on a closed database.
    >>> con.isolation_level
    Traceback (most recent call last):
      ...
    sqlite3.ProgrammingError: Cannot operate on a closed database.

The previous dqlite behaviour returned the constants ``True`` /
``None`` unconditionally — silently mis-reporting "everything's fine"
to teardown probes consulting the getters during dispose.

Sibling pattern: ``in_transaction`` returns ``False`` on a closed
connection (the divergence is documented in that getter's docstring);
that's a deliberate "be lenient" choice. autocommit / isolation_level
are not lenient — stdlib raises on all three; we match for two of the
three for cross-driver portability.
"""

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
    """Negative twin: open connection still returns ``True``."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = None
    conn.messages = []
    assert conn.autocommit is True


def test_sync_isolation_level_getter_succeeds_on_open() -> None:
    """Negative twin: open connection still returns ``None``."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = None
    conn.messages = []
    assert conn.isolation_level is None
