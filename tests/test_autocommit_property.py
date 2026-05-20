"""Pin the bare-dbapi ``autocommit`` property exposed on Connection
and AsyncConnection.

Stdlib ``sqlite3`` added ``Connection.autocommit`` in Python 3.12;
``psycopg`` exposes it as well. dqlite is genuinely autocommit-by-
default at the wire level (every statement commits unless the
caller issued an explicit BEGIN), so the bare dbapi reports
``True``. The SA adapter (sqlalchemy-dqlite) deliberately reports
``False`` because SA wraps the connection with explicit
BEGIN/COMMIT — both are accurate for their respective layer.

The setter accepts ``True`` as a no-op (mirroring the existing
mode) and rejects ``False`` with ``NotSupportedError`` because
the autocommit mode is fixed by the dqlite server.
"""

from __future__ import annotations

import os
import threading

import pytest

from dqlitedbapi import NotSupportedError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def _bare_sync_conn() -> Connection:
    """Construct a Connection without dialing — used by setter unit
    tests that don't need transport. Sets the threadsafety affinity
    fields so the setters' ``_check_thread()`` guard passes. Also
    sets ``_closed=False`` so the ``autocommit`` / ``isolation_level``
    getters' closed-state guard passes."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    return conn


def _bare_async_conn() -> AsyncConnection:
    """Construct an AsyncConnection without dialing. ``_check_loop_binding``
    requires ``_closed``, ``_creator_pid``, and ``_loop_ref``; the
    loop-bound check is a no-op when ``_loop_ref`` is None (binding not
    yet established)."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._creator_pid = os.getpid()
    conn._loop_ref = None
    return conn


class TestSyncAutocommitProperty:
    def test_autocommit_returns_true(self) -> None:
        conn = _bare_sync_conn()
        assert conn.autocommit is True

    def test_setting_true_is_noop(self) -> None:
        conn = _bare_sync_conn()
        conn.autocommit = True
        assert conn.autocommit is True

    def test_setting_false_raises_not_supported(self) -> None:
        conn = _bare_sync_conn()
        with pytest.raises(NotSupportedError, match="autocommit-by-default"):
            conn.autocommit = False


class TestAsyncAutocommitProperty:
    def test_autocommit_returns_true(self) -> None:
        conn = _bare_async_conn()
        assert conn.autocommit is True

    def test_setting_true_is_noop(self) -> None:
        conn = _bare_async_conn()
        conn.autocommit = True
        assert conn.autocommit is True

    def test_setting_false_raises_not_supported(self) -> None:
        conn = _bare_async_conn()
        with pytest.raises(NotSupportedError, match="autocommit-by-default"):
            conn.autocommit = False
