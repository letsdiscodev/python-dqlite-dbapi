"""Pin the bare-dbapi ``autocommit`` property: reports True (dqlite is wire-level
autocommit), setter accepts True as no-op and rejects False (mode is server-fixed)."""

from __future__ import annotations

import os
import threading

import pytest

from dqlitedbapi import NotSupportedError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def _bare_sync_conn() -> Connection:
    """Connection without dialing; primes thread-affinity and _closed so the
    setter/getter guards pass."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    return conn


def _bare_async_conn() -> AsyncConnection:
    """AsyncConnection without dialing; _loop_ref=None makes _check_loop_binding a no-op."""
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
