"""Fork-guard cluster pins for dbapi sync + aio surfaces (ensure-connection
fast path, stub family, close / force_close_transport state nulling)."""

from __future__ import annotations

import asyncio
import os
import weakref

import pytest

import dqliteclient.connection as _client_conn_mod
from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def _spawn_async_conn() -> AsyncConnection:
    """Build a hand-seeded AsyncConnection without real wire I/O."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = _client_conn_mod.get_current_pid()
    aconn._loop_ref = None
    aconn._connect_lock = None
    aconn._op_lock = None
    aconn._cursors = weakref.WeakSet()
    aconn.messages = []
    aconn._async_conn = None
    aconn._closed_flag = [False]
    aconn._connected_flag = [False]
    return aconn


class _FakeInnerConn:
    _in_use = False
    _closed = False

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_ensure_connection_fast_path_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fast-path return must raise ``after fork`` rather than hand back
    the parent's inner conn (which would only fail a frame later)."""
    aconn = _spawn_async_conn()
    aconn._async_conn = _FakeInnerConn()  # type: ignore[assignment]
    aconn._connected_flag[0] = True

    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        await aconn._ensure_connection()


@pytest.mark.asyncio
async def test_async_stub_executescript_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The stub family must surface ``after fork`` rather than NotSupportedError."""
    aconn = _spawn_async_conn()
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        aconn.executescript("SELECT 1;")


@pytest.mark.asyncio
async def test_async_stub_set_authorizer_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    aconn = _spawn_async_conn()
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        aconn.set_authorizer(lambda *a: 0)


@pytest.mark.asyncio
async def test_async_stub_total_changes_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    aconn = _spawn_async_conn()
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        aconn.total_changes()


def _spawn_sync_conn() -> Connection:
    """Build a hand-seeded sync Connection without real wire I/O."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_pid = _client_conn_mod.get_current_pid()
    conn._async_conn = None
    conn.messages = []
    conn._closed_flag = [False]
    return conn


def test_sync_stub_executescript_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _spawn_sync_conn()
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        conn.executescript("SELECT 1;")


def test_sync_stub_set_authorizer_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _spawn_sync_conn()
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        conn.set_authorizer(lambda *a: 0)


def test_sync_stub_total_changes_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _spawn_sync_conn()
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        conn.total_changes()


def test_sync_close_fork_branch_nulls_parent_loop_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``close()`` in a forked child must null the loop/thread state: the
    daemon-loop OS thread does not survive fork(2), so without this the
    Thread sits in ``threading._active`` forever with no exit event."""
    import threading

    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_pid = _client_conn_mod.get_current_pid()
    conn._async_conn = object()  # type: ignore[assignment]
    conn._loop = asyncio.new_event_loop()
    conn._thread = threading.Thread(target=lambda: None, daemon=True)
    conn._connect_lock = object()  # type: ignore[assignment]
    conn._transaction_owner = 12345
    conn._closed_flag = [False]
    conn._finalizer = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []

    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    try:
        conn.close()
    finally:
        pass

    assert conn._async_conn is None
    assert conn._loop is None
    assert conn._thread is None
    assert conn._connect_lock is None
    assert conn._transaction_owner is None


def test_sync_force_close_transport_fork_branch_nulls_parent_loop_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror pin for ``force_close_transport``'s fork branch."""
    import threading

    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_pid = _client_conn_mod.get_current_pid()
    conn._async_conn = object()  # type: ignore[assignment]
    conn._loop = asyncio.new_event_loop()
    conn._thread = threading.Thread(target=lambda: None, daemon=True)
    conn._connect_lock = object()  # type: ignore[assignment]
    conn._transaction_owner = 999
    conn._closed_flag = [False]
    conn._finalizer = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []

    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    conn.force_close_transport()

    assert conn._async_conn is None
    assert conn._loop is None
    assert conn._thread is None
    assert conn._connect_lock is None
    assert conn._transaction_owner is None
