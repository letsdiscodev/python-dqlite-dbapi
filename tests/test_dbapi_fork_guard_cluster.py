"""Fork-guard cluster pins for dbapi sync + aio surfaces.

Covers four mirror-of-sibling defects:

1. ``AsyncConnection._ensure_connection`` fast-path return — when
   ``_async_conn`` is already set, the path skipped the pid check
   so a forked child got the parent's inner conn silently.
2. ``AsyncConnection._stub_unsupported`` — the stdlib-``sqlite3``-
   parity stub family checked only ``_closed`` so a forked child
   calling ``executescript`` / ``set_authorizer`` / ``total_changes``
   etc. saw ``NotSupportedError`` instead of the canonical
   ``InterfaceError("after fork")``.
3. Sync ``Connection._stub_unsupported`` — symmetric to (2).
4. Sync ``Connection.close()`` fork branch — failed to null
   ``_async_conn`` / ``_loop`` / ``_thread`` / ``_connect_lock``
   / ``_transaction_owner`` so the inherited daemon-loop Thread
   stayed pinned in ``threading._active`` forever in the child.
   Mirror at ``force_close_transport`` fork branch.

Each pin uses the project's established monkeypatch-based fork
simulation (``monkeypatch.setattr(dqliteclient.connection,
"_current_pid", os.getpid() + 1)``) rather than a real ``os.fork`` —
the production code reads ``_current_pid`` via ``get_current_pid()``
and this is the same shape used by
``test_async_connection_cursor_fork_guard.py`` and
``test_connection_after_fork_raises.py``.
"""

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
    aconn._creator_pid = _client_conn_mod._current_pid
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
    """``AsyncConnection._ensure_connection`` must raise
    ``InterfaceError("after fork")`` on its fast-path return when a
    fork has crossed the boundary — otherwise the forked child would
    silently receive the parent's already-built inner
    ``DqliteConnection`` and only fail one frame later at
    ``cursor()`` / ``execute()``.
    """
    aconn = _spawn_async_conn()
    # Simulate "parent already connected".
    aconn._async_conn = _FakeInnerConn()  # type: ignore[assignment]
    aconn._connected_flag[0] = True

    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        await aconn._ensure_connection()


@pytest.mark.asyncio
async def test_async_stub_executescript_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The stub family routes through ``_stub_unsupported``; the
    helper must surface the canonical ``InterfaceError`` on fork
    rather than ``NotSupportedError``."""
    aconn = _spawn_async_conn()
    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        aconn.executescript("SELECT 1;")


@pytest.mark.asyncio
async def test_async_stub_set_authorizer_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    aconn = _spawn_async_conn()
    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        aconn.set_authorizer(lambda *a: 0)


@pytest.mark.asyncio
async def test_async_stub_total_changes_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    aconn = _spawn_async_conn()
    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        aconn.total_changes()


def _spawn_sync_conn() -> Connection:
    """Build a hand-seeded sync Connection without real wire I/O."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_pid = _client_conn_mod._current_pid
    conn._async_conn = None
    conn.messages = []
    conn._closed_flag = [False]
    return conn


def test_sync_stub_executescript_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _spawn_sync_conn()
    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        conn.executescript("SELECT 1;")


def test_sync_stub_set_authorizer_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _spawn_sync_conn()
    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        conn.set_authorizer(lambda *a: 0)


def test_sync_stub_total_changes_raises_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _spawn_sync_conn()
    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        conn.total_changes()


def test_sync_close_fork_branch_nulls_parent_loop_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``Connection.close()`` in a forked child must null
    ``_async_conn``, ``_loop``, ``_thread``, ``_connect_lock``,
    ``_transaction_owner`` so the child's GC can reap the daemon-
    loop Thread + asyncio loop chain instead of pinning them via
    ``threading._active`` forever (the daemon-loop OS thread does
    NOT survive POSIX ``fork(2)`` — only the calling thread crosses
    — so the Thread sits in ``_active`` indefinitely with no exit
    event to drive its removal). Mirror of the client-layer fix.
    """
    import threading

    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_pid = _client_conn_mod._current_pid
    conn._async_conn = object()  # type: ignore[assignment]
    conn._loop = asyncio.new_event_loop()
    conn._thread = threading.Thread(target=lambda: None, daemon=True)
    conn._connect_lock = object()  # type: ignore[assignment]
    conn._transaction_owner = 12345
    conn._closed_flag = [False]
    conn._finalizer = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []

    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    try:
        conn.close()
    finally:
        # Defensive cleanup of the sentinel loop we set up above.
        # ``close()`` should have nulled ``conn._loop`` (we still hold
        # a local reference for the assertion + close).
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
    conn._creator_pid = _client_conn_mod._current_pid
    conn._async_conn = object()  # type: ignore[assignment]
    conn._loop = asyncio.new_event_loop()
    conn._thread = threading.Thread(target=lambda: None, daemon=True)
    conn._connect_lock = object()  # type: ignore[assignment]
    conn._transaction_owner = 999
    conn._closed_flag = [False]
    conn._finalizer = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []

    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    conn.force_close_transport()

    assert conn._async_conn is None
    assert conn._loop is None
    assert conn._thread is None
    assert conn._connect_lock is None
    assert conn._transaction_owner is None
