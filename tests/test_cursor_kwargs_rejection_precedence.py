"""``Connection.cursor(**unknown)`` runs the affinity check before unknown-kwarg
rejection, so cross-thread/loop/fork callers see the more-salient diagnostic."""

from __future__ import annotations

import os
import threading
import weakref

import pytest

import dqliteclient.connection as _client_conn_mod
from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def test_sync_cursor_factory_kwarg_from_foreign_thread_raises_thread_error_first() -> None:
    """Cross-thread misuse surfaces as ``ProgrammingError``, not ``NotSupportedError``."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_pid = _client_conn_mod.get_current_pid()
    conn._creator_thread = threading.get_ident()
    conn.messages = []

    captured: list[BaseException] = []

    def worker() -> None:
        try:
            conn.cursor(factory=object)
        except BaseException as e:  # noqa: BLE001
            captured.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=2.0)

    assert captured, "expected an exception from the foreign-thread call"
    assert isinstance(captured[0], ProgrammingError), (
        f"expected ProgrammingError, got {type(captured[0]).__name__}: {captured[0]}"
    )
    assert "thread" in str(captured[0]).lower()


def test_async_cursor_factory_kwarg_after_fork_raises_interface_error_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forked-child misuse surfaces as ``InterfaceError``, not ``NotSupportedError``."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = _client_conn_mod.get_current_pid()
    aconn._loop_ref = None
    aconn._async_conn = None
    aconn._cursors = weakref.WeakSet()
    aconn.messages = []

    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    with pytest.raises(InterfaceError, match="after fork"):
        aconn.cursor(factory=object)
