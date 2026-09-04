"""Pin: ``AsyncConnection.cursor()`` enforces the fork-after-init guard."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def _make_async_connection_with_creator_pid(creator_pid: int) -> AsyncConnection:
    import weakref

    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = creator_pid
    aconn._loop_ref = None
    aconn._async_conn = None
    aconn._cursors = weakref.WeakSet()
    aconn.messages = []
    return aconn


def test_cursor_raises_when_current_pid_diverged_from_creator_pid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A diverged pid (simulated fork) must surface InterfaceError, not a live cursor."""

    aconn = _make_async_connection_with_creator_pid(creator_pid=99999)
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: 12345)

    with pytest.raises(InterfaceError, match="used after fork"):
        aconn.cursor()


def test_cursor_does_not_register_in_cursors_set_when_pid_diverged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cursors WeakSet must stay empty: the guard fires before any registration."""

    aconn = _make_async_connection_with_creator_pid(creator_pid=99999)
    aconn._cursors = MagicMock()
    aconn._cursors.add = MagicMock(side_effect=AssertionError("must not register"))
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: 12345)

    with pytest.raises(InterfaceError, match="used after fork"):
        aconn.cursor()
    aconn._cursors.add.assert_not_called()


def test_cursor_works_when_pid_matches_creator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Positive control: same-process call still returns a live cursor."""
    from dqliteclient import connection as _client_conn_mod

    aconn = _make_async_connection_with_creator_pid(creator_pid=_client_conn_mod.get_current_pid())
    cur = aconn.cursor()
    assert cur is not None
    assert cur in aconn._cursors
