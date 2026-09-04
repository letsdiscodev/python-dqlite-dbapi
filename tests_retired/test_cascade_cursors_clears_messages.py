"""Cursor cascade on connection close clears ``messages`` in both the
fork-branch and main-branch (fork-branch previously dropped the clear)."""

import os
import threading
import weakref
from unittest.mock import MagicMock

import pytest

import dqlitedbapi
from dqlitedbapi.cursor import Cursor


def _prime_connection() -> tuple[dqlitedbapi.Connection, Cursor]:
    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._creator_pid = os.getpid()
    conn._loop_lock = threading.Lock()
    conn._loop = None
    conn._thread = None
    conn._async_conn = None
    conn._cursors = weakref.WeakSet()
    conn._finalizer = MagicMock()
    conn._close_timeout = 0.5
    conn.messages = []

    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = [(1,), (2,)]
    cur._description = (("c", None, None, None, None, None, None),)
    cur._rowcount = 2
    cur._lastrowid = 7
    cur._row_index = 1
    cur._connection = conn
    cur.messages = [(RuntimeError, RuntimeError("stale message"))]
    conn._cursors.add(cur)

    return conn, cur


def test_cascade_clears_messages_on_main_branch() -> None:
    conn, cur = _prime_connection()
    conn._cascade_cursors()
    assert cur._closed is True
    assert cur._rows == []
    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._lastrowid is None
    assert cur._row_index == 0
    assert cur.messages == []


def test_force_close_transport_post_fork_clears_messages_on_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fork-branch cascade-closed cursor has empty messages, not the
    parent's stale entries."""
    conn, cur = _prime_connection()
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)
    conn.force_close_transport()
    assert cur._closed is True
    assert cur.messages == []
    assert cur._description is None
    assert cur._row_index == 0


def test_close_post_fork_clears_messages_on_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same pin for ``close()`` fork-branch."""
    conn, cur = _prime_connection()
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)
    conn.close()
    assert cur._closed is True
    assert cur.messages == []
