"""Pin: cursor cascade on connection close MUST clear ``messages``
in BOTH the fork-branch and the main-branch.

Before refactoring, the cursor-cascade body was duplicated four
times in sync ``Connection`` (close-fork, close-main,
force_close_transport-fork, force_close_transport-main). The two
fork-branch copies dropped the ``del cur.messages[:]`` step that
the two main-branch copies included. A cascade-closed cursor in a
forked child therefore retained stale ``messages`` entries —
violating the post-cascade contract documented at the main-branch
sites.

The refactor extracted ``_cascade_cursors()`` as a single private
method called from all four sites; the helper always clears
``messages`` (the union of both prior shapes).
"""

import os
import threading
import weakref
from unittest.mock import MagicMock

import pytest

import dqlitedbapi
from dqliteclient import connection as _client_conn_mod
from dqlitedbapi.cursor import Cursor


def _prime_connection() -> tuple[dqlitedbapi.Connection, Cursor]:
    """Build a Connection with one tracked cursor and stale messages."""
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
    cur.messages = [(RuntimeError, "stale message")]
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
    # Pin: messages cleared.
    assert cur.messages == []


def test_force_close_transport_post_fork_clears_messages_on_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the previously-asymmetric fork-branch behaviour: a
    cascade-closed cursor in a forked child must have empty
    messages, NOT the parent's stale entries."""
    conn, cur = _prime_connection()
    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)
    conn.force_close_transport()
    assert cur._closed is True
    # The drift fix: messages cleared on fork-branch.
    assert cur.messages == []
    # And the rest of the scrub.
    assert cur._description is None
    assert cur._row_index == 0


def test_close_post_fork_clears_messages_on_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same pin for ``close()`` fork-branch."""
    conn, cur = _prime_connection()
    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)
    conn.close()
    assert cur._closed is True
    assert cur.messages == []
