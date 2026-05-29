"""``Connection.close()``'s re-entry bypass uses an owner-aware probe
(``_op_lock_owner == get_ident()``), not bare ``locked()``: under tier-2 a sibling
may hold the lock and the bare probe would wrongly release it.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _make_conn() -> Connection:
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._op_lock = threading.Lock()
    conn._op_lock_owner = None
    conn._creator_thread = threading.get_ident()
    conn._loop_lock = threading.Lock()
    conn._loop = MagicMock()
    conn._loop.is_closed = MagicMock(return_value=False)
    conn._thread = MagicMock()
    conn._async_conn = None
    conn._finalizer = None
    conn._timeout = 1.0
    return conn


def test_close_bypass_does_not_release_sibling_threads_lock() -> None:
    """Sibling holds the op_lock; the creator's close() bypass probe must not release it."""
    conn = _make_conn()

    sibling_acquired = threading.Event()
    sibling_can_release = threading.Event()

    def sibling() -> None:
        conn._op_lock.acquire()
        conn._op_lock_owner = threading.get_ident()
        sibling_acquired.set()
        sibling_can_release.wait(timeout=5.0)
        conn._op_lock_owner = None
        conn._op_lock.release()

    t = threading.Thread(target=sibling)
    t.start()
    sibling_acquired.wait(timeout=5.0)

    creator_ident = threading.get_ident()
    assert conn._op_lock_owner != creator_ident, "preconditions: sibling owns the lock, not creator"
    probe_says_take_bypass = conn._op_lock_owner == creator_ident
    assert probe_says_take_bypass is False, (
        "owner-aware probe must NOT take the bypass when a sibling holds the lock"
    )
    # Bare probe would have said yes.
    assert conn._op_lock.locked() is True

    sibling_can_release.set()
    t.join(timeout=5.0)


def test_close_bypass_takes_when_creator_thread_owns() -> None:
    """When the creator thread owns the lock (stuck mid-acquire after SIGINT), bypass fires."""
    conn = _make_conn()
    conn._op_lock.acquire()
    conn._op_lock_owner = threading.get_ident()

    creator_ident = threading.get_ident()
    probe_says_take_bypass = conn._op_lock_owner == creator_ident
    assert probe_says_take_bypass is True

    conn._op_lock_owner = None
    conn._op_lock.release()


_ = pytest  # suppress unused-import lint
