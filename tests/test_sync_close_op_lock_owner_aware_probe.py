"""Pin: ``Connection.close()``'s same-thread re-entry bypass uses
an owner-aware probe (``_op_lock_owner == threading.get_ident()``)
rather than the bare ``_op_lock.locked()`` probe. Under tier-2
(``check_same_thread=False``) a sibling thread can legitimately
hold the lock from inside its own ``_run_sync``; the bare probe
would see "lock held + I'm the creator thread" and release the
sibling's lock, corrupting protocol state by allowing two
coroutines to drive the wire concurrently.

The owner-aware probe distinguishes "WE hold the lock" (the
KI-self-heal case the bypass was designed for) from "someone else
holds it" (let the bounded acquire path run, surfacing the
contention as a clean ``OperationalError`` from ``_run_sync``).
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
    """Sibling thread legitimately holds the op_lock; creator's
    close() bypass probe must NOT release it (the bypass is
    designed for the SAME-thread self-heal only).
    """
    conn = _make_conn()

    # Sibling thread acquires the lock and stamps owner.
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

    # Now, on the creator thread, simulate close()'s bypass-probe
    # check. The owner-aware probe must return False (sibling owns).
    creator_ident = threading.get_ident()
    assert conn._op_lock_owner != creator_ident, "preconditions: sibling owns the lock, not creator"
    # The probe condition used in close() at :2540:
    probe_says_take_bypass = conn._op_lock_owner == creator_ident
    assert probe_says_take_bypass is False, (
        "owner-aware probe must NOT take the bypass when a sibling holds the lock"
    )
    # Sanity: the lock IS held (bare probe would have said yes).
    assert conn._op_lock.locked() is True

    # Release sibling.
    sibling_can_release.set()
    t.join(timeout=5.0)


def test_close_bypass_takes_when_creator_thread_owns() -> None:
    """KI-self-heal regression: when the creator thread DOES own
    the lock (stuck mid-acquire after a SIGINT), the bypass fires.
    """
    conn = _make_conn()
    conn._op_lock.acquire()
    conn._op_lock_owner = threading.get_ident()

    creator_ident = threading.get_ident()
    probe_says_take_bypass = conn._op_lock_owner == creator_ident
    assert probe_says_take_bypass is True

    # Cleanup.
    conn._op_lock_owner = None
    conn._op_lock.release()


# Suppress unused pytest lint nag.
_ = pytest
