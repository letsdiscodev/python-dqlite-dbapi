"""close()'s same-thread bypass releases _op_lock if a KI-interrupted _run_sync left it latched.

A SIGINT in the post-acquire window of _run_sync's trailing finally leaves the lock held by
this thread; pre-fix the bypass returned without releasing and the next _run_sync deadlocked.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from dqlitedbapi.connection import Connection


def test_close_bypass_releases_latched_op_lock() -> None:
    """Latch _op_lock on the creator thread, then close(); the bypass must release it."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._op_lock = threading.Lock()
    conn._loop_lock = threading.Lock()
    conn._loop = MagicMock()
    conn._loop.is_closed = MagicMock(return_value=False)
    conn._loop.close = MagicMock()
    conn._thread = MagicMock()
    conn._thread.is_alive = MagicMock(return_value=False)
    conn._async_conn = None
    conn._connect_lock = None
    conn._thread = MagicMock()
    conn._closed_flag = [False]
    import weakref as _w

    conn._cursors = _w.WeakSet()
    conn._finalizer = None
    conn.messages = []
    import os

    conn._creator_pid = os.getpid()
    conn._transaction_owner = None
    conn._close_timeout = 1.0
    conn._inner_finalize_handle = []
    conn._address = ""

    # Stamp the owner slot too: the bypass probe is owner-aware to avoid
    # releasing a sibling thread's lock under tier-2.
    conn._op_lock.acquire()
    conn._op_lock_owner = threading.get_ident()
    assert conn._op_lock.locked()

    conn.close()

    acquired_post_close = conn._op_lock.acquire(timeout=0.01)
    assert acquired_post_close, (
        "close()'s same-thread bypass arm must release a latched "
        "_op_lock; pre-fix the bypass returned with the lock still "
        "held and the next _run_sync would deadlock for the full "
        "self._timeout"
    )
    conn._op_lock.release()
