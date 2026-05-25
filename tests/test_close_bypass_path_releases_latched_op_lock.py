"""Pin: ``Connection.close()``'s same-thread bypass path releases
``_op_lock`` if it was left latched by a prior KI-interrupted
``_run_sync``.

The pre-acquire KI window at ``_run_sync`` lines 1603-1617 has a
``contextlib.suppress(RuntimeError)`` compensation; the post-acquire
window (between ``LOAD_FAST acquired`` and ``CALL release()`` in
the trailing finally) does not. A SIGINT delivered in that window
leaves the lock held by this thread. ``close()`` detects the
same-thread latched lock and takes the bypass path — pre-fix, the
bypass returned without releasing, so the next ``_run_sync``
deadlocked for the full ``self._timeout``. Post-fix, the bypass
calls ``release()`` with ``suppress(RuntimeError)``.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from dqlitedbapi.connection import Connection


def test_close_bypass_releases_latched_op_lock() -> None:
    """Hand-construct the latched state: acquire ``_op_lock`` on
    the creator thread without releasing. Drive ``close()``. The
    bypass arm must release the lock so a subsequent ``acquire()``
    returns immediately rather than waiting on the latched lock."""
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

    # Simulate the latched state: acquire without release AND
    # stamp the owner slot (now required by the close() bypass
    # probe which is owner-aware to avoid releasing a sibling
    # thread's lock under tier-2).
    conn._op_lock.acquire()
    conn._op_lock_owner = threading.get_ident()
    assert conn._op_lock.locked()

    # Drive close(); the bypass arm should release the lock.
    conn.close()

    # Load-bearing: the lock must be released so a subsequent acquire
    # does not wait.
    acquired_post_close = conn._op_lock.acquire(timeout=0.01)
    assert acquired_post_close, (
        "close()'s same-thread bypass arm must release a latched "
        "_op_lock; pre-fix the bypass returned with the lock still "
        "held and the next _run_sync would deadlock for the full "
        "self._timeout"
    )
    conn._op_lock.release()
