"""PEP 249 §6.1: ``Connection.close()`` is idempotent — a second close
from any thread must be a no-op, not raise ``ProgrammingError`` from
the cross-thread guard.

The first close still runs the creator-thread guard (it tears down
the loop thread and primitives that are GIL-but-not-thread-safe), but
the closed-state short-circuit must run BEFORE the thread check.

Common trigger paths: ``weakref.finalize(conn, conn.close)`` from the
GC thread, ``atexit.register(conn.close)`` from the interpreter
shutdown thread, or any ThreadPoolExecutor cleanup that closes
connections owned by other threads.
"""

from __future__ import annotations

import threading

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import ProgrammingError


def test_second_close_from_other_thread_is_noop() -> None:
    """First close on creator thread; second close from another thread
    must NOT raise — PEP 249 idempotent-close contract."""
    conn = Connection("localhost:9001")
    conn.close()  # first close on creator thread (this thread)

    error: list[BaseException] = []

    def cross_thread_reclose() -> None:
        try:
            conn.close()  # idempotent re-close from worker thread
        except BaseException as exc:
            error.append(exc)

    t = threading.Thread(target=cross_thread_reclose)
    t.start()
    t.join(timeout=5.0)
    assert not error, f"unexpected exception from cross-thread reclose: {error[0]!r}"


def test_first_close_from_other_thread_still_raises() -> None:
    """The thread guard still applies to the FIRST close — it tears
    down loop primitives that are not thread-safe."""
    conn = Connection("localhost:9001")
    error: list[BaseException] = []

    def cross_thread_first_close() -> None:
        try:
            conn.close()
        except BaseException as exc:
            error.append(exc)

    t = threading.Thread(target=cross_thread_first_close)
    t.start()
    t.join(timeout=5.0)
    # Mark closed for cleanup so the test fixture doesn't hang on the
    # background loop thread.
    conn._closed = True
    assert len(error) == 1
    assert isinstance(error[0], ProgrammingError)


def test_repeated_close_on_same_thread_remains_idempotent() -> None:
    """Same-thread re-close was already a no-op via the existing
    ``if self._closed: return`` guard. Pin that the reorder doesn't
    regress this case."""
    conn = Connection("localhost:9001")
    conn.close()
    conn.close()  # second close on same (creator) thread — must be no-op
    conn.close()  # third for good measure
