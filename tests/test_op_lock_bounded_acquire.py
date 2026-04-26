"""Pin: ``_run_sync`` bounded ``_op_lock`` acquire raises on contention.

Same-thread re-entry from a signal handler (e.g. a SIGTERM handler
that calls ``close()`` while ``execute()`` is mid-await) used to
deadlock the non-reentrant ``threading.Lock``. Bounding the acquire
by ``self._timeout`` raises a clean ``InterfaceError`` instead.

Cross-thread waiters honour the same bound — long-running ops cannot
trap a sibling thread's call indefinitely.
"""

from __future__ import annotations

import threading
import time

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError


def test_same_thread_reentrant_acquire_raises_interface_error() -> None:
    """A same-thread re-entry (the signal-handler-calls-close case)
    must surface as a clean InterfaceError, not a silent deadlock."""
    conn = Connection("localhost:9001", timeout=0.5)
    try:
        # Hold the lock from this thread.
        conn._op_lock.acquire()
        try:
            # Schedule a no-op coroutine; the bounded acquire inside
            # _run_sync will time out in 0.5s and raise InterfaceError.
            async def _noop() -> int:
                return 0

            start = time.monotonic()
            with pytest.raises(InterfaceError, match="another operation is in progress"):
                conn._run_sync(_noop())
            elapsed = time.monotonic() - start
            # Confirm the bounded wait honoured the timeout (not a
            # deadlock).
            assert 0.4 < elapsed < 2.0, f"unexpected elapsed time {elapsed}"
        finally:
            conn._op_lock.release()
    finally:
        conn._closed = True


def test_cross_thread_contention_bounded_by_timeout() -> None:
    """Cross-thread waiters honour the same bound."""
    conn = Connection("localhost:9001", timeout=0.5)
    try:
        # Worker thread holds the lock for a while.
        held = threading.Event()
        release = threading.Event()

        def hold_lock() -> None:
            with conn._op_lock:
                held.set()
                release.wait()

        worker = threading.Thread(target=hold_lock)
        worker.start()
        held.wait()
        try:
            # Main thread tries to enter _run_sync while the worker
            # thread holds the lock — must time out cleanly.
            async def _noop() -> int:
                return 0

            start = time.monotonic()
            with pytest.raises(InterfaceError, match="another operation is in progress"):
                conn._run_sync(_noop())
            elapsed = time.monotonic() - start
            assert 0.4 < elapsed < 2.0, f"unexpected elapsed time {elapsed}"
        finally:
            release.set()
            worker.join(timeout=1.0)
    finally:
        conn._closed = True
