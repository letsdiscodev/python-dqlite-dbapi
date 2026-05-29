"""Pin: ``_run_sync`` bounds the ``_op_lock`` acquire by ``timeout``.

Same-thread re-entry (e.g. a signal handler calling ``close()`` mid-await)
used to deadlock the non-reentrant lock; the bound raises a clean
``OperationalError`` instead, which SA's ``is_disconnect`` recognises.
"""

from __future__ import annotations

import threading
import time

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import OperationalError


def test_same_thread_reentrant_acquire_raises_operational_error() -> None:
    """Same-thread re-entry must surface as OperationalError, not a deadlock."""
    conn = Connection("localhost:9001", timeout=0.5)
    try:
        conn._op_lock.acquire()
        try:

            async def _noop() -> int:
                return 0

            start = time.monotonic()
            with pytest.raises(OperationalError, match="op_lock acquire timed out"):
                conn._run_sync(_noop())
            elapsed = time.monotonic() - start
            assert 0.4 < elapsed < 2.0, f"unexpected elapsed time {elapsed}"
        finally:
            conn._op_lock.release()
    finally:
        conn._closed = True


def test_cross_thread_contention_bounded_by_timeout() -> None:
    """Cross-thread waiters honour the same bound."""
    conn = Connection("localhost:9001", timeout=0.5)
    try:
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

            async def _noop() -> int:
                return 0

            start = time.monotonic()
            with pytest.raises(OperationalError, match="op_lock acquire timed out"):
                conn._run_sync(_noop())
            elapsed = time.monotonic() - start
            assert 0.4 < elapsed < 2.0, f"unexpected elapsed time {elapsed}"
        finally:
            release.set()
            worker.join(timeout=1.0)
    finally:
        conn._closed = True
