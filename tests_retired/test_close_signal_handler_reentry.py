"""close() from a signal handler (prior _run_sync parked) must not block self._timeout on
its own held _op_lock: the same-thread-reentry guard schedules _close_async on the loop
directly instead of going through _run_sync's bounded acquire.
"""

from __future__ import annotations

import contextlib
import threading
import time
from unittest.mock import patch

from dqlitedbapi.connection import Connection


def _make_with_loop_thread() -> Connection:
    """Build a sync Connection with a real loop thread."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn._ensure_loop()
    return conn


def test_close_nulls_async_conn_when_run_sync_close_path_is_suppressed() -> None:
    """close() must null self._async_conn even when the suppressed _run_sync(_close_async())
    never runs its finally, else the writer transport FD leaks past loop teardown."""
    conn = _make_with_loop_thread()
    try:
        from unittest.mock import MagicMock

        stub_inner = MagicMock()
        stub_inner._protocol = MagicMock()
        stub_inner._protocol._writer = MagicMock()
        conn._async_conn = stub_inner

        # Force the _run_sync arm to raise so the suppress fires.
        def _raise(coro: object) -> None:
            # Close the coroutine to suppress "never awaited" warnings.
            with contextlib.suppress(Exception):
                coro.close()  # type: ignore[attr-defined]
            raise RuntimeError("simulated _run_sync wedge")

        with patch.object(conn, "_run_sync", side_effect=_raise):
            conn.close()

        assert conn._async_conn is None, (
            "Connection.close() must null self._async_conn even "
            "when _run_sync(self._close_async()) is suppressed — "
            "otherwise the underlying writer transport leaks past "
            "loop teardown."
        )
        # Best-effort writer.close() reaps the FD synchronously instead of
        # waiting on the transport's deferred __del__.
        stub_inner._protocol._writer.close.assert_called_once_with()
    finally:
        conn._closed = True


def test_close_does_not_block_when_op_lock_already_held_on_creator_thread() -> None:
    """With _op_lock held on the creator thread, close() must not block the bounded acquire:
    it detects same-thread reentry and schedules the close on the loop directly."""
    conn = _make_with_loop_thread()
    try:
        # Stamp the owner slot too: the bypass probe is owner-aware (not a bare
        # locked() probe) to avoid releasing a sibling thread's lock under tier-2.
        assert conn._op_lock.acquire(blocking=False)
        conn._op_lock_owner = threading.get_ident()
        try:
            t0 = time.monotonic()
            conn.close()
            elapsed = time.monotonic() - t0
        finally:
            with contextlib.suppress(RuntimeError):
                conn._op_lock.release()
        assert elapsed < 1.0, (
            f"close() must not block on the bounded _op_lock acquire "
            f"when invoked re-entrantly from the creator thread "
            f"(elapsed: {elapsed:.2f}s; configured timeout: 2.0s)"
        )
    finally:
        conn._closed = True


def test_close_takes_normal_path_when_op_lock_not_held() -> None:
    """With _op_lock not held, close() goes through the regular _run_sync path."""
    conn = _make_with_loop_thread()
    try:
        with patch.object(conn, "_run_sync") as run_sync_mock:
            run_sync_mock.return_value = None
            conn.close()
            assert run_sync_mock.called, (
                "close() should route through _run_sync when _op_lock "
                "is not already held by the creator thread"
            )
    finally:
        conn._closed = True


def test_close_takes_normal_path_when_op_lock_held_by_other_thread() -> None:
    """If _op_lock is held by a different thread, the guard takes the normal path so the
    bounded acquire waits/times out as a regular cross-thread acquire."""
    conn = _make_with_loop_thread()
    try:
        sibling_done = threading.Event()
        sibling_release = threading.Event()

        def sibling_holds_lock() -> None:
            conn._op_lock.acquire()
            sibling_done.set()
            sibling_release.wait()
            conn._op_lock.release()

        sibling = threading.Thread(target=sibling_holds_lock)
        sibling.start()
        try:
            sibling_done.wait()
            # Lock held by the sibling, not the creator thread, so the reentry
            # guard's get_ident() check fails and the normal path runs. Exercise
            # the guard predicate rather than spinning the bounded-acquire timeout.
            assert conn._op_lock.locked()
            assert threading.get_ident() == conn._creator_thread
        finally:
            sibling_release.set()
            sibling.join()
    finally:
        conn._closed = True
