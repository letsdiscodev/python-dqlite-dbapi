"""Pin: ``_cleanup_loop_thread`` finalizer is fork-safe — it skips
the loop / thread teardown when invoked in a forked child whose
parent owned the captured loop and thread refs.

Every other fork-traversing site in the dbapi codebase
(``Connection.close``, ``force_close_transport``,
``DqliteConnection.close``, ``AsyncConnection.close``, ``Pool.close``)
gates on ``get_current_pid() != self._creator_pid`` and short-
circuits. The ``weakref.finalize``-registered cleanup did NOT —
in a forked child it would: emit a false-positive ResourceWarning
(closed_flag snapshot is the parent's, frozen at fork-time at
False); ``loop.call_soon_threadsafe(loop.stop)`` against a parent-
owned loop (unsafe — pushes onto a queue with no consumer in the
child); ``thread.join`` for up to 5s on a non-existent OS thread;
``loop.close()`` on inherited selector FDs the parent still uses.

The fix passes ``creator_pid`` to the finalizer registration and
checks it at the top of the cleanup body.
"""

from __future__ import annotations

import asyncio
import os
import threading
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.connection import _cleanup_loop_thread


def test_cleanup_short_circuits_when_pid_mismatch_does_not_close_loop() -> None:
    """When called from a forked child (pid mismatch with the captured
    creator_pid), the cleanup must NOT call ``loop.close()`` /
    ``loop.call_soon_threadsafe`` / ``thread.join`` — those would
    operate on parent-owned state and either close inherited FDs or
    block on a non-existent OS thread.
    """
    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [False]

    parent_pid = os.getpid()
    child_pid = parent_pid + 1  # simulated child pid
    with patch("dqlitedbapi.connection.get_current_pid", return_value=child_pid):
        # Should be a no-op in the simulated child (pid mismatch).
        _cleanup_loop_thread(
            fake_loop,
            fake_thread,
            closed_flag,
            "host:9001",
            parent_pid,
        )

    fake_loop.call_soon_threadsafe.assert_not_called()
    fake_loop.close.assert_not_called()
    fake_thread.join.assert_not_called()


def test_cleanup_short_circuits_when_pid_mismatch_does_not_emit_warning() -> None:
    """In a forked child, the closed_flag is a snapshot of the parent's
    state at fork-time. Emitting a ResourceWarning based on that
    snapshot would be a false-positive — the parent may very well have
    closed the connection AFTER the fork. The pid-mismatch path must
    therefore also skip the warning.
    """
    import warnings as _warnings

    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [False]

    parent_pid = os.getpid()
    child_pid = parent_pid + 1
    with (
        patch("dqlitedbapi.connection.get_current_pid", return_value=child_pid),
        _warnings.catch_warnings(record=True) as captured,
    ):
        _warnings.simplefilter("always")
        _cleanup_loop_thread(
            fake_loop,
            fake_thread,
            closed_flag,
            "host:9001",
            parent_pid,
        )

    leak_warnings = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert not leak_warnings, (
        f"forked-child cleanup must not emit ResourceWarning; got "
        f"{[str(w.message) for w in leak_warnings]}"
    )


def test_cleanup_runs_normally_when_pid_matches() -> None:
    """Negative pin: when the cleanup runs in the SAME process that
    registered the finalizer (the normal GC path), it executes the
    full teardown — no behavioral change vs the pre-fix path.
    """
    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [True]  # user called close() — no warning expected

    same_pid = os.getpid()
    with patch("dqlitedbapi.connection.get_current_pid", return_value=same_pid):
        _cleanup_loop_thread(
            fake_loop,
            fake_thread,
            closed_flag,
            "host:9001",
            same_pid,
        )

    fake_loop.call_soon_threadsafe.assert_called_once()
    fake_loop.close.assert_called_once()
    fake_thread.join.assert_called_once()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_finalizer_in_forked_child_does_not_block_or_emit_warning() -> None:
    """End-to-end pin: construct a Connection (which registers a
    finalizer with creator_pid), fork, GC the inherited Connection
    in the child, and verify the child does not block on
    thread.join / does not close the parent's selector FDs / does
    not emit a false-positive ResourceWarning.
    """
    import contextlib as _contextlib
    import gc
    import time

    import dqlitedbapi

    conn = dqlitedbapi.connect("127.0.0.1:9999")
    # Force the loop thread to start so the finalizer has refs to
    # manipulate. In some test environments _ensure_loop raises pre-
    # connect; the test only needs the finalizer to be registered,
    # which happens unconditionally inside _ensure_loop's first call.
    with _contextlib.suppress(Exception):
        conn._ensure_loop()
    # Assert the finalizer registration happened so the test can't
    # trivially pass if _ensure_loop silently bailed before
    # registering.
    assert conn._finalizer is not None, (
        "test pre-condition: finalizer must be registered before fork"
    )

    pipe_r, pipe_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        # Child: clean up the inherited Connection. The finalizer
        # MUST short-circuit on pid mismatch.
        os.close(pipe_r)
        try:
            t0 = time.monotonic()
            del conn
            gc.collect()
            elapsed = time.monotonic() - t0
            # If the finalizer ran the thread.join, it would block
            # for up to _LOOP_THREAD_JOIN_TIMEOUT_SECONDS (5s).
            # Forked-child GC + finalize should complete in well under
            # 1 second.
            os.write(pipe_w, f"OK elapsed={elapsed:.3f}".encode())
        except BaseException as e:
            os.write(pipe_w, f"FAIL {type(e).__name__}: {e}".encode())
        finally:
            os.close(pipe_w)
            os._exit(0)

    os.close(pipe_w)
    raw = b""
    while True:
        chunk = os.read(pipe_r, 4096)
        if not chunk:
            break
        raw += chunk
    os.close(pipe_r)
    os.waitpid(pid, 0)
    msg = raw.decode()
    assert msg.startswith("OK"), f"forked-child finalizer reported: {msg}"
    # Parse the elapsed= field; assert below the join timeout.
    elapsed = float(msg.split("elapsed=")[1])
    assert elapsed < 1.0, (
        f"forked-child GC took {elapsed:.3f}s — likely the finalizer "
        f"ran thread.join on the parent's thread; expected fast no-op"
    )
    # Cleanup the parent's connection
    conn.close()
