"""``_cleanup_loop_thread`` is fork-safe: it skips loop/thread teardown
in a forked child (checking ``creator_pid`` at the top), avoiding a
false ResourceWarning, a join on a non-existent thread, and closing
inherited selector FDs the parent still uses."""

from __future__ import annotations

import asyncio
import os
import threading
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.connection import _cleanup_loop_thread


def test_cleanup_short_circuits_when_pid_mismatch_does_not_close_loop() -> None:
    """Pid mismatch (forked child): no ``loop.close()`` /
    ``call_soon_threadsafe`` / ``thread.join`` on parent-owned state."""
    fake_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock(spec=threading.Thread)
    closed_flag = [False]

    parent_pid = os.getpid()
    child_pid = parent_pid + 1  # simulated child pid
    with patch("dqlitedbapi.connection.get_current_pid", return_value=child_pid):
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
    """closed_flag is a parent snapshot at fork-time; emitting a
    ResourceWarning from it would be a false-positive (parent may close
    after the fork), so the pid-mismatch path also skips the warning."""
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
    """Same-process (normal GC) path executes the full teardown."""
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
    """End-to-end: fork, GC the inherited Connection in the child, and
    verify no thread.join block / no FD close / no false ResourceWarning."""
    import contextlib as _contextlib
    import gc
    import time

    import dqlitedbapi

    conn = dqlitedbapi.connect("127.0.0.1:9999")
    # _ensure_loop registers the finalizer unconditionally on first call,
    # even if it later raises pre-connect.
    with _contextlib.suppress(Exception):
        conn._ensure_loop()
    assert conn._finalizer is not None, (
        "test pre-condition: finalizer must be registered before fork"
    )

    pipe_r, pipe_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        # Child: GC the inherited Connection; the finalizer must
        # short-circuit on pid mismatch.
        os.close(pipe_r)
        try:
            t0 = time.monotonic()
            del conn
            gc.collect()
            elapsed = time.monotonic() - t0
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
    elapsed = float(msg.split("elapsed=")[1])
    assert elapsed < 1.0, (
        f"forked-child GC took {elapsed:.3f}s — likely the finalizer "
        f"ran thread.join on the parent's thread; expected fast no-op"
    )
    conn.close()
