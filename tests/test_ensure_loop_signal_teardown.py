"""``_ensure_loop`` must not publish a half-built loop/thread if a signal lands during
setup: a never-run loop reports ``is_closed()==False``, so a published orphan would pass the
recovery guard forever and wedge every later ``_run_sync``. The build is torn down on any
BaseException, and a subsequent call rebuilds a live loop.
"""

from __future__ import annotations

import asyncio
import os
import threading
import weakref

import pytest

from dqlitedbapi.connection import Connection


def _bare_connection() -> Connection:
    conn = Connection.__new__(Connection)
    conn._loop = None
    conn._thread = None
    conn._loop_lock = threading.Lock()
    conn._closed = False
    conn._creator_pid = os.getpid()
    conn._creator_thread = threading.get_ident()
    conn._inner_finalize_handle = []
    conn._closed_flag = [False]
    conn._address = ""
    conn._close_timeout = 1.0
    conn._finalizer = None
    return conn


def _ensure_loop(conn: Connection) -> asyncio.AbstractEventLoop:
    fn = getattr(conn._ensure_loop, "__wrapped__", None)
    result = fn(conn) if fn is not None else conn._ensure_loop()
    assert isinstance(result, asyncio.AbstractEventLoop)
    return result


def test_signal_during_setup_publishes_no_orphan_loop() -> None:
    conn = _bare_connection()
    real_start = threading.Thread.start
    calls = {"n": 0}

    def flaky_start(self: threading.Thread) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise KeyboardInterrupt("signal in the setup window")
        real_start(self)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(threading.Thread, "start", flaky_start)
        with pytest.raises(KeyboardInterrupt):
            _ensure_loop(conn)
        # No orphan published: the guard keys on _loop, which must still be None
        # (not a never-run, non-closed loop that would wedge the connection).
        assert conn._loop is None
        assert conn._thread is None

    # After the signal clears, a fresh call rebuilds a live, running loop.
    loop = _ensure_loop(conn)
    try:
        assert loop is conn._loop
        assert not loop.is_closed()
        assert conn._thread is not None and conn._thread.is_alive()
    finally:
        loop.call_soon_threadsafe(loop.stop)
        if conn._thread is not None:
            conn._thread.join(timeout=2.0)
        if not loop.is_closed():
            loop.close()


def test_signal_after_thread_start_stops_and_closes_the_loop() -> None:
    """Signal AFTER thread.start() but before publish: the teardown arm must stop+join
    the running loop thread and close the loop (not leak a live daemon thread)."""
    conn = _bare_connection()
    created: list[asyncio.AbstractEventLoop] = []
    real_new = asyncio.new_event_loop
    real_finalize = weakref.finalize
    calls = {"n": 0}

    def recording_new() -> asyncio.AbstractEventLoop:
        loop = real_new()
        created.append(loop)
        return loop

    def flaky_finalize(*args: object, **kwargs: object) -> object:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("signal after thread start")
        return real_finalize(*args, **kwargs)  # type: ignore[arg-type]

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(asyncio, "new_event_loop", recording_new)
        mp.setattr(weakref, "finalize", flaky_finalize)
        with pytest.raises(RuntimeError, match="signal after thread start"):
            _ensure_loop(conn)
        assert conn._loop is None
        assert conn._thread is None
        # The thread that did start was stopped+joined and its loop closed.
        assert created and created[0].is_closed()

    loop = _ensure_loop(conn)
    try:
        assert not loop.is_closed()
        assert conn._thread is not None and conn._thread.is_alive()
    finally:
        loop.call_soon_threadsafe(loop.stop)
        if conn._thread is not None:
            conn._thread.join(timeout=2.0)
        if not loop.is_closed():
            loop.close()


def test_orphan_never_run_loop_reports_not_closed() -> None:
    """Documents the premise: a never-run loop is NOT closed, so publishing one would
    slip past the is_closed() recovery guard."""
    loop = asyncio.new_event_loop()
    try:
        assert loop.is_closed() is False
    finally:
        loop.close()
