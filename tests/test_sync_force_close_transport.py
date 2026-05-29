"""Sync ``force_close_transport`` is a synchronous, idempotent, bounded
last-resort cleanup hook bounded by ``_close_timeout`` with no thread-affinity check."""

from __future__ import annotations

import os
import threading
import time
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _make_unconnected() -> Connection:
    """Construct a Connection without driving any real connect (no loop/thread/inner)."""
    return Connection("localhost:9001", database="x")


def _make_with_live_loop_and_inner() -> tuple[Connection, MagicMock]:
    """Connection wired to a real background loop with a fake inner exposing
    the ``_protocol._writer`` chain that ``force_close_transport`` walks."""
    conn = Connection("localhost:9001", database="x")
    loop = conn._ensure_loop()
    assert loop is not None
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner
    return conn, writer


def test_force_close_transport_idempotent_on_unconnected() -> None:
    conn = _make_unconnected()
    assert conn.closed is False
    conn.force_close_transport()
    conn.force_close_transport()
    assert conn.closed is True


def test_force_close_transport_marks_closed_flag_for_finalizer() -> None:
    """The closed_flag the finalizer reads must be set so a later GC sweep emits no
    ``ResourceWarning``."""
    conn = _make_unconnected()
    assert conn._closed_flag[0] is False
    conn.force_close_transport()
    assert conn._closed_flag[0] is True


def test_force_close_transport_calls_writer_close_via_loop() -> None:
    """``writer.close()`` is scheduled on the loop thread (a direct call would race
    the selector), and runs before ``loop.stop`` lands."""
    conn, writer = _make_with_live_loop_and_inner()
    try:
        conn.force_close_transport()
    finally:
        pass
    writer.close.assert_called_once_with()
    assert conn._async_conn is None
    assert conn._loop is None
    assert conn._thread is None


def test_force_close_transport_returns_within_close_timeout() -> None:
    """The thread join is bounded by ``_close_timeout`` (default 0.5 s)."""
    conn, _writer = _make_with_live_loop_and_inner()
    start = time.monotonic()
    conn.force_close_transport()
    elapsed = time.monotonic() - start
    # Generous slack for slow CI: the point is "bounded", not "fast".
    assert elapsed < 5.0


def test_force_close_transport_short_circuits_after_close() -> None:
    """If ``close()`` already ran, ``force_close_transport`` is a no-op."""
    conn = _make_unconnected()
    conn._closed = True
    conn.force_close_transport()


def test_force_close_transport_callable_from_foreign_thread() -> None:
    """No ``_check_thread`` — callable from any thread (finalize, signal handlers, SA pool)."""
    conn = _make_unconnected()
    err: list[BaseException] = []

    def call_from_other_thread() -> None:
        try:
            conn.force_close_transport()
        except BaseException as e:
            err.append(e)

    t = threading.Thread(target=call_from_other_thread)
    t.start()
    t.join(timeout=2.0)
    assert err == []
    assert conn.closed is True


def test_force_close_transport_fork_branch_skips_loop_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fork-after-init: must NOT touch the inherited daemon loop thread (dead after fork)
    or the parent-owned socket FDs. Mirrors :meth:`close`'s pid guard."""

    conn = _make_unconnected()
    fake_loop = MagicMock()
    fake_loop.is_closed.return_value = False
    fake_thread = MagicMock()
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner
    conn._loop = fake_loop
    conn._thread = fake_thread

    # Simulate fork: pid mismatch.
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: conn._creator_pid + 1)

    conn.force_close_transport()
    fake_loop.call_soon_threadsafe.assert_not_called()
    fake_loop.close.assert_not_called()
    fake_thread.join.assert_not_called()
    writer.close.assert_not_called()
    assert conn._closed is True
    assert conn._closed_flag[0] is True


def test_force_close_transport_cascades_cursors() -> None:
    """Tracked cursors get the same closed-state cascade as ``close()``."""
    conn = _make_unconnected()
    cur = conn.cursor()
    cur._rows = [(1,)]
    cur._description = (("col", 4, None, None, None, None, None),)
    cur._rowcount = 1
    assert cur._closed is False

    conn.force_close_transport()
    assert cur._closed is True
    assert cur._rows == []
    assert cur._description is None
    assert cur._rowcount == -1
