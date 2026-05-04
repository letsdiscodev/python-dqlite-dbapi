"""Pin: sync ``Connection.force_close_transport`` is a synchronous,
idempotent, bounded last-resort cleanup hook.

Unlike ``close()``, this path:

- Skips ``_run_sync(_close_async())`` so a parked ``reader.read()``
  cannot gate the shutdown.
- Schedules ``writer.close()`` via ``call_soon_threadsafe`` (the
  writer is not thread-safe per stdlib asyncio).
- Is bounded by ``self._close_timeout`` for the thread join, not
  ``self._timeout``.
- Has no thread-affinity check — terminate must work from finalize
  threads and signal handlers.

Intended for SQLAlchemy's sync ``do_terminate`` during
``engine.dispose()`` under partition + SIGTERM scenarios.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _make_unconnected() -> Connection:
    """Construct a Connection without driving any real connect.

    No ``_ensure_loop`` runs; ``_loop`` / ``_thread`` / ``_async_conn``
    stay ``None``. The terminate path's no-op branches are exercised
    here.
    """
    return Connection("localhost:9001", database="x")


def _make_with_live_loop_and_inner() -> tuple[Connection, MagicMock]:
    """Construct a Connection wired up to a real background loop with a
    fake inner ``DqliteConnection``. The fake exposes the
    ``_protocol._writer`` chain that ``force_close_transport`` walks.
    """
    conn = Connection("localhost:9001", database="x")
    # Drive lazy loop creation directly so the test doesn't need a
    # working dqlite cluster.
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
    conn.force_close_transport()  # second call is a no-op
    assert conn.closed is True


def test_force_close_transport_marks_closed_flag_for_finalizer() -> None:
    """The closed_flag the finalizer reads must be set so a subsequent
    GC sweep does NOT emit ``ResourceWarning``."""
    conn = _make_unconnected()
    assert conn._closed_flag[0] is False
    conn.force_close_transport()
    assert conn._closed_flag[0] is True


def test_force_close_transport_calls_writer_close_via_loop() -> None:
    """``writer.close()`` is scheduled on the loop thread (not called
    directly from the calling thread, which would race the selector).
    The loop processes the call before ``loop.stop`` lands.
    """
    conn, writer = _make_with_live_loop_and_inner()
    try:
        conn.force_close_transport()
    finally:
        # Ensure cleanup: the connection is "closed" but if anything
        # leaked we want to surface it via teardown.
        pass
    # The loop has been stopped + closed by force_close_transport.
    # writer.close() ran on the loop thread before the stop() callback
    # was processed.
    writer.close.assert_called_once_with()
    assert conn._async_conn is None
    assert conn._loop is None
    assert conn._thread is None


def test_force_close_transport_returns_within_close_timeout() -> None:
    """Bounded-time guarantee: the thread join uses
    ``self._close_timeout`` (default 0.5 s)."""
    conn, _writer = _make_with_live_loop_and_inner()
    start = time.monotonic()
    conn.force_close_transport()
    elapsed = time.monotonic() - start
    # close_timeout default is 0.5 s; allow generous slack for slow CI
    # runners. The point of the test is "bounded", not "fast".
    assert elapsed < 5.0


def test_force_close_transport_short_circuits_after_close() -> None:
    """If ``close()`` already ran, ``force_close_transport`` is a
    no-op (does not double-close)."""
    conn = _make_unconnected()
    conn._closed = True  # simulate close() having run
    # Should return cleanly without touching primitives.
    conn.force_close_transport()


def test_force_close_transport_callable_from_foreign_thread() -> None:
    """No ``_check_thread`` — finalize / signal handlers / SA's pool
    finalize can call this from any thread."""
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
    """Fork-after-init: ``force_close_transport`` must NOT touch the
    inherited daemon loop thread (which did not survive fork) or the
    inherited socket FDs (still owned by the parent). Mirrors
    :meth:`close`'s pid guard.
    """
    from dqliteclient import connection as _client_conn_mod

    conn = _make_unconnected()
    # Stand up a fake inner / loop / thread so the assertions below
    # have something concrete to check.
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
    monkeypatch.setattr(_client_conn_mod, "_current_pid", conn._creator_pid + 1)

    conn.force_close_transport()
    # The fork branch must NOT schedule writer.close on a loop the
    # child does not own, must NOT join the dead daemon thread, and
    # must NOT close the parent-shared socket FD.
    fake_loop.call_soon_threadsafe.assert_not_called()
    fake_loop.close.assert_not_called()
    fake_thread.join.assert_not_called()
    writer.close.assert_not_called()
    assert conn._closed is True
    assert conn._closed_flag[0] is True


def test_force_close_transport_cascades_cursors() -> None:
    """Tracked cursors get the same closed-state cascade as
    ``close()`` so buffered rows on them no longer answer."""
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
