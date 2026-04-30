"""Pin: dbapi ``Connection`` raises ``InterfaceError`` if used after
``os.fork``.

Fork-after-init is unsupported: the inherited TCP socket is shared with
the parent (writes would interleave on the wire), the inherited daemon
loop thread does not survive fork, and asyncio primitives bound to the
parent's loop are unusable in the child. Without an explicit guard the
child silently corrupts the wire or deadlocks.

The fix records ``os.getpid()`` in ``__init__`` and ``_check_thread``
raises a clear ``InterfaceError`` ("reconstruct from configuration in
the target process") on pid mismatch — surfacing the misuse instead of
producing opaque failures.

The test does not need a live server: the pid check fires before any
async work, so a connection in an unconnected state is sufficient.
"""

from __future__ import annotations

import contextlib
import os
from unittest.mock import patch

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_dbapi_connection_used_after_fork_raises_interface_error() -> None:
    conn = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        # Sanity: same-process pid check is a no-op, the cross-thread
        # check is the only thing exercised pre-fork.
        assert conn._creator_pid == os.getpid()

        # Use a pipe for child→parent assertion-result reporting so the
        # child can crash without taking pytest down.
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                os.close(r)
                try:
                    conn.cursor()
                    os.write(w, b"NO_RAISE")
                except InterfaceError as e:
                    msg = str(e)
                    if "fork" in msg and "reconstruct from configuration" in msg:
                        os.write(w, b"OK")
                    else:
                        os.write(w, f"WRONG_MSG:{msg}".encode())
                except Exception as e:  # noqa: BLE001
                    os.write(w, f"WRONG_TYPE:{type(e).__name__}:{e}".encode())
                finally:
                    os.close(w)
            finally:
                os._exit(0)
        os.close(w)
        result = b""
        while True:
            chunk = os.read(r, 4096)
            if not chunk:
                break
            result += chunk
        os.close(r)
        os.waitpid(pid, 0)
        assert result == b"OK", f"child reported: {result!r}"
    finally:
        # Don't call close() — the connection was never connected, and
        # the loop thread (parent's) is still healthy here.
        conn._closed_flag[0] = True


def test_dbapi_close_after_fork_scrubs_cursors() -> None:
    """The fork-short-circuit branch in ``Connection.close()`` must
    cascade ``_closed = True`` and clear buffered rows on tracked
    cursors so a forked child's cursor doesn't keep silently
    answering ``fetchone/fetchmany/fetchall`` from the parent's
    stale in-memory rows. Mirrors the non-fork branch and the stdlib
    ``sqlite3.Connection.close()`` cascade contract."""
    import contextlib as _contextlib

    conn = dqlitedbapi.connect("127.0.0.1:9999")
    cursor = conn.cursor()
    cursor._rows = [(1,), (2,)]
    cursor._description = (("id", None, None, None, None, None, None),)
    cursor._rowcount = 2
    cursor._lastrowid = 7
    cursor._row_index = 0

    fake_parent_pid = conn._creator_pid + 1
    conn._creator_pid = fake_parent_pid

    try:
        with patch("dqliteclient.connection._current_pid", fake_parent_pid + 1):
            conn.close()

        assert cursor._closed is True
        assert cursor._rows == []
        assert cursor._description is None
        assert cursor._rowcount == -1
        assert cursor._lastrowid is None
        assert cursor._row_index == 0
    finally:
        with _contextlib.suppress(Exception):
            conn._closed_flag[0] = True


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_dbapi_connection_close_after_fork_short_circuits() -> None:
    """``close()`` in the child must not run ``_close_async`` against
    the parent's defunct loop or send FIN on the inherited socket.
    Short-circuits to a quiet local-state flip.

    Drive ``_ensure_loop`` first so there's a daemon loop thread in
    the parent — which does NOT survive fork — to exercise the
    most dangerous case: close() trying to drive a dead loop.
    """
    conn = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        # Ensure the parent has a live loop thread so the child
        # inherits the references but not the actual thread.
        conn._ensure_loop()

        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                os.close(r)
                try:
                    conn.close()  # must not deadlock or raise
                    assert conn._closed
                    os.write(w, b"OK")
                except Exception as e:  # noqa: BLE001
                    os.write(w, f"WRONG:{type(e).__name__}:{e}".encode())
                finally:
                    os.close(w)
            finally:
                os._exit(0)
        os.close(w)
        result = b""
        while True:
            chunk = os.read(r, 4096)
            if not chunk:
                break
            result += chunk
        os.close(r)
        os.waitpid(pid, 0)
        assert result == b"OK", f"child reported: {result!r}"
    finally:
        # Parent-side cleanup. close() in the parent runs the full
        # teardown so the daemon thread joins.
        with contextlib.suppress(Exception):
            conn.close()
