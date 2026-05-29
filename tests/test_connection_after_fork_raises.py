"""``Connection`` used after ``os.fork`` raises ``InterfaceError``: the inherited
socket/loop-thread would silently corrupt the wire or deadlock in the child."""

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
        assert conn._creator_pid == os.getpid()

        # Pipe for child->parent reporting so a child crash can't take pytest down.
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
        # Don't close() — never connected, and the parent's loop thread is healthy.
        conn._closed_flag[0] = True


def test_dbapi_close_after_fork_scrubs_cursors() -> None:
    """The fork short-circuit in ``close()`` still cascades the cursor scrub."""
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
        with patch("dqliteclient.connection.os.getpid", return_value=fake_parent_pid + 1):
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
    """``close()`` in the child short-circuits: no ``_close_async`` against the
    parent's defunct loop, no FIN on the inherited socket."""
    conn = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        # Parent has a live loop thread; the child inherits the refs but not the thread.
        conn._ensure_loop()

        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                os.close(r)
                try:
                    conn.close()
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
        # Parent-side close runs full teardown so the daemon thread joins.
        with contextlib.suppress(Exception):
            conn.close()
