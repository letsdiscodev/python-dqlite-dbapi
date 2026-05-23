"""Pin: ``Connection.force_close_transport`` (sync and async) clears
``self.messages`` per the project's "every public Connection method
clears" discipline (extension of PEP 249 §6.4).

Every other public Connection method clears messages as the first
statement; ``force_close_transport`` (a driver-extension method
intended for last-resort shutdown like SA's ``do_terminate``) was
the discipline-gap.
"""

from __future__ import annotations

import os
import threading
from typing import Any, cast

import dqlitedbapi
import dqlitedbapi.aio


def _bare_sync_connection() -> Any:
    c = cast(Any, dqlitedbapi.Connection.__new__(dqlitedbapi.Connection))
    c._closed = False
    c._closed_flag = [False]
    c._async_conn = None
    c._creator_thread = threading.get_ident()
    c._creator_pid = os.getpid()
    c.messages = []
    c._cursors = set()
    c._loop = None
    c._thread = None
    c._loop_lock = threading.Lock()
    c._op_lock = threading.Lock()
    c._connect_lock = None
    c._finalizer = None
    c._close_timeout = 0.5
    return c


def _bare_async_connection() -> Any:
    c = cast(Any, dqlitedbapi.aio.AsyncConnection.__new__(dqlitedbapi.aio.AsyncConnection))
    c._closed = False
    c._closed_flag = [False]
    c._connected_flag = [False]
    c._async_conn = None
    c._creator_pid = os.getpid()
    c._creator_thread = threading.get_ident()
    c._loop_ref = None
    c._connect_lock = None
    c._op_lock = None
    c.messages = []
    c._finalizer = None
    return c


def test_sync_force_close_transport_clears_messages() -> None:
    c = _bare_sync_connection()
    c.messages.append((Exception, Exception("stale-pre-force-close")))
    c.force_close_transport()
    assert c.messages == [], (
        "Connection.force_close_transport must clear self.messages, "
        "matching the project's every-public-method discipline."
    )


def test_async_force_close_transport_clears_messages() -> None:
    c = _bare_async_connection()
    c.messages.append((Exception, Exception("stale-pre-force-close")))
    c.force_close_transport()
    assert c.messages == [], "AsyncConnection.force_close_transport must clear self.messages."


def test_sync_force_close_transport_idempotent_clears_each_call() -> None:
    """Re-arming messages between calls and re-clearing — pin the
    idempotence + clear contract jointly."""
    c = _bare_sync_connection()
    c.messages.append((Exception, Exception("first")))
    c.force_close_transport()
    assert c.messages == []
    c.messages.append((Exception, Exception("second-after-already-closed")))
    c.force_close_transport()
    assert c.messages == [], (
        "force_close_transport must clear messages on every call, "
        "even when already closed (idempotent)."
    )


def test_async_force_close_transport_idempotent_clears_each_call() -> None:
    c = _bare_async_connection()
    c.messages.append((Exception, Exception("first")))
    c.force_close_transport()
    assert c.messages == []
    c.messages.append((Exception, Exception("second")))
    c.force_close_transport()
    assert c.messages == []
