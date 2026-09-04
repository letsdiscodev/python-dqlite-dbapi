"""Pin: ``force_close_transport`` (sync and async) disarms the inner
client's ResourceWarning finalizer so a later GC sweep does not emit a
misleading "GC'd without close" on a connection closed via the force path.
"""

from __future__ import annotations

import weakref
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def _stub_inner_with_finalizer() -> MagicMock:
    """Fake inner with the three-flag gate and an armed weakref finalizer."""
    inner = MagicMock()
    inner._closed_flag = [False]
    inner._connected_flag = [True]

    def _noop_warn() -> None:
        pass

    inner._finalizer = weakref.finalize(inner, _noop_warn)
    return inner


def test_async_force_close_transport_disarms_inner_finalizer() -> None:
    """The async sibling detaches the inner finalizer and flips its closed flag."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    conn._cursors = weakref.WeakSet()
    conn.messages = []
    conn._creator_pid = __import__("os").getpid()
    inner = _stub_inner_with_finalizer()
    conn._async_conn = inner

    conn.force_close_transport()

    assert inner._closed_flag[0] is True
    assert inner._finalizer is None


def test_sync_force_close_transport_disarms_inner_finalizer() -> None:
    import threading

    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._connected_flag = [True]  # type: ignore[attr-defined]
    conn.messages = []
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = __import__("os").getpid()
    conn._loop_lock = threading.Lock()
    conn._loop = None
    conn._thread = None
    conn._connect_lock = None
    conn._finalizer = None
    conn._cursors = weakref.WeakSet()
    inner = _stub_inner_with_finalizer()
    conn._async_conn = inner

    conn.force_close_transport()

    assert inner._closed_flag[0] is True
    assert inner._finalizer is None
