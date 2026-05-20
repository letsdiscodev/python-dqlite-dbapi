"""Pin: ``force_close_transport`` (both sync and async siblings)
disarms the inner client's ResourceWarning finalizer
(``DqliteConnection._connection_unclosed_warning``) so a subsequent
GC sweep does NOT emit a misleading "GC'd without close" on the
very connection that was explicitly closed via the force path.

``close()`` detaches the inner finalizer inside ``_close_impl``;
``force_close_transport`` historically did not route through
``close()`` so the inner finalizer survived the explicit close
and the three-flag gate
(``_closed_flag`` AND ``_connected_flag``) failed open after
``self._async_conn`` was nulled.

Pin: after a ``force_close_transport`` on a connection with a
finalizer-armed inner, ``inner._closed_flag[0]`` is ``True`` and
``inner._finalizer`` is ``None``.
"""

from __future__ import annotations

import weakref
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def _stub_inner_with_finalizer() -> MagicMock:
    """A fake inner ``DqliteConnection`` with the three-flag gate
    pattern and an armed weakref finalizer. Returned ``inner``
    has the same shape ``_connection_unclosed_warning`` reads."""
    inner = MagicMock()
    inner._closed_flag = [False]
    inner._connected_flag = [True]

    def _noop_warn() -> None:
        pass

    inner._finalizer = weakref.finalize(inner, _noop_warn)
    return inner


def test_async_force_close_transport_disarms_inner_finalizer() -> None:
    """The async sibling must detach the inner's finalizer and flip
    its closed flag."""
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
    """The sync sibling must apply the same discipline."""
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
