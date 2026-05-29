"""Pin: ``force_close_transport`` never raises even when its defensive
suppress arms (detach, weakref.proxy, del messages) fire — callers
reach it from non-cooperative cleanup (do_terminate, atexit, GC).
"""

from __future__ import annotations

import os
import weakref
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


def _build_conn_skeleton(inner: object | None) -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    conn._cursors = weakref.WeakSet()
    conn.messages = []
    conn._creator_pid = os.getpid()
    conn._async_conn = inner  # type: ignore[assignment]
    conn._finalizer = None
    return conn


def test_force_close_transport_tolerates_inner_finalizer_detach_failure() -> None:
    """A finalizer whose ``detach()`` raises is swallowed and the ref nulled."""
    inner = MagicMock()
    inner._closed_flag = [False]
    inner._connected_flag = [True]
    inner._protocol = None  # no writer to reap
    bad_finalizer = MagicMock()
    bad_finalizer.detach = MagicMock(side_effect=RuntimeError("synthetic"))
    inner._finalizer = bad_finalizer

    conn = _build_conn_skeleton(inner)

    conn.force_close_transport()
    bad_finalizer.detach.assert_called_once()
    assert inner._finalizer is None, (
        "the inner finalizer reference must be nulled even when "
        "detach() raises so a subsequent GC sweep does not see "
        "a stale entry"
    )


def test_force_close_transport_cursor_cascade_tolerates_missing_messages() -> None:
    """A cursor lacking ``messages`` trips suppress(AttributeError) without
    breaking the cascade."""

    class _BareCur:
        pass

    cur = _BareCur()
    cur._closed = False  # type: ignore[attr-defined]
    cur._connection = MagicMock()  # type: ignore[attr-defined]

    conn = _build_conn_skeleton(inner=None)
    conn._cursors.add(cur)  # type: ignore[arg-type]

    conn.force_close_transport()
    assert cur._closed is True  # type: ignore[attr-defined]


def test_force_close_transport_cursor_cascade_tolerates_unreferenceable_connection() -> None:
    """A cursor whose ``_connection`` is non-referenceable trips
    suppress(TypeError) without breaking the cascade."""

    class _NoWeakref:
        __slots__ = ()  # forbids weakref.proxy(...) — raises TypeError

    cur = MagicMock()
    cur._closed = False
    cur._connection = _NoWeakref()
    cur.messages = []

    conn = _build_conn_skeleton(inner=None)
    conn._cursors.add(cur)

    conn.force_close_transport()
    assert cur._closed is True
